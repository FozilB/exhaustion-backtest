#!/usr/bin/env python3
"""Массовая проверка идеи «истощение пампа/дампа» на истории OKX.

Читает только публичные данные OKX (без ключей, без торговли). Правила те же, что в
Pine-скрипте для TradingView: импульс -> свеча истощения (объём + отказ цены)
-> подтверждение -> вход против импульса, стоп за свечой, цель в R, выход по времени.
Результат: отчёт (Markdown), CSV со сделками и короткая сводка в Telegram.
"""
from __future__ import annotations

import csv
import json
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
BAR_MS = {"1h": 3_600_000, "4h": 14_400_000}
OKX_BAR = {"1h": "1H", "4h": "4H"}
OUT_DIR = Path(os.environ.get("OUT_DIR", "backtest_out"))
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "cache"))


def log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {message}", flush=True)


class Blocked(Exception):
    """Биржа отклонила запрос с этого IP (HTTP 403/451)."""


class ApiError(Exception):
    """Запрос к бирже не удался."""


# --------------------------------------------------------------------------- данные OKX
class Okx:
    def __init__(self, min_interval: float = 0.2, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "read-only-exhaustion-backtest/1.0"})
        self.min_interval = min_interval
        self._last = 0.0

    def _get(self, path: str, params: dict) -> list:
        last_error = "?"
        for attempt in range(6):
            wait = self.min_interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                response = self.session.get(BASE_URL + path, params=params, timeout=20)
            except requests.RequestException as error:
                last_error = f"{type(error).__name__}: {str(error)[:160]}"
                time.sleep(2 * (attempt + 1))
                continue
            if response.status_code in (403, 451):
                raise Blocked(f"HTTP {response.status_code}")
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"HTTP {response.status_code}"
                time.sleep(3 * (attempt + 1))
                continue
            if response.status_code >= 400:
                raise ApiError(f"HTTP {response.status_code}: {response.text[:120]}")
            try:
                payload = response.json()
            except ValueError:
                last_error = "не JSON"
                time.sleep(2)
                continue
            code = str(payload.get("code"))
            if code in ("50011", "50061"):  # слишком частые запросы
                last_error = f"OKX {code}"
                time.sleep(3 * (attempt + 1))
                continue
            if code != "0":
                raise ApiError(f"OKX {code}: {payload.get('msg')}")
            return payload.get("data", [])
        raise ApiError(f"не удалось получить данные: {last_error}")

    def symbols(self, limit: int, exclude: set[str]) -> list[str]:
        """Самые ликвидные USDT-перпетуалы OKX по обороту за 24 часа."""
        found = []
        for item in self._get("/api/v5/market/tickers", {"instType": "SWAP"}):
            inst = item.get("instId", "")
            if not inst.endswith("-USDT-SWAP") or inst.split("-")[0] in exclude:
                continue
            try:
                turnover = float(item["volCcy24h"]) * float(item["last"])
            except (KeyError, TypeError, ValueError):
                continue
            found.append((turnover, inst))
        found.sort(reverse=True)
        return [inst for _, inst in found[:limit]]

    def history(self, inst_id: str, tf: str, since_ms: int) -> list[tuple]:
        """Закрытые свечи (ts, o, h, l, c, объём в монете), от старых к новым."""
        rows: dict[int, tuple] = {}
        after = None
        previous_oldest = None
        while True:
            params = {"instId": inst_id, "bar": OKX_BAR[tf], "limit": 100}
            if after is not None:
                params["after"] = after
            data = self._get("/api/v5/market/history-candles", params)
            if not data:
                break
            for row in data:
                if len(row) > 8 and str(row[8]) != "1":
                    continue  # свеча ещё формируется
                volume = float(row[6]) if len(row) > 6 and row[6] not in ("", None) else float(row[5])
                rows[int(row[0])] = (int(row[0]), float(row[1]), float(row[2]),
                                     float(row[3]), float(row[4]), volume)
            oldest = min(int(row[0]) for row in data)
            if oldest <= since_ms or (previous_oldest is not None and oldest >= previous_oldest):
                break
            previous_oldest = oldest
            after = str(oldest)
        return [rows[key] for key in sorted(rows) if key >= since_ms]


def load_rows(okx: Okx, inst_id: str, tf: str, days: int, now_ms: int) -> list[tuple]:
    """История монеты; кэш на диске живёт 12 часов, чтобы повторный запуск был быстрым."""
    since_ms = now_ms - days * 86_400_000
    path = CACHE_DIR / f"{inst_id}_{tf}.json"
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
        rows = [tuple(r) for r in cached["rows"]]
        fresh = time.time() - cached["fetched_at"] < 12 * 3600
        if fresh and rows and rows[0][0] <= since_ms + 3 * BAR_MS[tf]:
            return [r for r in rows if r[0] >= since_ms]
    except (OSError, ValueError, KeyError):
        pass
    rows = okx.history(inst_id, tf, since_ms)
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"fetched_at": time.time(), "rows": rows}), encoding="utf-8")
    except OSError:
        pass
    return rows


# --------------------------------------------------------------------------- правила
@dataclass(frozen=True)
class Cfg:
    name: str = "ОСНОВНОЙ"
    win: int = 6              # окно импульса, свечей
    move_atr: float = 5.0     # размах окна, в ATR
    vol_len: int = 20
    vol_mult: float = 2.0     # объём свечи истощения / средний
    wick: float = 0.40        # тень отказа, доля диапазона
    clos: float = 0.40        # или закрытие в крайней доле диапазона
    top_tol: float = 0.5      # допуск до экстремума окна, в ATR
    confirm: bool = True
    confirm_bars: int = 2
    stop_buf: float = 0.2     # запас стопа, в ATR
    rr: float = 1.5           # цель, в R
    max_hold: int = 12        # максимум свечей в сделке
    entry: str = "close"      # "close" = по закрытию подтверждающей свечи, "next_open" = по открытию следующей
    shorts: bool = True
    longs: bool = True


class Series:
    def __init__(self, rows: list[tuple]):
        self.ts = [r[0] for r in rows]
        self.o = [r[1] for r in rows]
        self.h = [r[2] for r in rows]
        self.l = [r[3] for r in rows]
        self.c = [r[4] for r in rows]
        self.v = [r[5] for r in rows]
        self.n = len(rows)
        self.atr = self._atr()
        self._windows: dict[int, tuple] = {}
        self._avg: dict[int, list] = {}

    def _atr(self, length: int = 14) -> list:
        n = self.n
        out: list = [None] * n
        if n < length:
            return out
        tr = [self.h[0] - self.l[0]]
        for i in range(1, n):
            tr.append(max(self.h[i] - self.l[i], abs(self.h[i] - self.c[i - 1]),
                          abs(self.l[i] - self.c[i - 1])))
        out[length - 1] = sum(tr[:length]) / length
        for i in range(length, n):
            out[i] = (out[i - 1] * (length - 1) + tr[i]) / length
        return out

    def windows(self, w: int) -> tuple:
        """Для каждой свечи: максимум, минимум окна и индексы этих экстремумов (последний при равенстве)."""
        if w not in self._windows:
            hh, ll, ih, il = [None] * self.n, [None] * self.n, [None] * self.n, [None] * self.n
            for i in range(w - 1, self.n):
                seg_h = self.h[i - w + 1:i + 1]
                seg_l = self.l[i - w + 1:i + 1]
                hh[i], ll[i] = max(seg_h), min(seg_l)
                ih[i] = i - seg_h[::-1].index(hh[i])
                il[i] = i - seg_l[::-1].index(ll[i])
            self._windows[w] = (hh, ll, ih, il)
        return self._windows[w]

    def avg_vol(self, length: int) -> list:
        """Средний объём предыдущих `length` свечей (без текущей)."""
        if length not in self._avg:
            out: list = [None] * self.n
            running = sum(self.v[:length])
            for i in range(length, self.n):
                out[i] = running / length
                running += self.v[i] - self.v[i - length]
            self._avg[length] = out
        return self._avg[length]


def _make_pos(direction: int, entry: float, stop: float, bar: int, check_from: int, cfg: Cfg):
    if direction < 0 and not stop > entry:
        return None
    if direction > 0 and not stop < entry:
        return None
    risk = abs(stop - entry)
    if risk <= 0:
        return None
    tp = entry - cfg.rr * risk if direction < 0 else entry + cfg.rr * risk
    return {"d": direction, "entry": entry, "stop": stop, "tp": tp, "bar": bar, "check_from": check_from}


def _exit(pos: dict, o: float, h: float, l: float, c: float, i: int, cfg: Cfg):
    stop, tp = pos["stop"], pos["tp"]
    if pos["d"] < 0:
        if o >= stop:
            return o, "стоп"          # гэп через стоп: исполнение по открытию, хуже стопа
        if h >= stop:
            return stop, "стоп"       # при одновременном касании стопа и цели считаем стоп первым
        if l <= tp:
            return tp, "цель"
    else:
        if o <= stop:
            return o, "стоп"
        if l <= stop:
            return stop, "стоп"
        if h >= tp:
            return tp, "цель"
    if i - pos["bar"] >= cfg.max_hold:
        return c, "время"
    return None


def _confirmed(cfg: Cfg, pending: tuple, i: int, close: float, short: bool) -> bool:
    high, low, bar = pending
    if cfg.confirm:
        if not (i > bar and i - bar <= cfg.confirm_bars):
            return False
        return close < low if short else close > high
    return i == bar


def simulate(cfg: Cfg, s: Series, coin: str, tf: str, cost_side: float) -> list[dict]:
    """Все сделки по одной монете. cost_side — комиссия+проскальзывание на одну сторону (доля)."""
    n, w = s.n, cfg.win
    if n < w + 20:
        return []
    hh_l, ll_l, ih_l, il_l = s.windows(w)
    avg = s.avg_vol(cfg.vol_len)
    atr = s.atr
    first = max(w + 13, cfg.vol_len)
    trades: list[dict] = []
    pend_s = pend_l = None
    pos = None
    queued = None

    def finish(position: dict, price: float, reason: str, i: int) -> dict:
        d = position["d"]
        gross = d * (price - position["entry"]) / position["entry"] * 100
        net = gross - 2 * cost_side * 100
        risk_pct = abs(position["stop"] - position["entry"]) / position["entry"] * 100
        return {"cfg": cfg.name, "coin": coin, "tf": tf, "dir": "шорт" if d < 0 else "лонг",
                "entry_ts": s.ts[position["bar"]], "entry": position["entry"],
                "stop": position["stop"], "tp": position["tp"], "exit_ts": s.ts[i], "exit": price,
                "reason": reason, "bars": i - position["bar"], "gross": gross, "net": net,
                "risk": risk_pct, "r": net / risk_pct if risk_pct > 0 else 0.0}

    for i in range(n):
        o, h, l, c = s.o[i], s.h[i], s.l[i], s.c[i]
        if queued is not None:
            if pos is None:
                direction, stop, _ = queued
                pos = _make_pos(direction, o, stop, i, i, cfg)
            queued = None
        if pos is not None and i >= pos["check_from"]:
            result = _exit(pos, o, h, l, c, i, cfg)
            if result is not None:
                trades.append(finish(pos, result[0], result[1], i))
                pos = None
        if i < first:
            continue
        ab = atr[i - w]
        if ab is None or ab <= 0:
            continue
        hh, ll, ih, il = hh_l[i], ll_l[i], ih_l[i], il_l[i]
        big = (hh - ll) >= cfg.move_atr * ab
        pump = big and il < ih
        dump = big and ih < il
        av = avg[i]
        volume_ok = av is not None and av > 0 and s.v[i] >= cfg.vol_mult * av
        rng = h - l
        up_wick = h - max(o, c)
        low_wick = min(o, c) - l
        bear = (pump and volume_ok and rng > 0 and h >= hh - cfg.top_tol * ab
                and (up_wick >= cfg.wick * rng or c <= l + cfg.clos * rng))
        bull = (dump and volume_ok and rng > 0 and l <= ll + cfg.top_tol * ab
                and (low_wick >= cfg.wick * rng or c >= h - cfg.clos * rng))
        if bear:
            pend_s = (h, l, i)
        if bull:
            pend_l = (h, l, i)
        sig_s = cfg.shorts and pend_s is not None and _confirmed(cfg, pend_s, i, c, True)
        sig_l = cfg.longs and pend_l is not None and _confirmed(cfg, pend_l, i, c, False)
        level_s = pend_s[0] if pend_s else None   # максимум свечи истощения (для шорта)
        level_l = pend_l[1] if pend_l else None   # минимум свечи истощения (для лонга)
        if pend_s is not None and i > pend_s[2] and (h > pend_s[0] or i - pend_s[2] >= cfg.confirm_bars):
            pend_s = None
        if pend_l is not None and i > pend_l[2] and (l < pend_l[1] or i - pend_l[2] >= cfg.confirm_bars):
            pend_l = None
        if pos is None and queued is None and (sig_s or sig_l):
            if sig_s:
                direction, stop = -1, level_s + cfg.stop_buf * atr[i]
                pend_s = None
            else:
                direction, stop = 1, level_l - cfg.stop_buf * atr[i]
                pend_l = None
            if cfg.entry == "close":
                pos = _make_pos(direction, c, stop, i, i + 1, cfg)
            else:
                queued = (direction, stop, i)
    return trades


# --------------------------------------------------------------------------- статистика
def _t_stat(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    sd = statistics.stdev(values)
    return statistics.fmean(values) / (sd / math.sqrt(len(values))) if sd > 0 else 0.0


def _day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def stats(trades: list[dict]) -> dict:
    n = len(trades)
    if n == 0:
        return {"n": 0}
    rets = [t["net"] for t in trades]
    gain = sum(r for r in rets if r > 0)
    loss = -sum(r for r in rets if r < 0)
    by_day: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        by_day[_day(t["entry_ts"])].append(t["net"])
    day_means = [statistics.fmean(v) for v in by_day.values()]
    return {"n": n, "win": 100 * sum(1 for r in rets if r > 0) / n,
            "avg": statistics.fmean(rets), "med": statistics.median(rets),
            "pf": gain / loss if loss > 0 else float("inf"),
            "avg_r": statistics.fmean(t["r"] for t in trades),
            "t": _t_stat(rets), "t_days": _t_stat(day_means), "days": len(by_day),
            "coins": len({t["coin"] for t in trades})}


def _fmt(value: float, digits: int = 2, sign: bool = False) -> str:
    if value == float("inf"):
        return "∞"
    return f"{value:+.{digits}f}" if sign else f"{value:.{digits}f}"


def row(label: str, st: dict) -> str:
    if st["n"] == 0:
        return f"| {label} | 0 | — | — | — | — | — | — |"
    return (f"| {label} | {st['n']} | {_fmt(st['win'], 1)}% | {_fmt(st['avg'], 2, True)}% | "
            f"{_fmt(st['med'], 2, True)}% | {_fmt(st['pf'])} | {_fmt(st['t'], 1, True)} | "
            f"{_fmt(st['t_days'], 1, True)} |")


TABLE_HEAD = ("| Выборка | Сделок | Выигрышей | Средняя сделка (после издержек) | Медиана | PF | t (по сделкам) | "
              "t (по дням) |\n|---|---|---|---|---|---|---|---|")


def build_configs() -> list[Cfg]:
    base = Cfg()
    return [
        base,
        replace(base, name="окно 4 свечи", win=4),
        replace(base, name="окно 8 свечей", win=8),
        replace(base, name="импульс 4 ATR", move_atr=4.0),
        replace(base, name="импульс 6 ATR", move_atr=6.0),
        replace(base, name="объём ≥1.5×", vol_mult=1.5),
        replace(base, name="объём ≥3×", vol_mult=3.0),
        replace(base, name="цель 1R", rr=1.0),
        replace(base, name="цель 2R", rr=2.0),
        replace(base, name="удержание до 6 свечей", max_hold=6),
        replace(base, name="удержание до 24 свечей", max_hold=24),
        replace(base, name="вход по открытию следующей свечи", entry="next_open"),
        replace(base, name="без подтверждения (вход на свече истощения)", confirm=False),
        replace(base, name="КОНТРОЛЬ: без фильтра объёма и отказа цены", vol_mult=0.0, wick=0.0),
    ]


def tf_report(tf: str, days: int, coins_loaded: int, results: dict[str, list[dict]], cfgs: list[Cfg]) -> str:
    primary = results[cfgs[0].name]
    control = results[cfgs[-1].name]
    lines = [f"## Таймфрейм {tf} — история {days} дн., монет с данными: {coins_loaded}", ""]
    lines += ["### Основной вариант (правила зафиксированы заранее)", TABLE_HEAD]
    lines.append(row("все сделки", stats(primary)))
    lines.append(row("шорты после пампа", stats([t for t in primary if t["dir"] == "шорт"])))
    lines.append(row("лонги после дампа", stats([t for t in primary if t["dir"] == "лонг"])))
    if primary:
        t_lo = min(t["entry_ts"] for t in primary)
        t_hi = max(t["entry_ts"] for t in primary)
        mid = (t_lo + t_hi) / 2
        lines.append(row("первая половина периода", stats([t for t in primary if t["entry_ts"] < mid])))
        lines.append(row("вторая половина периода", stats([t for t in primary if t["entry_ts"] >= mid])))
        per_day: dict[str, int] = defaultdict(int)
        for t in primary:
            per_day[_day(t["entry_ts"])] += 1
        busiest = {d for d, _ in sorted(per_day.items(), key=lambda kv: -kv[1])[:3]}
        lines.append(row(f"без 3 самых загруженных дней ({', '.join(sorted(busiest))})",
                         stats([t for t in primary if _day(t["entry_ts"]) not in busiest])))
        top5 = {id(t) for t in sorted(primary, key=lambda t: -t["net"])[:5]}
        lines.append(row("без 5 лучших сделок", stats([t for t in primary if id(t) not in top5])))
        by_coin: dict[str, list[float]] = defaultdict(list)
        for t in primary:
            by_coin[t["coin"]].append(t["net"])
        multi = {k: v for k, v in by_coin.items() if len(v) >= 3}
        if multi:
            plus = sum(1 for v in multi.values() if statistics.fmean(v) > 0)
            lines += ["", f"Монет с ≥3 сделками: {len(multi)}, из них со средним плюсом: {plus} "
                          f"({100 * plus / len(multi):.0f}%)."]
        by_month: dict[str, list[float]] = defaultdict(list)
        for t in primary:
            by_month[_day(t["entry_ts"])[:7]].append(t["net"])
        lines += ["", "По месяцам (сделок / средняя сделка):", "",
                  "| Месяц | Сделок | Средняя сделка |", "|---|---|---|"]
        for month in sorted(by_month):
            lines.append(f"| {month} | {len(by_month[month])} | "
                         f"{_fmt(statistics.fmean(by_month[month]), 2, True)}% |")
    lines += ["", "### Соседние настройки и контроль", TABLE_HEAD]
    for cfg in cfgs:
        lines.append(row(cfg.name, stats(results[cfg.name])))
    lines += ["", "Контроль показывает, что даёт простое «шортить пампы с подтверждением» без фильтра объёма "
              "и отказа цены. Если основной вариант не лучше контроля, фильтры ничего не добавляют.", ""]
    p, c = stats(primary), stats(control)
    if p["n"] and c["n"]:
        lines.append(f"Основной: {_fmt(p['avg'], 2, True)}% за сделку, контроль: {_fmt(c['avg'], 2, True)}% за сделку.")
        lines.append("")
    return "\n".join(lines)


HEADER = """# Проверка «истощение пампа/дампа» на истории OKX

Как читать цифры:
- «Средняя сделка» — доходность на сумму позиции после комиссии и проскальзывания, в процентах.
- PF (профит-фактор) — сумма прибылей / сумма убытков. Ниже 1 — стратегия теряет деньги.
- t-статистика: около 2 и выше — результат вряд ли случаен. Сделки внутри одного дня связаны (рынок двигается
  сразу у многих монет), поэтому смотрите на «t (по дням)», она строже.
- Много вариантов настроек всегда дадут разброс просто случайно: решает ОСНОВНОЙ вариант, остальное — проверка
  устойчивости, а не повод выбрать лучший.

Ограничения: берутся только монеты, которые торгуются сейчас (исчезнувшие монеты отсутствуют, это искажает
результат); фандинг не учтён; все сделки считаются независимыми, портфель и плечо не моделируются;
стоп и цель в одной свече — всегда стоп первым.
"""


def telegram_summary(meta: dict, per_tf: list[dict]) -> str:
    lines = [f"🧪 Проверка «истощение пампа/дампа» (OKX, {meta['coins']} монет)"]
    for item in per_tf:
        p, c, ex = item["primary"], item["control"], item["ex_busy"]
        lines.append("")
        lines.append(f"{item['tf']} ({item['days']} дн.):")
        if p["n"] == 0:
            lines.append("сделок не найдено")
            continue
        lines.append(f"сделок {p['n']}, выигрышей {p['win']:.0f}%, средняя сделка {p['avg']:+.2f}% "
                     f"(после комиссий), PF {_fmt(p['pf'])}")
        lines.append(f"t по сделкам {p['t']:+.1f}, t по дням {p['t_days']:+.1f}")
        if ex["n"]:
            lines.append(f"без 3 самых загруженных дней: {ex['avg']:+.2f}%, PF {_fmt(ex['pf'])}")
        if c["n"]:
            lines.append(f"контроль без фильтров: {c['avg']:+.2f}%, PF {_fmt(c['pf'])}")
    if meta.get("quick"):
        lines += ["", "Это быстрая проверка связи на нескольких монетах: делать выводы по этим цифрам нельзя."]
    if meta.get("local"):
        lines += ["", "Полный отчёт: папка backtest_out, файл report.md (откроется сам)."]
    else:
        lines += ["", "Полный отчёт: Actions → этот запуск → Summary и Artifacts."]
    return "\n".join(lines)


def show_local(summary: str, report_path: Path) -> None:
    """На своём компьютере: показать сводку в окне и открыть отчёт."""
    print("\n" + summary, flush=True)
    if os.environ.get("GITHUB_ACTIONS") or os.environ.get("NO_OPEN") == "1":
        return
    try:
        if sys.platform == "win32":
            os.startfile(str(report_path))  # type: ignore[attr-defined]
    except OSError:
        pass


def send_telegram(text: str) -> None:
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        log("Telegram не нужен: сводка выше, полный отчёт в папке backtest_out")
        return
    try:
        response = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                 data={"chat_id": chat, "text": text[:3800]}, timeout=20)
        if response.status_code != 200:
            log(f"Telegram ответил HTTP {response.status_code}")
    except requests.RequestException as error:
        log(f"Telegram недоступен: {type(error).__name__}")


# --------------------------------------------------------------------------- запуск
def write_trades_csv(path: Path, results: dict[str, list[dict]]) -> None:
    fields = ["cfg", "coin", "tf", "dir", "entry_ts", "entry_time_utc", "entry", "stop", "tp", "exit_ts",
              "exit", "reason", "bars", "gross", "net", "risk", "r"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for trades in results.values():
            for t in trades:
                writer.writerow({**t, "entry_time_utc": datetime.fromtimestamp(
                    t["entry_ts"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")})


def main() -> int:
    quick = os.environ.get("MODE", "full").lower() == "quick"
    limit = 8 if quick else int(os.environ.get("SYMBOL_LIMIT", "100"))
    tfs = ["4h"] if quick else [t.strip() for t in os.environ.get("TIMEFRAMES", "4h,1h").split(",") if t.strip()]
    days_for = {"4h": int(os.environ.get("DAYS_4H", "540")), "1h": int(os.environ.get("DAYS_1H", "180"))}
    cost_side = (float(os.environ.get("FEE_PCT", "0.05")) + float(os.environ.get("SLIPPAGE_PCT", "0.02"))) / 100
    exclude = {b.strip().upper() for b in os.environ.get("EXCLUDE_BASES", "USDC,USDG,TUSD,FDUSD").split(",") if b.strip()}
    okx = Okx(float(os.environ.get("REQUEST_INTERVAL", "0.2")))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    cfgs = build_configs()
    now_ms = int(time.time() * 1000)
    try:
        insts = okx.symbols(limit, exclude)
        if not insts:
            raise ApiError("биржа не вернула список контрактов")
        log(f"Режим: {'быстрый' if quick else 'полный'}; контрактов: {len(insts)}; таймфреймы: {tfs}")
        report = [HEADER]
        per_tf: list[dict] = []
        for tf in tfs:
            days = days_for[tf]
            series: dict[str, Series] = {}
            failed = 0
            started = time.time()
            for k, inst in enumerate(insts, 1):
                try:
                    rows = load_rows(okx, inst, tf, days, now_ms)
                except ApiError as error:
                    failed += 1
                    log(f"[{k}/{len(insts)}] {inst} {tf}: пропуск ({error})")
                    continue
                if len(rows) >= 100:
                    series[inst] = Series(rows)
                log(f"[{k}/{len(insts)}] {inst} {tf}: {len(rows)} свечей, прошло {time.time() - started:.0f} с")
            if failed > len(insts) * 0.3:
                raise ApiError(f"не удалось загрузить {failed} из {len(insts)} монет")
            results: dict[str, list[dict]] = {}
            for cfg in cfgs:
                trades: list[dict] = []
                for inst, s in series.items():
                    trades += simulate(cfg, s, inst.replace("-SWAP", ""), tf, cost_side)
                results[cfg.name] = trades
                log(f"{tf} · {cfg.name}: сделок {len(trades)}")
            write_trades_csv(OUT_DIR / f"trades_{tf}.csv", results)
            section = tf_report(tf, days, len(series), results, cfgs)
            report.append(section)
            (OUT_DIR / "report.md").write_text("\n".join(report), encoding="utf-8")
            primary = results[cfgs[0].name]
            per_day: dict[str, int] = defaultdict(int)
            for t in primary:
                per_day[_day(t["entry_ts"])] += 1
            busiest = {d for d, _ in sorted(per_day.items(), key=lambda kv: -kv[1])[:3]}
            per_tf.append({"tf": tf, "days": days, "primary": stats(primary),
                           "control": stats(results[cfgs[-1].name]),
                           "ex_busy": stats([t for t in primary if _day(t["entry_ts"]) not in busiest])})
            print(section, flush=True)
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as handle:
                handle.write("\n".join(report))
        summary = telegram_summary({"coins": len(insts), "quick": quick,
                                    "local": not os.environ.get("GITHUB_ACTIONS")}, per_tf)
        send_telegram(summary)
        log("Готово")
        show_local(summary, OUT_DIR / "report.md")
        return 0
    except Blocked as error:
        log(f"ОШИБКА: биржа блокирует запросы ({error})")
        send_telegram(f"⚠️ Проверка не завершена: OKX заблокировал запросы ({error})")
        return 1
    except ApiError as error:
        log(f"ОШИБКА: {error}")
        if any(word in str(error) for word in ("ConnectionError", "Timeout", "SSLError", "ProxyError")):
            log("Похоже, OKX недоступен из этой сети. Проверь в браузере https://www.okx.com/api/v5/public/time ; "
                "если не открывается — включи VPN или попробуй другой интернет (например, раздачу с телефона).")
        send_telegram(f"⚠️ Проверка не завершена: {str(error)[:300]}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
