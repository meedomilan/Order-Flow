import asyncio
import json
import logging
import os
import signal
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web

# ============================================================
# Ahmed OF Scanner — Binance USD-M Futures -> Telegram
# نفس منطق Bullish / Bearish OF في مؤشر Pine المرسل
# الفريمات فقط: 15m, 1h, 4h
# ============================================================

BINANCE_REST_BASES = [x.strip().rstrip("/") for x in os.getenv(
    "BINANCE_REST_BASES",
    "https://fapi.binance.com,https://fapi1.binance.com,https://fapi2.binance.com,https://fapi3.binance.com,https://fapi4.binance.com",
).split(",") if x.strip()]
BINANCE_WS = os.getenv("BINANCE_WS", "wss://fstream.binance.com/ws")
TELEGRAM_API = "https://api.telegram.org"
RIYADH = ZoneInfo("Asia/Riyadh")

TIMEFRAMES = ("15m", "1h", "4h")
TF_LABELS = {"15m": "15 دقيقة", "1h": "ساعة", "4h": "4 ساعات"}

# إعدادات Order Flow الأصلية من المؤشر
OF_SWING = int(os.getenv("OF_SWING", "4"))
OF_ATR_LEN = int(os.getenv("OF_ATR_LEN", "14"))
OF_IMPULSE = float(os.getenv("OF_IMPULSE", "0.70"))
OF_USE_VOLUME = os.getenv("OF_USE_VOLUME", "false").lower() == "true"
OF_VOL_LEN = int(os.getenv("OF_VOL_LEN", "20"))
OF_VOL_MULT = float(os.getenv("OF_VOL_MULT", "1.10"))
ZONE_SOURCE = os.getenv("ZONE_SOURCE", "Body + Wick")
BREAK_BY_CLOSE = os.getenv("BREAK_BY_CLOSE", "true").lower() == "true"

HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "180"))
REST_CONCURRENCY = int(os.getenv("REST_CONCURRENCY", "4"))
WS_STREAMS_PER_CONNECTION = int(os.getenv("WS_STREAMS_PER_CONNECTION", "500"))
REST_REQUEST_GAP = float(os.getenv("REST_REQUEST_GAP", "0.08"))
STATE_FILE = Path(os.getenv("STATE_FILE", "/tmp/ahmed_of_state.json"))
PORT = int(os.getenv("PORT", "8080"))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ahmed-of-scanner")


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    closed: bool = True


@dataclass
class ZoneState:
    bull_top: Optional[float] = None
    bull_bottom: Optional[float] = None
    bull_broken: bool = True
    bull_was_inside: bool = False
    bull_tests: int = 0
    bull_created_at: Optional[int] = None

    bear_top: Optional[float] = None
    bear_bottom: Optional[float] = None
    bear_broken: bool = True
    bear_was_inside: bool = False
    bear_tests: int = 0
    bear_created_at: Optional[int] = None

    last_open_time: Optional[int] = None
    current_open_time: Optional[int] = None


STATES: Dict[str, ZoneState] = {}
CANDLE_BUFFERS: Dict[str, List[Candle]] = {}
LAST_ALERT_KEYS: set[str] = set()
SESSION: Optional[aiohttp.ClientSession] = None
STOP_EVENT = asyncio.Event()


def state_key(symbol: str, timeframe: str) -> str:
    return f"{symbol}:{timeframe}"


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    if value >= 1:
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return f"{value:.10f}".rstrip("0").rstrip(".")


def is_pivot_low(candles: List[Candle], candidate: int, swing: int) -> bool:
    if candidate - swing < 0 or candidate + swing >= len(candles):
        return False
    value = candles[candidate].low
    left = [c.low for c in candles[candidate - swing:candidate]]
    right = [c.low for c in candles[candidate + 1:candidate + swing + 1]]
    return value < min(left) and value <= min(right)


def is_pivot_high(candles: List[Candle], candidate: int, swing: int) -> bool:
    if candidate - swing < 0 or candidate + swing >= len(candles):
        return False
    value = candles[candidate].high
    left = [c.high for c in candles[candidate - swing:candidate]]
    right = [c.high for c in candles[candidate + 1:candidate + swing + 1]]
    return value > max(left) and value >= max(right)


def wilder_atr(candles: List[Candle], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(candles)
    if not candles:
        return out
    trs: List[float] = []
    prev_close: Optional[float] = None
    for c in candles:
        tr = c.high - c.low if prev_close is None else max(
            c.high - c.low,
            abs(c.high - prev_close),
            abs(c.low - prev_close),
        )
        trs.append(tr)
        prev_close = c.close

    if len(trs) < length:
        return out
    atr = sum(trs[:length]) / length
    out[length - 1] = atr
    for i in range(length, len(trs)):
        atr = ((atr * (length - 1)) + trs[i]) / length
        out[i] = atr
    return out


def sma(values: List[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0:
        return out
    running = 0.0
    for i, value in enumerate(values):
        running += value
        if i >= length:
            running -= values[i - length]
        if i >= length - 1:
            out[i] = running / length
    return out


def apply_bar_logic(
    symbol: str,
    timeframe: str,
    candles: List[Candle],
    index: int,
    state: ZoneState,
    emit_alerts: bool,
) -> List[Tuple[str, float, float, float, int]]:
    """يعيد قائمة تنبيهات: (side, zone_bottom, zone_top, price, event_time)."""
    alerts: List[Tuple[str, float, float, float, int]] = []
    if index < 0 or index >= len(candles):
        return alerts

    atr_values = wilder_atr(candles[: index + 1], OF_ATR_LEN)
    atr = atr_values[index]
    if atr is None or atr <= 0:
        return alerts

    vol_avg_values = sma([c.volume for c in candles[: index + 1]], OF_VOL_LEN)
    current = candles[index]
    candidate = index - OF_SWING

    # نفس ta.pivotlow(low, ofSwing, ofSwing)
    if candidate >= OF_SWING and is_pivot_low(candles, candidate, OF_SWING):
        pivot = candles[candidate]
        if ZONE_SOURCE == "Full Candle":
            c_top = pivot.high
        else:
            c_top = max(pivot.open, pivot.close)

        if ZONE_SOURCE == "Candle Body":
            c_bottom = min(pivot.open, pivot.close)
        else:
            c_bottom = pivot.low

        impulse = (current.close - c_top) / max(atr, 1e-12)
        avg_at_pivot = vol_avg_values[candidate] if candidate < len(vol_avg_values) else None
        vol_ok = (
            not OF_USE_VOLUME
            or (avg_at_pivot is not None and pivot.volume > avg_at_pivot * OF_VOL_MULT)
        )
        if impulse >= OF_IMPULSE and vol_ok:
            state.bull_top = c_top
            state.bull_bottom = c_bottom
            state.bull_broken = False
            state.bull_tests = 0
            state.bull_was_inside = False
            state.bull_created_at = pivot.open_time

    # نفس ta.pivothigh(high, ofSwing, ofSwing)
    if candidate >= OF_SWING and is_pivot_high(candles, candidate, OF_SWING):
        pivot = candles[candidate]
        if ZONE_SOURCE == "Candle Body":
            c_top = max(pivot.open, pivot.close)
        else:
            c_top = pivot.high

        if ZONE_SOURCE == "Full Candle":
            c_bottom = pivot.low
        else:
            c_bottom = min(pivot.open, pivot.close)

        impulse = (c_bottom - current.close) / max(atr, 1e-12)
        avg_at_pivot = vol_avg_values[candidate] if candidate < len(vol_avg_values) else None
        vol_ok = (
            not OF_USE_VOLUME
            or (avg_at_pivot is not None and pivot.volume > avg_at_pivot * OF_VOL_MULT)
        )
        if impulse >= OF_IMPULSE and vol_ok:
            state.bear_top = c_top
            state.bear_bottom = c_bottom
            state.bear_broken = False
            state.bear_tests = 0
            state.bear_was_inside = False
            state.bear_created_at = pivot.open_time

    inside_bull = (
        not state.bull_broken
        and state.bull_top is not None
        and state.bull_bottom is not None
        and current.high >= state.bull_bottom
        and current.low <= state.bull_top
    )
    inside_bear = (
        not state.bear_broken
        and state.bear_top is not None
        and state.bear_bottom is not None
        and current.high >= state.bear_bottom
        and current.low <= state.bear_top
    )

    bull_touch = inside_bull and not state.bull_was_inside
    bear_touch = inside_bear and not state.bear_was_inside

    if bull_touch:
        state.bull_tests += 1
        if emit_alerts:
            alerts.append((
                "bull",
                float(state.bull_bottom),
                float(state.bull_top),
                current.close,
                int(time.time() * 1000),
            ))
    if bear_touch:
        state.bear_tests += 1
        if emit_alerts:
            alerts.append((
                "bear",
                float(state.bear_bottom),
                float(state.bear_top),
                current.close,
                int(time.time() * 1000),
            ))

    state.bull_was_inside = inside_bull
    state.bear_was_inside = inside_bear

    bull_break_value = current.close if BREAK_BY_CLOSE else current.low
    bear_break_value = current.close if BREAK_BY_CLOSE else current.high
    if (
        not state.bull_broken
        and state.bull_bottom is not None
        and bull_break_value < state.bull_bottom
    ):
        state.bull_broken = True
    if (
        not state.bear_broken
        and state.bear_top is not None
        and bear_break_value > state.bear_top
    ):
        state.bear_broken = True

    state.last_open_time = current.open_time
    return alerts


async def telegram_send(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log.error("ضع TELEGRAM_BOT_TOKEN و TELEGRAM_CHAT_ID في Variables داخل Railway")
        return False
    assert SESSION is not None
    url = f"{TELEGRAM_API}/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(4):
        try:
            async with SESSION.post(url, json=payload, timeout=20) as response:
                data = await response.json(content_type=None)
                if response.status == 200 and data.get("ok"):
                    return True
                retry_after = data.get("parameters", {}).get("retry_after")
                if response.status == 429 and retry_after:
                    await asyncio.sleep(float(retry_after) + 1)
                    continue
                log.error("Telegram error %s: %s", response.status, data)
        except Exception as exc:
            log.warning("Telegram attempt %s failed: %s", attempt + 1, exc)
        await asyncio.sleep(2 ** attempt)
    return False


def build_message(
    symbol: str,
    timeframe: str,
    side: str,
    bottom: float,
    top: float,
    price: float,
    event_time_ms: int,
) -> str:
    dt = datetime.fromtimestamp(event_time_ms / 1000, tz=RIYADH)
    date_text = dt.strftime("%d-%m-%Y %H:%M:%S")
    tv_symbol = f"BINANCE:{symbol}.P"
    tradingview = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"
    binance = f"https://www.binance.com/en/futures/{symbol}"

    if side == "bull":
        title = "🟢 <b>لمس البلوك الشرائي — Bullish OF</b>"
        direction = "📍 دخل السعر منطقة الشراء لأول مرة"
    else:
        title = "🔴 <b>لمس البلوك البيعي — Bearish OF</b>"
        direction = "📍 دخل السعر منطقة البيع لأول مرة"

    return (
        f"{title}\n\n"
        f"💰 العملة: <b>#{symbol}.P</b>\n"
        f"⏰ الفريم: <b>{TF_LABELS.get(timeframe, timeframe)}</b>\n"
        f"💵 السعر: <b>{fmt_price(price)}</b>\n"
        f"🧱 المنطقة: <b>{fmt_price(bottom)} — {fmt_price(top)}</b>\n"
        f"{direction}\n\n"
        f"🕒 {date_text} (السعودية)\n\n"
        f'🔗 <a href="{tradingview}">TradingView</a> | '
        f'<a href="{binance}">Binance</a>'
    )


async def emit_zone_alert(
    symbol: str,
    timeframe: str,
    alert: Tuple[str, float, float, float, int],
) -> None:
    side, bottom, top, price, event_time = alert
    # المفتاح يضمن عدم تكرار نفس دخول المنطقة بعد إعادة رسالة WebSocket لنفس التحديث
    state = STATES[state_key(symbol, timeframe)]
    created = state.bull_created_at if side == "bull" else state.bear_created_at
    tests = state.bull_tests if side == "bull" else state.bear_tests
    dedup_key = f"{symbol}:{timeframe}:{side}:{created}:{tests}"
    if dedup_key in LAST_ALERT_KEYS:
        return
    LAST_ALERT_KEYS.add(dedup_key)
    if len(LAST_ALERT_KEYS) > 20000:
        LAST_ALERT_KEYS.clear()
        LAST_ALERT_KEYS.add(dedup_key)

    text = build_message(symbol, timeframe, side, bottom, top, price, event_time)
    ok = await telegram_send(text)
    if ok:
        log.info("Alert sent: %s %s %s", symbol, timeframe, side)
        await save_state()


async def binance_get_json(path: str, params: Optional[dict] = None, attempts: int = 10):
    """GET عام مع تدوير نطاقات Binance ومعالجة 418/429/451 تلقائيًا."""
    assert SESSION is not None
    last_error: Optional[Exception] = None
    bases = BINANCE_REST_BASES or ["https://fapi.binance.com"]
    for attempt in range(attempts):
        base = bases[attempt % len(bases)]
        url = f"{base}{path}"
        try:
            async with SESSION.get(url, params=params, timeout=35) as response:
                if response.status == 200:
                    return await response.json()
                body = (await response.text())[:300]
                if response.status in (418, 429, 451) or response.status >= 500:
                    retry_after = response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else min(60.0, 2.0 ** min(attempt, 5))
                    wait += random.uniform(0.2, 1.0)
                    log.warning("Binance HTTP %s via %s; retry in %.1fs: %s", response.status, base, wait, body)
                    await asyncio.sleep(wait)
                    continue
                raise RuntimeError(f"Binance HTTP {response.status}: {body}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            wait = min(30.0, 1.5 * (attempt + 1)) + random.uniform(0.1, 0.8)
            log.warning("Binance request failed via %s: %s; retry in %.1fs", base, exc, wait)
            await asyncio.sleep(wait)
    raise RuntimeError(f"All Binance endpoints failed for {path}: {last_error}")


async def get_futures_symbols() -> List[str]:
    data = await binance_get_json("/fapi/v1/exchangeInfo", attempts=15)
    symbols = [
        item["symbol"]
        for item in data.get("symbols", [])
        if item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
    ]
    if not symbols:
        raise RuntimeError("Binance returned no active USDT perpetual contracts")
    return sorted(set(symbols))


async def fetch_klines(
    semaphore: asyncio.Semaphore,
    symbol: str,
    timeframe: str,
) -> Tuple[str, str, List[Candle]]:
    params = {"symbol": symbol, "interval": timeframe, "limit": HISTORY_LIMIT}
    async with semaphore:
        try:
            rows = await binance_get_json("/fapi/v1/klines", params=params, attempts=8)
            await asyncio.sleep(REST_REQUEST_GAP + random.uniform(0.0, 0.04))
            candles = [
                Candle(
                    open_time=int(r[0]),
                    open=float(r[1]),
                    high=float(r[2]),
                    low=float(r[3]),
                    close=float(r[4]),
                    volume=float(r[5]),
                    close_time=int(r[6]),
                    closed=int(r[6]) < int(time.time() * 1000),
                )
                for r in rows
            ]
            return symbol, timeframe, candles
        except Exception as exc:
            log.error("Klines failed %s %s: %s", symbol, timeframe, exc)
            return symbol, timeframe, []


def rebuild_state(symbol: str, timeframe: str, candles: List[Candle]) -> ZoneState:
    state = ZoneState()
    # نبني الحالة تاريخيًا بدون إرسال رسائل قديمة
    for i in range(len(candles)):
        apply_bar_logic(symbol, timeframe, candles, i, state, emit_alerts=False)
    if candles:
        state.current_open_time = candles[-1].open_time
    return state


async def warm_up(symbols: List[str]) -> None:
    semaphore = asyncio.Semaphore(REST_CONCURRENCY)
    jobs = [fetch_klines(semaphore, s, tf) for s in symbols for tf in TIMEFRAMES]
    total = len(jobs)
    done = 0
    log.info("Warming %s symbol/timeframe states...", total)

    for future in asyncio.as_completed(jobs):
        symbol, timeframe, candles = await future
        done += 1
        if candles:
            key = state_key(symbol, timeframe)
            CANDLE_BUFFERS[key] = candles
            STATES[key] = rebuild_state(symbol, timeframe, candles)
        if done % 100 == 0 or done == total:
            log.info("Warm-up progress: %s/%s", done, total)
    await save_state()


async def process_kline_event(data: dict) -> None:
    event = data.get("data", data)
    if event.get("e") != "kline":
        return
    symbol = event.get("s")
    k = event.get("k", {})
    timeframe = k.get("i")
    if timeframe not in TIMEFRAMES or not symbol:
        return

    key = state_key(symbol, timeframe)
    state = STATES.get(key)
    buffer = CANDLE_BUFFERS.get(key)
    if state is None or buffer is None:
        return

    candle = Candle(
        open_time=int(k["t"]),
        open=float(k["o"]),
        high=float(k["h"]),
        low=float(k["l"]),
        close=float(k["c"]),
        volume=float(k["v"]),
        close_time=int(k["T"]),
        closed=bool(k["x"]),
    )

    if buffer and buffer[-1].open_time == candle.open_time:
        buffer[-1] = candle
    else:
        buffer.append(candle)
        if len(buffer) > HISTORY_LIMIT:
            del buffer[:-HISTORY_LIMIT]
        state.current_open_time = candle.open_time

    # نعيد بناء الحالة حتى الشمعة السابقة، ثم نطبق الشمعة الحالية مرة واحدة.
    # هذا يمنع تراكم high/low المتكرر من إفساد حالة wasInside مع تحديثات WebSocket.
    historical = buffer[:-1]
    fresh = rebuild_state(symbol, timeframe, historical) if historical else ZoneState()
    alerts = apply_bar_logic(symbol, timeframe, buffer, len(buffer) - 1, fresh, emit_alerts=True)

    # إذا سبق أن كانت الشمعة الحالية داخل المنطقة في تحديث سابق، لا نكرر اللمس.
    # ننقل حالة الدخول السابقة من الحالة الحية قبل اعتماد fresh.
    if state.current_open_time == candle.open_time:
        filtered = []
        for alert in alerts:
            side = alert[0]
            if side == "bull" and state.bull_was_inside:
                continue
            if side == "bear" and state.bear_was_inside:
                continue
            filtered.append(alert)
        alerts = filtered

    STATES[key] = fresh
    STATES[key].current_open_time = candle.open_time

    for alert in alerts:
        await emit_zone_alert(symbol, timeframe, alert)


async def websocket_worker(streams: List[str], worker_id: int) -> None:
    assert SESSION is not None
    request_id = worker_id * 100000
    while not STOP_EVENT.is_set():
        try:
            async with SESSION.ws_connect(
                BINANCE_WS,
                heartbeat=150,
                receive_timeout=240,
                max_msg_size=2_000_000,
            ) as ws:
                log.info("WS %s connected, subscribing to %s streams", worker_id, len(streams))
                for start in range(0, len(streams), 200):
                    request_id += 1
                    await ws.send_json({
                        "method": "SUBSCRIBE",
                        "params": streams[start:start + 200],
                        "id": request_id,
                    })
                    await asyncio.sleep(0.25)

                async for message in ws:
                    if STOP_EVENT.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(message.data)
                            if "result" in payload and "id" in payload:
                                continue
                            await process_kline_event(payload)
                        except Exception:
                            log.exception("WS %s event processing error", worker_id)
                    elif message.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("WS %s disconnected: %s", worker_id, exc)
        if not STOP_EVENT.is_set():
            await asyncio.sleep(5)


async def save_state() -> None:
    try:
        payload = {
            "states": {k: asdict(v) for k, v in STATES.items()},
            "saved_at": int(time.time()),
        }
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except Exception as exc:
        log.debug("State save skipped: %s", exc)


async def periodic_save() -> None:
    while not STOP_EVENT.is_set():
        await asyncio.sleep(300)
        await save_state()


async def health(_: web.Request) -> web.Response:
    return web.json_response({
        "ok": True,
        "service": "Ahmed OF Scanner",
        "symbols_timeframes": len(STATES),
        "timeframes": TIMEFRAMES,
        "telegram_configured": bool(BOT_TOKEN and CHAT_ID),
    })


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server running on port %s", PORT)
    return runner


async def send_startup_message(symbol_count: int) -> None:
    text = (
        "✅ <b>Ahmed OF Scanner بدأ العمل</b>\n\n"
        f"💹 العقود: <b>{symbol_count} Binance Futures</b>\n"
        "⏰ الفريمات: <b>15m — 1H — 4H</b>\n"
        "🟢 Bullish OF Touch\n"
        "🔴 Bearish OF Touch\n\n"
        "لن تُرسل إشارات RSI أو أي إشارات أخرى."
    )
    await telegram_send(text)


async def main() -> None:
    global SESSION
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram variables are missing; scanner runs but cannot send alerts.")

    timeout = aiohttp.ClientTimeout(total=40)
    connector = aiohttp.TCPConnector(limit=max(30, REST_CONCURRENCY + 20), ttl_dns_cache=300, enable_cleanup_closed=True)
    SESSION = aiohttp.ClientSession(timeout=timeout, connector=connector)
    runner = await start_health_server()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, STOP_EVENT.set)
        except NotImplementedError:
            pass

    try:
        symbols = await get_futures_symbols()
        log.info("Found %s active USDT perpetual contracts", len(symbols))
        await warm_up(symbols)
        await send_startup_message(len(symbols))

        streams = [f"{symbol.lower()}@kline_{tf}" for symbol in symbols for tf in TIMEFRAMES]
        groups = [
            streams[i:i + WS_STREAMS_PER_CONNECTION]
            for i in range(0, len(streams), WS_STREAMS_PER_CONNECTION)
        ]
        log.info("Starting %s WebSocket connections for %s streams", len(groups), len(streams))

        tasks = [
            asyncio.create_task(websocket_worker(group, i + 1))
            for i, group in enumerate(groups)
        ]
        tasks.append(asyncio.create_task(periodic_save()))

        await STOP_EVENT.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await save_state()
        await runner.cleanup()
        if SESSION is not None:
            await SESSION.close()


if __name__ == "__main__":
    asyncio.run(main())
