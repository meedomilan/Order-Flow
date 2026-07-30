import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import websockets

# =========================
# الإعدادات من Railway
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
TIMEFRAMES = [x.strip() for x in os.getenv("TIMEFRAMES", "15m,1h,4h").split(",") if x.strip()]
FAST_EMA = int(os.getenv("FAST_EMA", "8"))
SLOW_EMA = int(os.getenv("SLOW_EMA", "21"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "0"))
SEND_TEST_MESSAGE = os.getenv("SEND_TEST_MESSAGE", "true").lower() == "true"
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "30"))
STREAMS_PER_CONNECTION = int(os.getenv("STREAMS_PER_CONNECTION", "180"))
INIT_CONCURRENCY = int(os.getenv("INIT_CONCURRENCY", "12"))

BINANCE_REST = os.getenv("BINANCE_REST", "https://fapi.binance.com")
BINANCE_WS = os.getenv("BINANCE_WS", "wss://fstream.binance.com/stream?streams=")
SAUDI_TZ = ZoneInfo("Asia/Riyadh")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("orderflow-bot")

# آخر الإغلاقات لكل عملة وفريم
closes = defaultdict(lambda: deque(maxlen=max(SLOW_EMA + 20, 60)))

# حالة التنبيه لمنع التكرار
# المفتاح: symbol|timeframe ، والقيمة: candle_open_time + direction
last_alert_key = {}
last_alert_time = defaultdict(float)

# حالة آخر شمعة
last_candle_open = {}


def ema(values, length):
    """حساب EMA بنفس الفكرة المستخدمة في Pine ta.ema."""
    if not values:
        return None
    alpha = 2.0 / (length + 1.0)
    result = float(values[0])
    for value in values[1:]:
        result = alpha * float(value) + (1.0 - alpha) * result
    return result


def price_text(value):
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return f"{value:.10f}".rstrip("0").rstrip(".")


def tradingview_link(symbol):
    return f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"


def binance_link(symbol):
    base = symbol.replace("USDT", "_USDT")
    return f"https://www.binance.com/en/futures/{base}"


async def telegram_send(session, text):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("BOT_TOKEN أو CHAT_ID غير موجود في Variables")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    async with session.post(url, json=payload, timeout=20) as response:
        body = await response.text()
        if response.status != 200:
            raise RuntimeError(f"Telegram {response.status}: {body}")


async def get_json(session, path, params=None, retries=5):
    url = BINANCE_REST + path
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=25) as response:
                if response.status == 200:
                    return await response.json()
                body = await response.text()
                if response.status in (418, 429):
                    wait = min(60, 5 * (attempt + 1))
                    log.warning("Binance rate limit %s، انتظار %s ثانية", response.status, wait)
                    await asyncio.sleep(wait)
                    continue
                raise RuntimeError(f"Binance {response.status}: {body[:300]}")
        except Exception:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)


async def load_symbols(session):
    info, tickers = await asyncio.gather(
        get_json(session, "/fapi/v1/exchangeInfo"),
        get_json(session, "/fapi/v1/ticker/24hr"),
    )
    volumes = {x["symbol"]: float(x.get("quoteVolume", 0)) for x in tickers}
    result = []
    for item in info["symbols"]:
        symbol = item["symbol"]
        if (
            item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and volumes.get(symbol, 0) >= MIN_QUOTE_VOLUME
        ):
            result.append(symbol)
    result.sort()
    return result


async def initialize_one(session, semaphore, symbol, timeframe):
    async with semaphore:
        data = await get_json(
            session,
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": timeframe, "limit": max(SLOW_EMA + 10, 40)},
        )
        key = f"{symbol}|{timeframe}"
        # نستبعد آخر شمعة المفتوحة، لأنها ستأتي مباشرة من WebSocket
        for candle in data[:-1]:
            closes[key].append(float(candle[4]))
        await asyncio.sleep(0.04)


async def initialize_history(session, symbols):
    semaphore = asyncio.Semaphore(INIT_CONCURRENCY)
    jobs = [
        initialize_one(session, semaphore, symbol, timeframe)
        for symbol in symbols
        for timeframe in TIMEFRAMES
    ]
    total = len(jobs)
    completed = 0
    for future in asyncio.as_completed(jobs):
        try:
            await future
        except Exception as exc:
            log.error("فشل تحميل سجل: %s", exc)
        completed += 1
        if completed % 100 == 0 or completed == total:
            log.info("تهيئة البيانات: %s/%s", completed, total)


async def process_kline(session, payload):
    data = payload.get("data", payload)
    if data.get("e") != "kline":
        return

    symbol = data["s"]
    k = data["k"]
    timeframe = k["i"]
    key = f"{symbol}|{timeframe}"

    candle_open_time = int(k["t"])
    candle_closed = bool(k["x"])
    open_price = float(k["o"])
    high_price = float(k["h"])
    low_price = float(k["l"])
    close_price = float(k["c"])
    volume = float(k["v"])

    history = list(closes[key])
    if len(history) < SLOW_EMA:
        return

    # نحسب EMA باستخدام الإغلاقات السابقة + السعر الحالي الحي
    live_values = history + [close_price]
    fast = ema(live_values, FAST_EMA)
    slow = ema(live_values, SLOW_EMA)
    zone_low = min(fast, slow)
    zone_high = max(fast, slow)

    candle_range = max(high_price - low_price, 1e-12)
    close_position = max(0.0, min(1.0, (close_price - low_price) / candle_range))
    estimated_buy_volume = volume * close_position
    estimated_sell_volume = volume * (1.0 - close_position)
    total = max(estimated_buy_volume + estimated_sell_volume, 1e-12)
    buy_pct = estimated_buy_volume / total * 100.0
    sell_pct = estimated_sell_volume / total * 100.0

    # نفس اتجاه السحابة في المؤشر:
    # شراء: EMA8 >= EMA21 وضغط الشراء >= البيع
    flow_buy = fast >= slow and buy_pct >= sell_pct
    direction = "BUY" if flow_buy else "SELL"

    # السعر وصل إلى السحابة إذا نطاق الشمعة تقاطع معها
    touched = high_price >= zone_low and low_price <= zone_high

    alert_identity = f"{candle_open_time}:{direction}"
    now = time.time()

    if (
        touched
        and last_alert_key.get(key) != alert_identity
        and now - last_alert_time[key] >= ALERT_COOLDOWN_SECONDS
    ):
        last_alert_key[key] = alert_identity
        last_alert_time[key] = now

        emoji = "🟢" if direction == "BUY" else "🔴"
        title = "ORDER FLOW BUY TOUCH" if direction == "BUY" else "ORDER FLOW SELL TOUCH"
        side_ar = "شرائية" if direction == "BUY" else "بيعية"
        saudi_time = datetime.now(SAUDI_TZ).strftime("%d-%m-%Y %H:%M:%S")

        message = (
            f"{emoji} <b>{title}</b>\n\n"
            f"💰 العملة: <b>#{symbol}.P</b>\n"
            f"⏰ الفريم: <b>{timeframe}</b>\n"
            f"💵 السعر: <b>{price_text(close_price)}</b>\n\n"
            f"📊 المنطقة: Order Flow {side_ar}\n"
            f"📍 حدودها: {price_text(zone_low)} — {price_text(zone_high)}\n"
            f"🟢 ضغط الشراء: {buy_pct:.1f}%\n"
            f"🔴 ضغط البيع: {sell_pct:.1f}%\n\n"
            f"🕒 {saudi_time} (السعودية)\n\n"
            f'🔗 <a href="{binance_link(symbol)}">Binance</a> | '
            f'<a href="{tradingview_link(symbol)}">TradingView</a>'
        )
        try:
            await telegram_send(session, message)
            log.info("%s %s %s", title, symbol, timeframe)
        except Exception as exc:
            log.error("فشل إرسال Telegram: %s", exc)
            # نسمح بإعادة المحاولة عند وصول تحديث جديد
            last_alert_key.pop(key, None)

    # عند إغلاق الشمعة نضيف إغلاقها مرة واحدة للتاريخ
    if candle_closed and last_candle_open.get(key) != candle_open_time:
        closes[key].append(close_price)
        last_candle_open[key] = candle_open_time
        # السماح بتنبيه جديد في الشمعة القادمة
        last_alert_key.pop(key, None)


async def stream_group(session, stream_names, group_number):
    url = BINANCE_WS + "/".join(stream_names)
    while True:
        try:
            log.info("فتح WebSocket المجموعة %s بعدد %s stream", group_number, len(stream_names))
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=10000,
                open_timeout=30,
            ) as websocket:
                async for raw in websocket:
                    try:
                        payload = json.loads(raw)
                        await process_kline(session, payload)
                    except Exception as exc:
                        log.exception("خطأ معالجة رسالة المجموعة %s: %s", group_number, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("انقطع WebSocket المجموعة %s: %s — إعادة اتصال", group_number, exc)
            await asyncio.sleep(5)


async def health_logger(symbol_count):
    while True:
        log.info(
            "البوت يعمل | العملات=%s | الفريمات=%s | السجلات=%s",
            symbol_count,
            ",".join(TIMEFRAMES),
            len(closes),
        )
        await asyncio.sleep(300)


async def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("أضف BOT_TOKEN و CHAT_ID في Railway Variables")

    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        symbols = await load_symbols(session)
        if not symbols:
            raise RuntimeError("لم يتم العثور على عقود USDT دائمة")

        log.info("تم العثور على %s عقد USDT دائم", len(symbols))
        await initialize_history(session, symbols)

        if SEND_TEST_MESSAGE:
            await telegram_send(
                session,
                "✅ <b>Order Flow Bot بدأ العمل</b>\n\n"
                f"💰 عدد العملات: <b>{len(symbols)}</b>\n"
                f"⏰ الفريمات: <b>{', '.join(TIMEFRAMES)}</b>\n"
                f"📊 EMA: <b>{FAST_EMA} / {SLOW_EMA}</b>\n"
                "🔔 التنبيه: أول وصول للسعر إلى سحابة Order Flow",
            )

        streams = [
            f"{symbol.lower()}@kline_{timeframe}"
            for symbol in symbols
            for timeframe in TIMEFRAMES
        ]
        groups = [
            streams[i:i + STREAMS_PER_CONNECTION]
            for i in range(0, len(streams), STREAMS_PER_CONNECTION)
        ]

        tasks = [
            asyncio.create_task(stream_group(session, group, index + 1))
            for index, group in enumerate(groups)
        ]
        tasks.append(asyncio.create_task(health_logger(len(symbols))))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("توقف البوت بخطأ قاتل")
        raise
