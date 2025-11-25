import re
import asyncio
import logging
import json
import time
import websockets
import os
import urllib.request
import urllib.parse
import certifi
from datetime import timedelta
from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# --- CONFIGURATION ---
API_ID = int(os.environ.get('API_ID', 0))
API_HASH = os.environ.get('API_HASH', '')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '')
SESSION_STRING = os.environ.get('SESSION_STRING', '')
MONGO_URI = os.environ.get('MONGO_URI', '')
PORT = int(os.environ.get("PORT", 8765))
HTTP_PORT = int(os.environ.get("HTTP_PORT", 10000))  # For Render health check
OWNER_ID = int(os.environ.get('OWNER_ID', 0))  # Your Telegram User ID for bot commands

# Global State
mongo_client = None
signals_collection = None
deleted_collection = None
connected_clients = set()
telegram_client = None
bot_client = None

# --- BOT STATE ---
class BotState:
    def __init__(self):
        self.start_time = time.time()
        self.is_paused = False
        self.messages_scanned = 0
        self.signals_sent = 0
        self.last_signal_time = None
        
    def get_uptime(self):
        delta = timedelta(seconds=int(time.time() - self.start_time))
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m {seconds}s"

state = BotState()

# Safety: Extract Bot ID
try:
    BOT_ID = int(BOT_TOKEN.split(':')[0]) if BOT_TOKEN else 0
except (AttributeError, IndexError, ValueError):
    BOT_ID = 0

CHANNELS = {
    'public': ['Binancesignalwithishara'],
    'vip': [-1002138095358]
}

# --- LOGGING SETUP ---
logging.basicConfig(
    format='[%(levelname)s] %(asctime)s: %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- CONSTANTS & REGEX ---
BLACKLIST_PAIRS = {
    'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY',
    'RESULTS', 'FEEDBACK', 'RISK', 'REWARD', 'MINUTS', 'MINUTES', 'FREE',
    'ZERO', 'CREATOR', 'FXBUN', 'CREATORFXBUN', 'ZONE', 'ENTRY', 'TARGET',
    'PROFIT', 'LOSS', 'SPOT', 'FUTURE', 'LEVERAGE', 'MARGIN', 'CROSS',
    'ISOLATED', 'SETUP', 'ANALYSIS', 'DISCLAIMER'
}

PATTERNS = {
    'pair_strict': r'(?:\#|\$)?([A-Z0-9]{2,8}(?:/[A-Z0-9]{2,8})?)',
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    'entry': r'(?:Entry|Buy|EP|Enter)(?:\s*(?:Zone|Range|Price|Target)?)?[\s:-]*([0-9\.,\s\-]+)',
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|T\.P)[\s\n:-]*([0-9\.,\s\-/✅]+)',
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s]*((?:Cross|Iso|Isolated)?\s*[0-9]+x?)',
}

# --- HELPER FUNCTIONS ---
def is_db_connected():
    """Safe check if database is connected"""
    return signals_collection is not None

def is_deleted(signal_id):
    if deleted_collection is None:
        return False
    try:
        return deleted_collection.find_one({'id': signal_id}) is not None
    except PyMongoError as e:
        logger.error(f"DB Error checking deleted: {e}")
        return False

def save_signal_to_db(signal_data):
    if signals_collection is None:
        return False
    if not signal_data or 'id' not in signal_data:
        return False
    if is_deleted(signal_data['id']):
        return False
    try:
        existing = signals_collection.find_one({'id': signal_data['id']})
        signals_collection.update_one(
            {'id': signal_data['id']},
            {'$set': signal_data},
            upsert=True
        )
        return existing is None
    except PyMongoError as e:
        logger.error(f"⚠️ DB Save Error: {e}")
        return False

def get_recent_history(limit=50):
    if signals_collection is None:
        return []
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except PyMongoError as e:
        logger.error(f"DB Error fetching history: {e}")
        return []

def delete_signal(signal_id):
    if signals_collection is None or deleted_collection is None:
        return
    try:
        signals_collection.delete_one({'id': str(signal_id)})
        deleted_collection.update_one(
            {'id': str(signal_id)},
            {'$set': {'id': str(signal_id), 'deleted_at': time.time()}},
            upsert=True
        )
        logger.info(f"🗑️ Deleted signal {signal_id}")
    except PyMongoError as e:
        logger.error(f"DB Error deleting signal: {e}")

def parse_signal(text, timestamp=None, custom_id=None):
    if not text:
        return None
    clean_text = text.replace('**', '').replace('__', '').replace('`', '').strip()

    pair_match = re.search(PATTERNS['pair_strict'], clean_text, re.IGNORECASE)
    if not pair_match:
        return None

    raw_pair = pair_match.group(1).upper().replace('/', '')
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3:
        return None

    is_major = any(x in raw_pair for x in ['USD', 'BTC', 'ETH', 'SOL', 'BNB'])
    has_direction = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    if not is_major and not has_direction:
        return None

    ts = timestamp if timestamp else time.time()
    sig_id = str(custom_id) if custom_id else str(int(ts * 1000))

    signal = {
        'id': sig_id,
        'pair': raw_pair,
        'raw_text': clean_text,
        'timestamp': ts,
        'status': 'pending'
    }

    if dir_match := re.search(PATTERNS['direction'], clean_text, re.IGNORECASE):
        signal['direction'] = dir_match.group(1).capitalize()
    elif 'buy' in clean_text.lower():
        signal['direction'] = 'Long'
    elif 'sell' in clean_text.lower():
        signal['direction'] = 'Short'
    else:
        signal['direction'] = 'Unknown'

    if entry_match := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        signal['entry'] = entry_match.group(1).strip()
    else:
        signal['entry'] = 'Market'

    if target_match := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw_targets = target_match.group(1).replace('\n', ' ')
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s+', raw_targets) if t.strip()]
    else:
        signal['targets'] = []

    if lev_match := re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE):
        content = lev_match.group(0)
        signal['leverage'] = content.split(':', 1)[1].strip() if ':' in content else content
    else:
        signal['leverage'] = 'Standard'

    return signal

def get_clean_id(id_value):
    return abs(int(id_value)) if id_value is not None else 0

# --- STATS FUNCTIONS ---
def get_24h_stats():
    """Get win/loss stats from the last 24 hours"""
    if signals_collection is None:
        return None
    try:
        cutoff = time.time() - 86400

        pipeline = [
            {'$match': {'timestamp': {'$gte': cutoff}}},
            {'$group': {
                '_id': None,
                'total': {'$sum': 1},
                'wins': {'$sum': {'$cond': [{'$regexMatch': {'input': {'$ifNull': ['$status', '']}, 'regex': '^TP'}}, 1, 0]}},
                'losses': {'$sum': {'$cond': [{'$eq': ['$status', 'lost']}, 1, 0]}},
                'pending': {'$sum': {'$cond': [{'$eq': ['$status', 'pending']}, 1, 0]}}
            }}
        ]

        result = list(signals_collection.aggregate(pipeline))
        if result:
            return result[0]
        return {'total': 0, 'wins': 0, 'losses': 0, 'pending': 0}
    except PyMongoError as e:
        logger.error(f"Stats error: {e}")
        return None

# --- TELEGRAM NOTIFICATION ---
def send_via_http(token, chat_id, message, parse_mode="Markdown"):
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": message, "parse_mode": parse_mode}
        encoded = urllib.parse.urlencode(data).encode('utf-8')
        req = urllib.request.Request(url, data=encoded)
        with urllib.request.urlopen(req, timeout=10):
            return True
    except Exception as e:
        logger.error(f"HTTP Send Error: {e}")
        return False

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID:
        return
    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    targets_str = "\n".join([f"   🎯 {t}" for t in signal.get('targets', [])])
    if not targets_str:
        targets_str = "   Open Target"
    msg = (
        f"⚡ *{signal['pair']}* {emoji} *{signal['direction'].upper()}*\n\n"
        f"📥 *Entry:* {signal['entry']}\n"
        f"⚙️ *Lev:* {signal['leverage']}\n\n"
        f"*Targets:*\n{targets_str}\n\n"
        f"🔎 _Source: {signal.get('source', 'Unknown')}_"
    )
    await asyncio.to_thread(send_via_http, BOT_TOKEN, BOT_CHAT_ID, msg)

# --- BOT COMMAND CENTER ---
async def handle_bot_command(event):
    """Handle commands sent to the bot"""
    text = event.text.strip()
    sender_id = event.sender_id
    
    # Only allow owner to use commands
    if OWNER_ID and sender_id != OWNER_ID:
        await event.reply("⛔ Unauthorized. You are not the owner of this bot.")
        return
    
    # /start - Welcome message
    if text == '/start':
        await event.reply(
            "🤖 *SignalScraper Command Center*\n\n"
            "Available commands:\n"
            "`/status` - Bot status & uptime\n"
            "`/pause` - Toggle signal broadcasting\n"
            "`/stats` - 24h win/loss statistics\n"
            "`/add PAIR DIR [LEV]` - Add manual signal\n"
            "`/channels` - List monitored channels\n"
            "`/clients` - Show connected dashboards\n"
            "`/help` - Show this help",
            parse_mode='md'
        )
        return

    # /status - Get bot status
    if text == '/status':
        status_emoji = "⏸️ Paused" if state.is_paused else "🟢 Running"
        db_status = "✅ Connected" if is_db_connected() else "❌ Disconnected"
        ws_clients = len(connected_clients)
        tg_status = "✅ Connected" if (telegram_client and telegram_client.is_connected()) else "❌ Disconnected"

        response = (
            f"📊 *Bot Status*\n\n"
            f"*Status:* {status_emoji}\n"
            f"*Uptime:* `{state.get_uptime()}`\n"
            f"*Messages Scanned:* `{state.messages_scanned}`\n"
            f"*Signals Sent:* `{state.signals_sent}`\n"
            f"*Dashboard Clients:* `{ws_clients}`\n"
            f"*Database:* {db_status}\n"
            f"*Telegram:* {tg_status}\n"
        )
        if state.last_signal_time:
            time_ago = int(time.time() - state.last_signal_time)
            mins, secs = divmod(time_ago, 60)
            if mins > 0:
                response += f"*Last Signal:* `{mins}m {secs}s ago`"
            else:
                response += f"*Last Signal:* `{secs}s ago`"

        await event.reply(response, parse_mode='md')
        return

    # /pause - Toggle pause
    if text == '/pause':
        state.is_paused = not state.is_paused
        status = "⏸️ *Paused* - Signals will NOT be broadcast" if state.is_paused else "▶️ *Resumed* - Signals will be broadcast"
        await event.reply(f"Signal broadcasting: {status}", parse_mode='md')
        logger.info(f"Bot {'paused' if state.is_paused else 'resumed'} by owner")
        return

    # /stats - Get 24h statistics
    if text == '/stats':
        stats = get_24h_stats()
        if stats:
            total = stats.get('total', 0)
            wins = stats.get('wins', 0)
            losses = stats.get('losses', 0)
            pending = stats.get('pending', 0)
            resolved = wins + losses
            win_rate = round((wins / resolved) * 100, 1) if resolved > 0 else 0

            response = (
                f"📈 *24h Statistics*\n\n"
                f"*Total Signals:* `{total}`\n"
                f"*Wins (TP Hit):* `{wins}` ✅\n"
                f"*Losses:* `{losses}` ❌\n"
                f"*Pending:* `{pending}` ⏳\n"
                f"*Win Rate:* `{win_rate}%`"
            )
        else:
            response = "❌ Could not fetch statistics. Database may be unavailable."

        await event.reply(response, parse_mode='md')
        return

    # /add PAIR DIRECTION [LEVERAGE] - Add manual signal
    if text.startswith('/add '):
        parts = text.split()[1:]

        if len(parts) < 2:
            await event.reply(
                "📝 *Usage:* `/add PAIR DIRECTION [LEVERAGE]`\n\n"
                "*Examples:*\n"
                "`/add BTCUSDT Long`\n"
                "`/add ETHUSDT Short 20x`\n"
                "`/add SOLUSDT Long 10x`",
                parse_mode='md'
            )
            return

        pair = parts[0].upper().replace('/', '')
        direction = parts[1].capitalize()
        leverage = parts[2] if len(parts) > 2 else '20x'

        if direction not in ['Long', 'Short']:
            await event.reply("❌ Direction must be `Long` or `Short`", parse_mode='md')
            return

        # Create manual signal
        sig_id = f"cmd_{int(time.time() * 1000)}"
        signal = {
            'id': sig_id,
            'pair': pair,
            'direction': direction,
            'entry': 'Market',
            'targets': [],
            'leverage': leverage,
            'timestamp': time.time(),
            'status': 'pending',
            'type': 'Manual',
            'source': 'Bot Command'
        }

        is_new = save_signal_to_db(signal)
        if is_new:
            await broadcast_signal(signal)
            await send_telegram_alert(signal)
            state.signals_sent += 1
            state.last_signal_time = time.time()

            await event.reply(
                f"✅ *Signal Added*\n\n"
                f"*Pair:* `{pair}`\n"
                f"*Direction:* `{direction}`\n"
                f"*Leverage:* `{leverage}`\n"
                f"*ID:* `{sig_id}`",
                parse_mode='md'
            )
        else:
            await event.reply("❌ Failed to add signal. Check database connection.")
        return

    # /channels - List monitored channels
    if text == '/channels':
        public = ', '.join(str(c) for c in CHANNELS['public']) or 'None'
        vip = ', '.join(str(c) for c in CHANNELS['vip']) or 'None'

        await event.reply(
            f"📡 *Monitored Channels*\n\n"
            f"*Public:* `{public}`\n"
            f"*VIP:* `{vip}`",
            parse_mode='md'
        )
        return

    # /clients - Show connected dashboard clients
    if text == '/clients':
        count = len(connected_clients)
        await event.reply(
            f"🖥️ *Connected Dashboards:* `{count}`",
            parse_mode='md'
        )
        return

    # /help - Show help
    if text == '/help':
        await event.reply(
            "🎮 *Command Center*\n\n"
            "`/status` - Bot status & uptime\n"
            "`/pause` - Toggle signal broadcasting\n"
            "`/stats` - 24h win/loss statistics\n"
            "`/add PAIR DIR [LEV]` - Add manual signal\n"
            "`/channels` - List monitored channels\n"
            "`/clients` - Connected dashboards\n"
            "`/help` - This help message",
            parse_mode='md'
        )
        return

# --- WEBSOCKET FUNCTIONS ---
async def broadcast_signal(signal_data, delete_action=False):
    if not connected_clients:
        return
    try:
        if delete_action:
            msg = json.dumps({"action": "delete", "id": signal_data})
        else:
            msg = json.dumps({k: v for k, v in signal_data.items() if k != '_id'})

        dead_clients = set()
        for client in connected_clients.copy():
            try:
                await client.send(msg)
            except:
                dead_clients.add(client)
        
        connected_clients.difference_update(dead_clients)
    except Exception as e:
        logger.error(f"Broadcast error: {e}")

async def websocket_handler(websocket):
    logger.info("✅ Dashboard Connected")
    connected_clients.add(websocket)

    try:
        if signals_collection is not None:
            history = get_recent_history(50)
            for old_signal in reversed(history):
                await websocket.send(json.dumps(old_signal))

        async for message in websocket:
            if signals_collection is None:
                continue
            try:
                data = json.loads(message)
                action = data.get('action')

                if action == 'delete':
                    delete_signal(data.get('id'))
                    await broadcast_signal(data.get('id'), delete_action=True)

                elif action == 'add':
                    logger.info("➕ Manual Signal from Dashboard")
                    payload = data.get('payload')
                    if 'id' not in payload:
                        payload['id'] = f"man_{int(time.time() * 1000)}"
                    payload['source'] = 'Dashboard'

                    if save_signal_to_db(payload):
                        await broadcast_signal(payload)
                        await send_telegram_alert(payload)
                        state.signals_sent += 1
                        state.last_signal_time = time.time()

            except json.JSONDecodeError:
                logger.warning("Invalid JSON received from websocket")

    except websockets.exceptions.ConnectionClosed:
        logger.info("Dashboard disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        connected_clients.discard(websocket)

# --- BACKFILL ---
async def perform_backfill(client, valid_channels):
    timeout = 30
    start = time.time()

    while signals_collection is None:
        if time.time() - start > timeout:
            logger.warning("Backfill timeout - DB not available")
            return
        await asyncio.sleep(1)

    logger.info("⏳ Backfilling history...")
    count = 0
    vip_clean_ids = set()
    for v in CHANNELS['vip']:
        if isinstance(v, int):
            vip_clean_ids.add(get_clean_id(v))

    for channel_id in valid_channels:
        try:
            async for message in client.iter_messages(channel_id, limit=50):
                if not message.text or "🔎 _Source:" in message.text:
                    continue

                unique_id = f"tg_{channel_id}_{message.id}"
                if is_deleted(unique_id):
                    continue

                parsed = parse_signal(
                    message.text,
                    timestamp=message.date.timestamp(),
                    custom_id=unique_id
                )

                if parsed:
                    parsed['type'] = 'VIP' if (channel_id in vip_clean_ids) else 'Public'
                    parsed['source'] = 'Backfill'
                    if save_signal_to_db(parsed):
                        count += 1

        except Exception as e:
            logger.error(f"Backfill error for {channel_id}: {e}")

    logger.info(f"✅ Backfill Done. Synced {count} signals.")

# --- HTTP HEALTH CHECK ---
async def health_check(request):
    """Simple health check endpoint for Render"""
    return web.Response(text=json.dumps({
        "status": "ok",
        "uptime": state.get_uptime(),
        "signals_sent": state.signals_sent,
        "db_connected": is_db_connected(),
        "ws_clients": len(connected_clients)
    }), content_type='application/json')

async def start_http_server():
    """Start HTTP server for health checks"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HTTP_PORT)
    await site.start()
    logger.info(f"🌐 HTTP Health Check running on port {HTTP_PORT}")

# --- MAIN ---
async def main():
    global mongo_client, signals_collection, deleted_collection, telegram_client, bot_client

    # 1. START HTTP SERVER FOR RENDER HEALTH CHECK
    await start_http_server()

    # 2. START WEBSOCKET SERVER
    logger.info(f"🚀 Starting WebSocket Server on port {PORT}...")
    server = await websockets.serve(
        websocket_handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=20
    )

    # 3. BOOTSTRAP
    async def bootstrap_app():
        global mongo_client, signals_collection, deleted_collection, telegram_client, bot_client

        # Connect MongoDB
        logger.info("🔌 Connecting to MongoDB...")
        if not MONGO_URI:
            logger.warning("⚠️ MONGO_URI not set - running without database")
        else:
            try:
                mongo_client = MongoClient(
                    MONGO_URI,
                    tlsCAFile=certifi.where(),
                    serverSelectionTimeoutMS=5000
                )
                db = mongo_client["crypto_scraper"]
                signals_collection = db["signals"]
                deleted_collection = db["deleted_signals"]

                # Create indexes
                signals_collection.create_index([("id", 1)], unique=True)
                signals_collection.create_index([("timestamp", -1)])
                deleted_collection.create_index([("id", 1)], unique=True)

                await asyncio.to_thread(mongo_client.admin.command, 'ping')
                logger.info("✅ MongoDB Connected")
            except Exception as e:
                logger.critical(f"❌ DB Fail: {e}")

        # Start Bot Client for Command Center
        if BOT_TOKEN:
            try:
                bot_client = TelegramClient('bot', API_ID, API_HASH)
                await bot_client.start(bot_token=BOT_TOKEN)
                logger.info("✅ Bot Client Connected (Command Center)")

                # Add command handler
                @bot_client.on(events.NewMessage(incoming=True))
                async def bot_command_handler(event):
                    if event.text and event.text.startswith('/'):
                        await handle_bot_command(event)

                logger.info(f"🎮 Send commands to your bot. Owner ID: {OWNER_ID}")
            except Exception as e:
                logger.error(f"❌ Bot Client Error: {e}")

        # Connect Telegram User Client for Scraping
        if not SESSION_STRING:
            logger.warning("⚠️ SESSION_STRING not set - Telegram scraping disabled")
            # Keep bot running
            if bot_client:
                await bot_client.run_until_disconnected()
            return

        try:
            telegram_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            logger.info("🔌 Connecting to Telegram (Scraper)...")
            await telegram_client.start()
            logger.info("✅ Telegram Scraper Connected")

            # Resolve channels
            valid_channels_set = set()
            for chat in CHANNELS['public'] + CHANNELS['vip']:
                try:
                    entity = await telegram_client.get_entity(chat)
                    valid_channels_set.add(get_clean_id(entity.id))
                    logger.info(f"📡 Monitoring: {chat}")
                except Exception as e:
                    logger.warning(f"Could not resolve channel {chat}: {e}")

            # --- EVENT HANDLERS ---
            async def process_event(event):
                state.messages_scanned += 1

                if event.sender_id == BOT_ID:
                    return

                clean_id = get_clean_id(event.chat_id)
                if clean_id not in valid_channels_set and not event.is_private:
                    return

                # Skip if paused
                if state.is_paused:
                    return

                unique_id = f"tg_{clean_id}_{event.id}"
                if is_deleted(unique_id):
                    return

                parsed = parse_signal(
                    event.text,
                    timestamp=event.date.timestamp(),
                    custom_id=unique_id
                )

                if parsed:
                    if clean_id in valid_channels_set:
                        vip_ids = [get_clean_id(x) for x in CHANNELS['vip']]
                        parsed['type'] = 'VIP' if (clean_id in vip_ids) else 'Public'
                        try:
                            chat_obj = await event.get_chat()
                            parsed['source'] = getattr(chat_obj, 'title', 'Channel')
                        except:
                            parsed['source'] = 'Channel'
                    else:
                        parsed['source'] = 'Saved/Private'
                        parsed['type'] = 'Manual'

                    is_new = save_signal_to_db(parsed)
                    await broadcast_signal(parsed)

                    if is_new:
                        await send_telegram_alert(parsed)
                        state.signals_sent += 1
                        state.last_signal_time = time.time()
                        logger.info(f"📤 Signal: {parsed['pair']} {parsed['direction']}")

            telegram_client.add_event_handler(process_event, events.NewMessage)
            telegram_client.add_event_handler(process_event, events.MessageEdited)

            # Delete handler
            async def del_msg(event):
                if not event.chat_id:
                    return
                clean_id = get_clean_id(event.chat_id)
                if clean_id in valid_channels_set:
                    for msg_id in event.deleted_ids:
                        unique_id = f"tg_{clean_id}_{msg_id}"
                        delete_signal(unique_id)
                        await broadcast_signal(unique_id, delete_action=True)

            telegram_client.add_event_handler(del_msg, events.MessageDeleted)

            # Backfill
            await perform_backfill(telegram_client, valid_channels_set)

            logger.info("🎮 Bot ready! Send /help to your bot for commands")

            # Keep both clients running
            await asyncio.gather(
                telegram_client.run_until_disconnected(),
                bot_client.run_until_disconnected() if bot_client else asyncio.sleep(float('inf'))
            )

        except Exception as e:
            logger.error(f"Telegram Error: {e}")

    asyncio.create_task(bootstrap_app())
    await asyncio.get_running_loop().create_future()

if __name__ == '__main__':
    # Suppress noisy loggers
    for logger_name in ["websockets", "websockets.server", "websockets.protocol",
                        "websockets.asyncio.server", "asyncio", "aiohttp"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
