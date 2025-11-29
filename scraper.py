import re
import asyncio
import logging
import json
import time
import websockets
import sys
import os
import certifi
import aiohttp
import unicodedata
import http
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from motor.motor_asyncio import AsyncIOMotorClient  # FIX #7: Async MongoDB
from pymongo.errors import ConnectionFailure, PyMongoError

# --- CONFIGURATION ---
# FIX #1: Remove default values - credentials MUST come from environment
API_ID = int(os.environ.get('API_ID', 0))
API_HASH = os.environ.get('API_HASH', '')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '')
SESSION_STRING = os.environ.get('SESSION_STRING', '')
MONGO_URI = os.environ.get('MONGO_URI', '')
PORT = int(os.environ.get("PORT", 8765))

# Validate required credentials at startup
if not all([API_ID, API_HASH, BOT_TOKEN, BOT_CHAT_ID, SESSION_STRING]):
    raise ValueError("Missing required environment variables: API_ID, API_HASH, BOT_TOKEN, BOT_CHAT_ID, SESSION_STRING")

# --- GLOBAL STATE ---
START_TIME = time.time()
SCRAPER_PAUSED = False
MSGS_SCANNED = 0
SIGNALS_SENT = 0
LAST_SIGNAL_TIME = None
CHANNEL_NAMES_CACHE = {}

# DB Placeholders
mongo_client = None
signals_collection = None
deleted_collection = None

try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
    ADMIN_ID = int(BOT_CHAT_ID)
except (AttributeError, IndexError, ValueError):
    BOT_ID = 0
    ADMIN_ID = 0

# --- CHANNELS ---
CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'],
    'vip': [-1002138095358, -1001905653511]
}

# --- LOGGING ---
logging.basicConfig(
    format='[%(levelname)s] %(asctime)s: %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

connected_clients = set()
clients_lock = asyncio.Lock()  # FIX #6: Lock for thread-safe set operations

# --- CONSTANTS & REGEX ---
BLACKLIST_PAIRS = {
    'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY',
    'RESULTS', 'FEEDBACK', 'RISK', 'REWARD', 'MINUTS', 'MINUTES', 'FREE',
    'ZERO', 'CREATOR', 'FXBUN', 'CREATORFXBUN', 'ZONE', 'ENTRY', 'TARGET',
    'PROFIT', 'LOSS', 'SPOT', 'FUTURE', 'LEVERAGE', 'MARGIN', 'CROSS',
    'ISOLATED', 'SETUP', 'ANALYSIS', 'DISCLAIMER', 'ADVERTISEMENT', 'PROMO'
}

PATTERNS = {
    'pair_strict': r'(?:\#|\$)?([A-Z0-9]{2,8}(?:/[A-Z0-9]{2,8})?)',
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    'entry': r'(?:Entry|Buy|Sell|Short|Long|EP|Enter|Price|Above|Below|At)(?:\s*(?:Long|Short|Zone|Range|Price|Target|at|Above|Below|\-)?)?[\s:-]*([0-9\.,\s\-]+)',
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|Take\s*Profit|T\.P)[\s\n:-]*(wait|wating|[0-9\.,\s\-/✅]+)',
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s\-]*(?:Cross|Iso|Isolated)?\s*([0-9]+[xX](?:\s*or\s*[xX]?[0-9]+[xX]?)?)',
    'stop_loss': r'(?:SL|Stop\s*Loss)[\s:-]*(wait|wating|[0-9\.]+)'
}

# --- HELPER FUNCTIONS ---

def format_uptime(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return f"{int(d)}d {int(h)}h {int(m)}m"

# FIX #3: Handle both string usernames and integer IDs
def get_clean_id(id_value):
    """Convert channel ID to positive integer, or return 0 for usernames."""
    if id_value is None:
        return 0
    if isinstance(id_value, str):
        # It's a username, not an ID - return hash or 0
        try:
            return abs(int(id_value))
        except ValueError:
            return 0  # Username strings can't be converted
    return abs(int(id_value))

async def is_deleted(signal_id):  # FIX #7: Now async
    if deleted_collection is None:
        return False
    try:
        result = await deleted_collection.find_one({'id': signal_id})
        return result is not None
    except PyMongoError as e:
        logger.error(f"DB error checking deleted: {e}")
        return False

async def save_signal_to_db(signal_data):  # FIX #7: Now async
    if signals_collection is None:
        return False
    if not signal_data or 'id' not in signal_data:
        return False
    if await is_deleted(signal_data['id']):
        return False
    try:
        existing = await signals_collection.find_one({'id': signal_data['id']})
        if existing and signal_data.get('source') == 'Backfill' and existing.get('source') != 'Backfill':
            signal_data['source'] = existing['source']
        await signals_collection.update_one(
            {'id': signal_data['id']},
            {'$set': signal_data},
            upsert=True
        )
        return existing is None
    except PyMongoError as e:
        logger.error(f"⚠️ DB Save Error: {e}")  # FIX #2: Log errors
        return False

async def get_recent_history(limit=50):  # FIX #7: Now async
    if signals_collection is None:
        return []
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return await cursor.to_list(length=limit)
    except PyMongoError as e:
        logger.error(f"DB error getting history: {e}")
        return []

async def delete_signal(signal_id):  # FIX #7: Now async
    # FIX #5: Check both collections for None
    if signals_collection is None or deleted_collection is None:
        return
    try:
        await signals_collection.delete_one({'id': str(signal_id)})
        await deleted_collection.update_one(
            {'id': str(signal_id)},
            {'$set': {'id': str(signal_id), 'deleted_at': time.time()}},
            upsert=True
        )
        logger.info(f"🗑️ Deleted signal {signal_id}")
    except PyMongoError as e:
        logger.error(f"DB error deleting signal: {e}")

# --- PARSING ENGINE ---
def parse_signal(text, timestamp=None, custom_id=None):
    if not text:
        return None
    
    normalized_text = unicodedata.normalize('NFKC', text)
    clean_text = normalized_text.replace('**', '').replace('__', '').replace('`', '').strip()
    
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
        'id': sig_id, 'pair': raw_pair, 'raw_text': clean_text,
        'timestamp': ts, 'status': 'pending'
    }

    if dir_match := re.search(PATTERNS['direction'], clean_text, re.IGNORECASE):
        d = dir_match.group(1).capitalize()
        if d == 'Buy':
            signal['direction'] = 'Long'
        elif d == 'Sell':
            signal['direction'] = 'Short'
        else:
            signal['direction'] = d
    else:
        signal['direction'] = 'Unknown'

    if entry_match := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        raw_entry = entry_match.group(1).strip().lstrip('-').strip()
        signal['entry'] = raw_entry
    else:
        signal['entry'] = 'Market'

    if target_match := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw_targets = target_match.group(1).replace('\n', ' ')
        if 'wait' in raw_targets.lower() or 'wating' in raw_targets.lower():
            signal['targets'] = ['Wait']
        else:
            signal['targets'] = [
                t.strip() for t in re.split(r'\/|,|\s\-\s|\s+', raw_targets) 
                if t.strip() and t.strip() not in ['-', 'TP']
            ]
    else:
        signal['targets'] = []

    if lev_match := re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE):
        signal['leverage'] = lev_match.group(1).strip()
    else:
        signal['leverage'] = 'Standard'
        
    if sl_match := re.search(PATTERNS['stop_loss'], clean_text, re.IGNORECASE):
        signal['stop_loss'] = sl_match.group(1).strip()

    return signal

# --- BACKFILL ---
async def perform_backfill(client, valid_channels, entity_map):
    """
    valid_channels: set of clean (positive) integer IDs
    entity_map: dict mapping clean_id -> original entity (for iteration)
    """
    while signals_collection is None:
        await asyncio.sleep(1)
    
    logger.info("⏳ Backfilling...")
    vip_clean_ids = {get_clean_id(v) for v in CHANNELS['vip']}
    
    for clean_id, entity in entity_map.items():
        channel_title = CHANNEL_NAMES_CACHE.get(clean_id, "Unknown Channel")
        
        try:
            async for message in client.iter_messages(entity, limit=30):
                if not message.text:
                    continue
                unique_id = f"tg_{clean_id}_{message.id}"
                if await is_deleted(unique_id):
                    continue
                
                parsed = parse_signal(message.text, timestamp=message.date.timestamp(), custom_id=unique_id)
                if parsed:
                    # FIX #4: Use clean_id for VIP check (already clean)
                    parsed['type'] = 'VIP' if clean_id in vip_clean_ids else 'Public'
                    parsed['source'] = channel_title
                    await save_signal_to_db(parsed)
        except Exception as e:
            logger.error(f"Backfill error for {channel_title}: {e}")  # FIX #2
    
    logger.info("✅ Backfill Done.")

async def broadcast_signal(signal_data, delete_action=False):
    async with clients_lock:  # FIX #6: Thread-safe access
        if not connected_clients:
            return
        clients_snapshot = set(connected_clients)
    
    try:
        if delete_action:
            msg = json.dumps({"action": "delete", "id": signal_data})
        else:
            msg = json.dumps({k: v for k, v in signal_data.items() if k != '_id'})
        
        # Send to all clients, remove dead ones
        dead_clients = set()
        for client in clients_snapshot:
            try:
                await client.send(msg)
            except websockets.exceptions.ConnectionClosed:
                dead_clients.add(client)
            except Exception as e:
                logger.warning(f"Broadcast error: {e}")
                dead_clients.add(client)
        
        # Clean up dead clients
        if dead_clients:
            async with clients_lock:
                connected_clients.difference_update(dead_clients)
    except Exception as e:
        logger.error(f"Broadcast failed: {e}")

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID:
        return

    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    
    if signal.get('targets') and 'Wait' in signal['targets']:
        targets_str = "   ⏳ TP: Wait"
    else:
        targets_str = "\n".join([f"   🎯 {t}" for t in signal.get('targets', [])])
    
    sl_str = f"\n🛑 **SL:** {signal.get('stop_loss', 'N/A')}" if signal.get('stop_loss') else ""
    
    msg = (
        f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
        f"📥 **Entry:** {signal['entry']}\n"
        f"⚙️ **Lev:** {signal['leverage']}"
        f"{sl_str}\n\n"
        f"**Targets:**\n{targets_str}\n\n"
        f"🔎 _Source: {signal.get('source', 'Unknown')}_"
    )

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": BOT_CHAT_ID, "text": msg, "parse_mode": "Markdown"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Alert Failed: {await response.text()}")
    except Exception as e:
        logger.error(f"Telegram alert error: {e}")

async def websocket_handler(websocket):
    async with clients_lock:  # FIX #6
        connected_clients.add(websocket)
    
    try:
        # Send history
        if signals_collection:
            history = await get_recent_history(50)
            for old_signal in reversed(history):
                await websocket.send(json.dumps(old_signal))
        
        async for message in websocket:
            if not signals_collection:
                continue
            try:
                data = json.loads(message)
                if data.get('action') == 'delete':
                    await delete_signal(data.get('id'))
                    await broadcast_signal(data.get('id'), delete_action=True)
                elif data.get('action') == 'add':
                    payload = data.get('payload', {})
                    if 'id' not in payload:
                        payload['id'] = f"man_{int(time.time() * 1000)}"
                    await save_signal_to_db(payload)
                    await broadcast_signal(payload)
                    await send_telegram_alert(payload)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON from client: {e}")
            except Exception as e:
                logger.error(f"WebSocket message error: {e}")
    except websockets.exceptions.ConnectionClosed:
        pass  # Normal disconnect
    except Exception as e:
        logger.error(f"WebSocket handler error: {e}")
    finally:
        async with clients_lock:  # FIX #6: Safe removal
            connected_clients.discard(websocket)  # discard doesn't raise KeyError

# --- HEALTH CHECK ---
async def health_check(connection, request):
    if request.path == "/health" or request.path == "/":
        return http.HTTPStatus.OK, [], b"OK"
    return None

async def main():
    global mongo_client, signals_collection, deleted_collection
    
    logger.info(f"🚀 Starting Server on port {PORT}...")
    server = await websockets.serve(
        websocket_handler, 
        "0.0.0.0", 
        PORT, 
        process_request=health_check,
        ping_interval=20, 
        ping_timeout=20
    )

    async def bootstrap_app():
        global mongo_client, signals_collection, deleted_collection
        
        if MONGO_URI:
            try:
                # FIX #7: Use async Motor client
                mongo_client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
                db = mongo_client["crypto_scraper"]
                signals_collection = db["signals"]
                deleted_collection = db["deleted_signals"]
                
                # FIX #9: Create indexes for performance
                await signals_collection.create_index("id", unique=True)
                await signals_collection.create_index("timestamp")
                await deleted_collection.create_index("id", unique=True)
                
                logger.info("✅ MongoDB Connected")
            except Exception as e:
                logger.error(f"MongoDB connection failed: {e}")
                return

        if SESSION_STRING:
            try:
                client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
                logger.info("🔌 Connecting Telegram...")
                await client.start()
                
                # Cache Warmup
                try:
                    async for dialog in client.iter_dialogs(limit=100):
                        CHANNEL_NAMES_CACHE[get_clean_id(dialog.id)] = dialog.name
                except Exception as e:
                    logger.warning(f"Dialog cache error: {e}")

                valid_channels_set = set()
                entity_map = {}  # Maps clean_id -> entity for backfill
                
                for chat in CHANNELS['public'] + CHANNELS['vip']:
                    try:
                        entity = await client.get_entity(chat)
                        clean_id = get_clean_id(entity.id)
                        valid_channels_set.add(clean_id)
                        entity_map[clean_id] = entity
                        CHANNEL_NAMES_CACHE[clean_id] = getattr(entity, 'title', 'Unknown')
                    except Exception as e:
                        logger.warning(f"Failed to get entity for {chat}: {e}")

                vip_clean_ids = {get_clean_id(v) for v in CHANNELS['vip']}

                async def process_event(event):
                    global MSGS_SCANNED, SIGNALS_SENT, LAST_SIGNAL_TIME
                    
                    if event.sender_id == BOT_ID:
                        return 
                    if SCRAPER_PAUSED and not event.is_private:
                        return

                    clean_id = get_clean_id(event.chat_id)
                    if clean_id not in valid_channels_set and not event.is_private:
                        return
                    
                    MSGS_SCANNED += 1
                    unique_id = f"tg_{clean_id}_{event.id}"
                    
                    if await is_deleted(unique_id):
                        return
                    
                    parsed = parse_signal(event.text, timestamp=event.date.timestamp(), custom_id=unique_id)
                    if parsed:
                        if clean_id in valid_channels_set:
                            parsed['type'] = 'VIP' if clean_id in vip_clean_ids else 'Public'
                            parsed['source'] = CHANNEL_NAMES_CACHE.get(clean_id, 'Channel')
                        else:
                            parsed['source'] = 'Saved'
                            parsed['type'] = 'Manual'
                        
                        is_new = await save_signal_to_db(parsed)
                        await broadcast_signal(parsed)
                        if is_new: 
                            SIGNALS_SENT += 1
                            LAST_SIGNAL_TIME = time.time()
                            await send_telegram_alert(parsed)

                # FIX #8: Use regex pattern that matches commands at start of message
                @client.on(events.NewMessage(pattern=r'^/\w+'))
                async def admin_handler(event):
                    global SCRAPER_PAUSED, MSGS_SCANNED
                    
                    if event.sender_id != ADMIN_ID:
                        return
                    
                    cmd = event.text.split()[0].lower()
                    
                    if cmd == '/status':
                        uptime = format_uptime(time.time() - START_TIME)
                        last_sig = f"{format_uptime(time.time() - LAST_SIGNAL_TIME)} ago" if LAST_SIGNAL_TIME else "None"
                        
                        async with clients_lock:
                            client_count = len(connected_clients)
                        
                        msg = (
                            f"📊 **Bot Status**\n\n"
                            f"**Status:** {'🔴 PAUSED' if SCRAPER_PAUSED else '🟢 Running'}\n"
                            f"**Uptime:** {uptime}\n"
                            f"**Messages Scanned:** {MSGS_SCANNED}\n"
                            f"**Signals Sent:** {SIGNALS_SENT}\n"
                            f"**Dashboard Clients:** {client_count}\n"
                            f"**Database:** {'✅ Connected' if mongo_client else '❌ Error'}\n"
                            f"**Telegram:** ✅ Connected\n"
                            f"**Last Signal:** {last_sig}"
                        )
                        await event.respond(msg)
                    
                    elif cmd == '/reset':
                        try:
                            await signals_collection.delete_many({})
                            MSGS_SCANNED = 0
                            await event.respond("🧹 **Database Cleared!** Re-scanning...")
                            asyncio.create_task(perform_backfill(client, valid_channels_set, entity_map))
                        except Exception as e:
                            logger.error(f"Reset failed: {e}")
                            await event.respond(f"❌ Reset Failed: {e}")
                    
                    elif cmd == '/pause':
                        SCRAPER_PAUSED = True
                        await event.respond("⏸ Paused")
                    
                    elif cmd == '/resume':
                        SCRAPER_PAUSED = False
                        await event.respond("▶️ Resumed")

                client.add_event_handler(process_event, events.NewMessage)
                client.add_event_handler(process_event, events.MessageEdited)
                
                async def del_msg(event):
                    if not event.chat_id:
                        return
                    clean = get_clean_id(event.chat_id)
                    if clean in valid_channels_set:
                        for mid in event.deleted_ids:
                            uid = f"tg_{clean}_{mid}"
                            await delete_signal(uid)
                            await broadcast_signal(uid, delete_action=True)

                client.add_event_handler(del_msg, events.MessageDeleted)
                await perform_backfill(client, valid_channels_set, entity_map)
                await client.run_until_disconnected()
            
            except Exception as e:
                logger.error(f"Telegram bootstrap failed: {e}")

    asyncio.create_task(bootstrap_app())
    await asyncio.get_running_loop().create_future()

if __name__ == '__main__':
    loggers = ["websockets", "websockets.server", "websockets.protocol", "websockets.asyncio.server", "asyncio"]
    for l in loggers:
        logging.getLogger(l).setLevel(logging.CRITICAL)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
