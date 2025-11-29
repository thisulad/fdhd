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
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError

# --- CONFIGURATION ---
API_ID = int(os.environ.get('API_ID', 18384173))
API_HASH = os.environ.get('API_HASH', 'bb8b0e6fba49bd873f68ac98547ded2b')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8215053396:AAHhwbn74Bfzv-tvf7oVHNQwb3K54f-8qyo')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '943672693')
SESSION_STRING = os.environ.get('SESSION_STRING', '')
MONGO_URI = os.environ.get('MONGO_URI')
PORT = int(os.environ.get("PORT", 8765))

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
# Ensure these IDs are correct. 
# Private channels MUST be in the account's chat list.
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

# --- CONSTANTS & REGEX ---
BLACKLIST_PAIRS = {
    'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY',
    'RESULTS', 'FEEDBACK', 'RISK', 'REWARD', 'MINUTS', 'MINUTES', 'FREE',
    'ZERO', 'CREATOR', 'FXBUN', 'CREATORFXBUN', 'ZONE', 'ENTRY', 'TARGET',
    'PROFIT', 'LOSS', 'SPOT', 'FUTURE', 'LEVERAGE', 'MARGIN', 'CROSS',
    'ISOLATED', 'SETUP', 'ANALYSIS', 'DISCLAIMER', 'ADVERTISEMENT', 'PROMO'
}

PATTERNS = {
    'pair_strict': r'(?:⚡|🔥|\#|\$)?\s*([A-Z0-9]{2,8}(?:[/-][A-Z0-9]{2,8})?)',
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    'entry': r'(?:Entry|Buy|Sell|Short|Long|EP|Enter|Price|Above|Below|At)[\s\w/\-]*(?:market\s*price|market|cmp|current|price|zone|range|target|at|above|below)?[\s:\-]*(market\s*price|market|cmp|current|[0-9\.,\s\-]+)',
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|Take\s*Profit|T\.P)[\s\n:-]*(wait|wating|[0-9\.,\s\-/✅%]+)',
    'leverage': r'(?:Lev(?:erage|erge|rage)?\s*|Margin\s*)?[:\s\-]*(?:Cross|Iso|Isolated)?\s*([0-9]+[xX]|Low\s*margin|High\s*leverage)',
    'stop_loss': r'(?:SL|Stop\s*Loss)[\s:-]*(wait|wating|[0-9\.]+)'
}

# --- HELPER FUNCTIONS ---

def get_clean_id(id_value):
    """Convert -10012345 to 12345 for consistent matching"""
    try:
        return abs(int(id_value))
    except:
        return 0

def format_uptime(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return f"{int(d)}d {int(h)}h {int(m)}m"

def is_deleted(signal_id):
    if deleted_collection is None: return False
    return deleted_collection.find_one({'id': signal_id}) is not None

def save_signal_to_db(signal_data):
    if signals_collection is None: return False
    if not signal_data or 'id' not in signal_data: return False
    if is_deleted(signal_data['id']): return False
    try:
        existing = signals_collection.find_one({'id': signal_data['id']})
        if existing and signal_data['source'] == 'Backfill' and existing.get('source') != 'Backfill':
            signal_data['source'] = existing['source']
        signals_collection.update_one({'id': signal_data['id']}, {'$set': signal_data}, upsert=True)
        return existing is None
    except Exception as e:
        logger.error(f"⚠️ DB Save Error: {e}")
        return False

def get_recent_history(limit=50):
    if signals_collection is None: return []
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except: return []

def delete_signal(signal_id):
    if signals_collection is None: return
    try:
        signals_collection.delete_one({'id': str(signal_id)})
        deleted_collection.update_one({'id': str(signal_id)}, {'$set': {'id': str(signal_id), 'deleted_at': time.time()}}, upsert=True)
        logger.info(f"🗑️ Deleted signal {signal_id}")
    except: pass

# --- PARSING ENGINE ---
def parse_signal(text, timestamp=None, custom_id=None):
    if not text: return None
    normalized_text = unicodedata.normalize('NFKC', text)
    clean_text = normalized_text.replace('**', '').replace('__', '').replace('`', '').strip()
    
    pair_match = re.search(PATTERNS['pair_strict'], clean_text, re.IGNORECASE)
    if not pair_match: return None
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3: return None
    
    is_major = any(x in raw_pair for x in ['USD', 'BTC', 'ETH', 'SOL', 'BNB'])
    has_direction = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    if not is_major and not has_direction: return None

    ts = timestamp if timestamp else time.time()
    sig_id = str(custom_id) if custom_id else str(int(ts * 1000))

    signal = {
        'id': sig_id, 'pair': raw_pair, 'raw_text': clean_text,
        'timestamp': ts, 'status': 'pending'
    }

    if dir_match := re.search(PATTERNS['direction'], clean_text, re.IGNORECASE):
        d = dir_match.group(1).capitalize()
        signal['direction'] = 'Long' if d == 'Buy' else ('Short' if d == 'Sell' else d)
    else:
        signal['direction'] = 'Unknown'

    if entry_match := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        raw_entry = entry_match.group(1).strip().lstrip('-').strip()
        signal['entry'] = 'Market' if any(x in raw_entry.lower() for x in ['market', 'cmp', 'current']) else raw_entry
    else:
        signal['entry'] = 'Market'

    if target_match := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw_targets = target_match.group(1).replace('\n', ' ')
        if 'wait' in raw_targets.lower() or 'wating' in raw_targets.lower():
            signal['targets'] = ['Wait']
        else:
            signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s\-\s|\s+', raw_targets) if t.strip() and t.strip() not in ['-', 'TP', '%']]
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
async def perform_backfill(client, valid_channels):
    while signals_collection is None: await asyncio.sleep(1)
    logger.info("⏳ Backfilling...")
    
    for channel_id in valid_channels:
        clean_id = get_clean_id(channel_id)
        channel_title = CHANNEL_NAMES_CACHE.get(clean_id, "Unknown Channel")
        try:
            async for message in client.iter_messages(channel_id, limit=30):
                if not message.text: continue
                unique_id = f"tg_{channel_id}_{message.id}"
                if is_deleted(unique_id): continue
                parsed = parse_signal(message.text, timestamp=message.date.timestamp(), custom_id=unique_id)
                if parsed:
                    parsed['type'] = 'VIP' if clean_id in {get_clean_id(v) for v in CHANNELS['vip']} else 'Public'
                    parsed['source'] = channel_title
                    save_signal_to_db(parsed)
        except: pass
    logger.info("✅ Backfill Done.")

async def broadcast_signal(signal_data, delete_action=False):
    if not connected_clients: return
    try:
        msg = json.dumps({"action": "delete", "id": signal_data}) if delete_action else json.dumps({k:v for k,v in signal_data.items() if k != '_id'})
        await asyncio.gather(*[client.send(msg) for client in connected_clients], return_exceptions=True)
    except: pass

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID: return
    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    targets_str = "   ⏳ TP: Wait" if 'Wait' in signal['targets'] else "\n".join([f"   🎯 {t}" for t in signal['targets']])
    sl_str = f"\n🛑 **SL:** {signal.get('stop_loss', 'N/A')}" if signal.get('stop_loss') else ""
    msg = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
           f"📥 **Entry:** {signal['entry']}\n"
           f"⚙️ **Lev:** {signal['leverage']}"
           f"{sl_str}\n\n"
           f"**Targets:**\n{targets_str}\n\n"
           f"🔎 _Source: {signal.get('source', 'Unknown')}_")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": BOT_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload)
    except: pass

async def websocket_handler(websocket):
    connected_clients.add(websocket)
    if signals_collection:
        for old in reversed(get_recent_history(50)): await websocket.send(json.dumps(old))
    try:
        async for message in websocket:
            if not signals_collection: continue
            try:
                data = json.loads(message)
                if data.get('action') == 'delete':
                    delete_signal(data.get('id'))
                    await broadcast_signal(data.get('id'), delete_action=True)
                elif data.get('action') == 'add':
                    payload = data.get('payload')
                    if 'id' not in payload: payload['id'] = f"man_{int(time.time()*1000)}"
                    save_signal_to_db(payload)
                    await broadcast_signal(payload)
                    await send_telegram_alert(payload)
            except: pass
    except: pass
    finally: connected_clients.remove(websocket)

async def main():
    global mongo_client, signals_collection, deleted_collection
    
    # 1. Start Server INSTANTLY (Fixes Render Port Timeout)
    logger.info(f"🚀 Starting Server on port {PORT}...")
    server = await websockets.serve(websocket_handler, "0.0.0.0", PORT, ping_interval=30, ping_timeout=120)

    # 2. Resilient Background Task
    async def bootstrap_app():
        global mongo_client, signals_collection, deleted_collection
        
        while True: # Keep trying to connect forever
            try:
                # DB Connect
                if not mongo_client and MONGO_URI:
                    mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
                    db = mongo_client["crypto_scraper"]
                    signals_collection = db["signals"]
                    deleted_collection = db["deleted_signals"]
                    logger.info("✅ MongoDB Connected")

                # Telegram Connect
                if SESSION_STRING:
                    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
                    logger.info("🔌 Connecting Telegram...")
                    await client.start()
                    logger.info("✅ Telegram Connected")
                    
                    # --- CRITICAL: FIND CHANNELS ---
                    # Scan ALL dialogs to find the private channels
                    logger.info("📇 Scanning Dialogs for Channels...")
                    valid_channels_set = set()
                    
                    # Convert config IDs to clean integers
                    target_ids = {get_clean_id(c) for c in CHANNELS['public'] + CHANNELS['vip']}
                    
                    async for dialog in client.iter_dialogs(limit=None): # No limit, scan all
                        clean_id = get_clean_id(dialog.id)
                        CHANNEL_NAMES_CACHE[clean_id] = dialog.name
                        
                        # If this dialog is in our target list, add it
                        if clean_id in target_ids:
                            valid_channels_set.add(clean_id)
                            logger.info(f"   ✅ Found Channel: {dialog.name} ({clean_id})")

                    # If we missed any, try adding them by raw ID (fallback)
                    for raw_id in target_ids:
                        if raw_id not in valid_channels_set:
                            logger.warning(f"   ⚠️ Warning: Could not find channel {raw_id} in dialog list. Bot might not be a member.")
                            valid_channels_set.add(raw_id) # Force add anyway

                    # Handlers
                    async def process_event(event):
                        global MSGS_SCANNED, SIGNALS_SENT, LAST_SIGNAL_TIME
                        if event.sender_id == BOT_ID or (SCRAPER_PAUSED and not event.is_private): return
                        
                        clean_id = get_clean_id(event.chat_id)
                        if clean_id not in valid_channels_set and not event.is_private: return
                        
                        MSGS_SCANNED += 1
                        unique_id = f"tg_{clean_id}_{event.id}"
                        if is_deleted(unique_id): return
                        
                        parsed = parse_signal(event.text, timestamp=event.date.timestamp(), custom_id=unique_id)
                        if parsed:
                            vip_ids = {get_clean_id(x) for x in CHANNELS['vip']}
                            parsed['type'] = 'VIP' if clean_id in vip_ids else 'Public'
                            parsed['source'] = CHANNEL_NAMES_CACHE.get(clean_id, 'Channel')
                            
                            is_new = save_signal_to_db(parsed)
                            await broadcast_signal(parsed)
                            if is_new:
                                SIGNALS_SENT += 1
                                LAST_SIGNAL_TIME = time.time()
                                await send_telegram_alert(parsed)

                    @client.on(events.NewMessage(pattern='/'))
                    async def admin_handler(event):
                        global SCRAPER_PAUSED, MSGS_SCANNED
                        if event.sender_id != ADMIN_ID: return
                        cmd = event.text.split()[0].lower()
                        if cmd == '/status':
                            msg = f"📊 **Bot Status**\n🟢 Running\nLast Sig: {format_uptime(time.time() - LAST_SIGNAL_TIME) if LAST_SIGNAL_TIME else 'None'} ago"
                            await event.respond(msg)
                        elif cmd == '/reset':
                            try:
                                signals_collection.delete_many({})
                                MSGS_SCANNED = 0
                                await event.respond("🧹 DB Cleared.")
                                asyncio.create_task(perform_backfill(client, valid_channels_set))
                            except: pass

                    client.add_event_handler(process_event, events.NewMessage)
                    client.add_event_handler(process_event, events.MessageEdited)
                    
                    await perform_backfill(client, valid_channels_set)
                    await client.run_until_disconnected()
                    
            except Exception as e:
                logger.error(f"❌ Critical Crash: {e}")
                logger.info("♻️ Restarting loop in 10s...")
                await asyncio.sleep(10)

    asyncio.create_task(bootstrap_app())
    await asyncio.get_running_loop().create_future()

if __name__ == '__main__':
    loggers = ["websockets", "websockets.server", "websockets.protocol", "websockets.asyncio.server", "asyncio"]
    for l in loggers: logging.getLogger(l).setLevel(logging.CRITICAL)
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
