import re
import asyncio
import logging
import json
import time
import websockets
import sys
import os
import urllib.request
import urllib.parse
import http
import certifi
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

# Safety: Extract Bot ID
try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
except (AttributeError, IndexError, ValueError):
    BOT_ID = 0

# Channels to monitor
CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'], 
    'vip': [-1002138095358] 
}

# --- LOGGING SETUP ---
logging.basicConfig(
    format='[%(levelname)s] %(asctime)s: %(message)s', 
    level=logging.INFO,
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- MONGODB CONNECTION ---
if not MONGO_URI:
    logger.critical("❌ FATAL: MONGO_URI is missing.")
    sys.exit(1)

try:
    mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
    db = mongo_client["crypto_scraper"]
    signals_collection = db["signals"]
    deleted_collection = db["deleted_signals"]
    mongo_client.admin.command('ping')
    logger.info("✅ MongoDB Atlas Connected Successfully")
except Exception as e:
    logger.critical(f"❌ MongoDB Connection Failed: {e}")
    sys.exit(1)

connected_clients = set()

# --- CONSTANTS & REGEX ---
BLACKLIST_PAIRS = {
    'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY', 
    'RESULTS', 'FEEDBACK', 'RISK', 'REWARD', 'MINUTS', 'MINUTES', 'FREE', 
    'ZERO', 'CREATOR', 'FXBUN', 'CREATORFXBUN', 'ZONE', 'ENTRY', 'TARGET', 
    'PROFIT', 'LOSS', 'SPOT', 'FUTURE', 'LEVERAGE', 'MARGIN', 'CROSS', 
    'ISOLATED', 'SETUP', 'ANALYSIS', 'DISCLAIMER'
}

PATTERNS = {
    # STRICTER PAIR REGEX
    'pair_strict': r'(?:\#|\$)?([A-Z0-9]{2,8}(?:/[A-Z0-9]{2,8})?)',
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    'entry': r'(?:Entry|Buy|EP|Enter)(?:\s*(?:Zone|Range|Price|Target)?)?[\s:-]*([0-9\.,\s\-]+)',
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|T\.P)[\s\n:-]*([0-9\.,\s\-/✅]+)',
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s]*((?:Cross|Iso|Isolated)?\s*[0-9]+x?)',
}

# --- DATABASE FUNCTIONS ---

def is_deleted(signal_id):
    """Check if this ID was previously deleted manually"""
    return deleted_collection.find_one({'id': signal_id}) is not None

def save_signal_to_db(signal_data):
    """
    Upsert signal. 
    Returns: True if this is a NEW signal, False if it was just an update.
    """
    if not signal_data or 'id' not in signal_data: return False
    
    # Check if deleted before (Backfill protection)
    if is_deleted(signal_data['id']): return False

    try:
        # Check existence first
        existing = signals_collection.find_one({'id': signal_data['id']})
        
        # Save/Update
        signals_collection.update_one(
            {'id': signal_data['id']}, 
            {'$set': signal_data}, 
            upsert=True
        )
        
        # If no existing record, it's NEW
        return existing is None
        
    except PyMongoError as e:
        logger.error(f"⚠️ DB Save Error: {e}")
        return False

def get_recent_history(limit=50):
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except PyMongoError as e:
        logger.error(f"⚠️ DB Fetch Error: {e}")
        return []

def delete_signal(signal_id):
    try:
        # Remove from active signals
        signals_collection.delete_one({'id': str(signal_id)})
        # Add to deleted list (Backfill ban)
        deleted_collection.update_one(
            {'id': str(signal_id)}, 
            {'$set': {'id': str(signal_id), 'deleted_at': time.time()}}, 
            upsert=True
        )
        logger.info(f"🗑️ Deleted signal {signal_id}")
    except PyMongoError as e:
        logger.error(f"⚠️ DB Delete Error: {e}")

# --- PARSING ENGINE ---
def parse_signal(text, timestamp=None, custom_id=None):
    if not text: return None
    clean_text = text.replace('**', '').replace('__', '').replace('`', '').strip()
    
    # 1. Find Potential Pair
    pair_match = re.search(PATTERNS['pair_strict'], clean_text, re.IGNORECASE)
    if not pair_match: return None 
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    
    # 2. Blacklist Check
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3: return None
    
    # 3. Context Check (The "Random Word" Killer)
    is_major = any(x in raw_pair for x in ['USD', 'BTC', 'ETH', 'SOL', 'BNB'])
    has_direction = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    
    # If it's a random word like "MINUTS" (no USD, no Direction nearby), SKIP IT
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
    elif 'buy' in clean_text.lower(): signal['direction'] = 'Long'
    elif 'sell' in clean_text.lower(): signal['direction'] = 'Short'
    else: signal['direction'] = 'Unknown'

    if entry_match := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        signal['entry'] = entry_match.group(1).strip()
    else: signal['entry'] = 'Market'

    if target_match := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw_targets = target_match.group(1).replace('\n', ' ')
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s+', raw_targets) if t.strip()]
    else: signal['targets'] = []

    if lev_match := re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE):
        content = lev_match.group(0)
        signal['leverage'] = content.split(':', 1)[1].strip() if ':' in content else content
    else: signal['leverage'] = 'Standard'

    return signal

# --- HELPER: ID NORMALIZER ---
def get_clean_id(id_value):
    return abs(int(id_value)) if id_value is not None else 0

# --- BACKFILL ---
async def perform_backfill(client, valid_channels):
    logger.info("⏳ Starting Backfill (Background Task)...")
    count = 0
    vip_clean_ids = set()
    for v in CHANNELS['vip']:
        if isinstance(v, int): vip_clean_ids.add(get_clean_id(v))

    for channel_id in valid_channels:
        try:
            async for message in client.iter_messages(channel_id, limit=50):
                if not message.text or "🔎 _Source:" in message.text: continue
                unique_id = f"tg_{channel_id}_{message.id}"
                
                # Skip if manually deleted
                if is_deleted(unique_id): continue
                
                parsed = parse_signal(message.text, timestamp=message.date.timestamp(), custom_id=unique_id)
                if parsed:
                    parsed['type'] = 'VIP' if (channel_id in vip_clean_ids) else 'Public'
                    parsed['source'] = 'Backfill'
                    # Just save, don't alert
                    save_signal_to_db(parsed)
                    count += 1
        except Exception as e:
            logger.error(f"⚠️ Backfill error on {channel_id}: {e}")
            
    logger.info(f"✅ Backfill Done. Synced {count} signals.")

# --- WEBSOCKET & NOTIFICATION ---
async def broadcast_signal(signal_data, delete_action=False):
    if not connected_clients: return
    try:
        if delete_action:
            msg = json.dumps({"action": "delete", "id": signal_data})
        else:
            clean_data = {k:v for k,v in signal_data.items() if k != '_id'}
            msg = json.dumps(clean_data)
            
        await asyncio.gather(*[client.send(msg) for client in connected_clients], return_exceptions=True)
    except Exception as e:
        logger.error(f"⚠️ Broadcast Error: {e}")

def send_via_http(token, chat_id, message):
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
        encoded = urllib.parse.urlencode(data).encode('utf-8')
        req = urllib.request.Request(url, data=encoded)
        with urllib.request.urlopen(req) as response: pass
    except Exception: pass

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID: return
    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    targets_str = "\n".join([f"   🎯 {t}" for t in signal['targets']])
    
    msg = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
           f"📥 **Entry:** {signal['entry']}\n"
           f"⚙️ **Lev:** {signal['leverage']}\n\n"
           f"**Targets:**\n{targets_str}\n\n"
           f"🔎 _Source: {signal.get('source', 'Unknown')}_")
    
    await asyncio.to_thread(send_via_http, BOT_TOKEN, BOT_CHAT_ID, msg)

async def websocket_handler(websocket):
    logger.info("✅ Dashboard Connected")
    connected_clients.add(websocket)
    
    # Send history
    history = get_recent_history(50)
    for old_signal in reversed(history):
        await websocket.send(json.dumps(old_signal))
        
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                action = data.get('action')
                if action == 'delete':
                    delete_signal(data.get('id'))
                    await broadcast_signal(data.get('id'), delete_action=True)
                elif action == 'add':
                    logger.info(f"➕ Manual Signal")
                    payload = data.get('payload')
                    if 'id' not in payload: payload['id'] = f"man_{int(time.time()*1000)}"
                    payload['source'] = 'Manual'
                    
                    save_signal_to_db(payload)
                    await broadcast_signal(payload)
                    await send_telegram_alert(payload)
            except json.JSONDecodeError: pass
    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: logger.error(f"WS Error: {e}")
    finally:
        connected_clients.remove(websocket)

async def health_check(path, request_headers):
    if path == "/health":
        return http.HTTPStatus.OK, [], b"OK"
    return None

# --- MAIN ---
async def main():
    global client
    
    # 1. Start Client
    if SESSION_STRING:
        try:
            client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        except Exception: return
    else:
        client = TelegramClient('scraper_session', API_ID, API_HASH)

    logger.info("Connecting to Telegram...")
    await client.start()
    
    # 2. Resolve Channels
    valid_channels_set = set()
    all_chat_ids = CHANNELS['public'] + CHANNELS['vip']
    
    for chat in all_chat_ids:
        try:
            entity = await client.get_entity(chat)
            clean_id = get_clean_id(entity.id)
            valid_channels_set.add(clean_id)
            logger.info(f"   ✅ Verified: {getattr(entity, 'title', chat)} [ID: {clean_id}]")
        except Exception: pass

    # 3. DEFINE EVENT PROCESSOR
    async def process_event(event):
        if event.sender_id == BOT_ID: return 

        event_clean_id = get_clean_id(event.chat_id)
        is_watched = event_clean_id in valid_channels_set
        is_private = event.is_private

        if not (is_watched or is_private): return

        unique_id = f"tg_{event_clean_id}_{event.id}"
        
        # Check deletion list
        if is_deleted(unique_id): return

        parsed = parse_signal(event.text, timestamp=event.date.timestamp(), custom_id=unique_id)
        
        if parsed:
            if is_watched:
                 vip_ids = [get_clean_id(x) for x in CHANNELS['vip']]
                 parsed['type'] = 'VIP' if (event_clean_id in vip_ids) else 'Public'
                 chat_obj = await event.get_chat()
                 parsed['source'] = getattr(chat_obj, 'title', 'Channel')
            else:
                 parsed['source'] = 'Saved/Private'
                 parsed['type'] = 'Manual'

            # 1. SAVE & CHECK DUPLICATION
            is_new = save_signal_to_db(parsed)
            
            # 2. UPDATE DASHBOARD
            await broadcast_signal(parsed)
            
            # 3. ALERT (Only if fresh)
            if is_new:
                logger.info(f"🔔 FRESH ALERT: {parsed['pair']}")
                await send_telegram_alert(parsed)

    # Listeners
    @client.on(events.NewMessage)
    async def new_msg(e): await process_event(e)

    @client.on(events.MessageEdited)
    async def edit_msg(e): await process_event(e)
    
    @client.on(events.MessageDeleted)
    async def del_msg(event):
        if not event.chat_id: return
        clean_id = get_clean_id(event.chat_id)
        if clean_id in valid_channels_set:
            for msg_id in event.deleted_ids:
                unique_id = f"tg_{clean_id}_{msg_id}"
                delete_signal(unique_id)
                await broadcast_signal(unique_id, delete_action=True)

    logger.info(f"🚀 Starting Server on port {PORT}...")
    
    # 4. START SERVER (FIX FOR RENDER TIMEOUT)
    # Ping interval keeps connection alive
    async with websockets.serve(
        websocket_handler, 
        "0.0.0.0", 
        PORT, 
        process_request=health_check, 
        ping_interval=20, 
        ping_timeout=20
    ):
        # 5. RUN BACKFILL IN BACKGROUND (This prevents timeout)
        asyncio.create_task(perform_backfill(client, valid_channels_set))
        
        # Keep Alive
        await client.run_until_disconnected()

if __name__ == '__main__':
    logging.getLogger("websockets.server").setLevel(logging.ERROR)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
