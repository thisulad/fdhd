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

try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
except:
    BOT_ID = 0

CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'], 
    'vip': [-1002138095358] 
}

logging.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s', level=logging.INFO, datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

if not MONGO_URI:
    logger.critical("❌ MONGO_URI is missing.")
    sys.exit(1)

try:
    mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
    db = mongo_client["crypto_scraper"]
    signals_collection = db["signals"]
    # New Collection to remember deleted signals so Backfill doesn't bring them back
    deleted_collection = db["deleted_signals"]
    mongo_client.admin.command('ping')
    logger.info("✅ MongoDB Connected")
except Exception as e:
    logger.critical(f"❌ MongoDB Failed: {e}")
    sys.exit(1)

connected_clients = set()

# --- UPGRADED REGEX & BLACKLIST ---
# We explicitly block common English words that look like pairs
BLACKLIST_PAIRS = {
    'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY', 
    'RESULTS', 'FEEDBACK', 'RISK', 'REWARD', 'MINUTS', 'MINUTES', 'FREE', 
    'ZERO', 'CREATOR', 'FXBUN', 'CREATORFXBUN', 'ZONE', 'ENTRY', 'TARGET', 
    'PROFIT', 'LOSS', 'SPOT', 'FUTURE', 'LEVERAGE', 'MARGIN', 'CROSS', 
    'ISOLATED', 'SETUP', 'ANALYSIS', 'DISCLAIMER'
}

PATTERNS = {
    # STRICTER PAIR REGEX: 
    # 1. Must be 2-8 uppercase letters.
    # 2. AND (Must contain "/" OR "USD" OR be followed by Long/Short/Buy/Sell)
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
    if not signal_data or 'id' not in signal_data: return False
    
    # If user deleted this before, DO NOT save it again (Backfill Protection)
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
        logger.error(f"DB Save Error: {e}")
        return False

def get_recent_history(limit=50):
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except: return []

def delete_signal(signal_id):
    """Deletes from active signals AND adds to 'deleted' list so backfill ignores it"""
    try:
        signals_collection.delete_one({'id': str(signal_id)})
        # Remember this ID is "banned"
        deleted_collection.update_one(
            {'id': str(signal_id)}, 
            {'$set': {'id': str(signal_id), 'deleted_at': time.time()}}, 
            upsert=True
        )
        logger.info(f"🗑️ Deleted & Banned signal {signal_id}")
    except Exception as e:
        logger.error(f"Delete Error: {e}")

# --- PARSING ENGINE (Stricter) ---
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
    # If the pair doesn't have "USD" or "BTC" or "ETH", verify it looks like a signal
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

def get_clean_id(id_value):
    return abs(int(id_value)) if id_value is not None else 0

async def perform_backfill(client, valid_channels):
    logger.info("⏳ Backfilling...")
    vip_ids = {get_clean_id(v) for v in CHANNELS['vip'] if isinstance(v, int)}
    count = 0
    for channel_id in valid_channels:
        try:
            async for message in client.iter_messages(channel_id, limit=50):
                if not message.text: continue
                unique_id = f"tg_{channel_id}_{message.id}"
                
                # Check if we deleted this signal before
                if is_deleted(unique_id): continue

                parsed = parse_signal(message.text, timestamp=message.date.timestamp(), custom_id=unique_id)
                if parsed:
                    parsed['type'] = 'VIP' if (channel_id in vip_ids) else 'Public'
                    parsed['source'] = 'Backfill'
                    save_signal_to_db(parsed)
                    count += 1
        except: pass
    logger.info(f"✅ Backfill: {count} signals.")

async def broadcast_signal(signal_data, delete_action=False):
    if not connected_clients: return
    try:
        # If deleting, we send just the ID and action
        if delete_action:
            msg = json.dumps({"action": "delete", "id": signal_data})
        else:
            clean_data = {k:v for k,v in signal_data.items() if k != '_id'}
            msg = json.dumps(clean_data)
            
        await asyncio.gather(*[client.send(msg) for client in connected_clients], return_exceptions=True)
    except Exception as e: logger.error(f"WS Error: {e}")

def send_via_http(token, chat_id, message):
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
        urllib.request.urlopen(urllib.request.Request(url, data=urllib.parse.urlencode(data).encode('utf-8')))
    except: pass

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID: return
    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    msg = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
           f"📥 **Entry:** {signal['entry']}\n"
           f"**Targets:** {', '.join(signal['targets'])}\n"
           f"🔎 _Source: {signal.get('source', 'Unknown')}_")
    await asyncio.to_thread(send_via_http, BOT_TOKEN, BOT_CHAT_ID, msg)

async def websocket_handler(websocket):
    connected_clients.add(websocket)
    for old in reversed(get_recent_history(50)):
        await websocket.send(json.dumps(old))
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get('action') == 'delete':
                    delete_signal(data.get('id')) # This bans it from backfill too
                    await broadcast_signal(data.get('id'), delete_action=True)
                elif data.get('action') == 'add':
                    payload = data.get('payload')
                    if 'id' not in payload: payload['id'] = f"man_{int(time.time()*1000)}"
                    save_signal_to_db(payload)
                    await broadcast_signal(payload)
            except: pass
    except: pass
    finally: connected_clients.remove(websocket)

async def health_check(path, h):
    if path == "/health": return http.HTTPStatus.OK, [], b"OK"

async def main():
    global client
    if SESSION_STRING:
        try: client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        except: return
    else: client = TelegramClient('scraper_session', API_ID, API_HASH)
    await client.start()
    
    valid_channels_set = set()
    all_chat_ids = CHANNELS['public'] + CHANNELS['vip']
    for chat in all_chat_ids:
        try:
            entity = await client.get_entity(chat)
            clean_id = get_clean_id(entity.id)
            valid_channels_set.add(clean_id)
        except: pass

    await perform_backfill(client, valid_channels_set)

    async def process_event(event):
        if event.sender_id == BOT_ID: return 
        clean_id = get_clean_id(event.chat_id)
        if clean_id not in valid_channels_set and not event.is_private: return

        unique_id = f"tg_{clean_id}_{event.id}"
        
        # --- NEW: DELETE HANDLER ---
        # Check if this logic is called by MessageDeleted? No, MessageDeleted has no text.
        # See below for separate handler.
        
        parsed = parse_signal(event.text, timestamp=event.date.timestamp(), custom_id=unique_id)
        if parsed:
            if clean_id in valid_channels_set:
                 vip_ids = [get_clean_id(x) for x in CHANNELS['vip']]
                 parsed['type'] = 'VIP' if (clean_id in vip_ids) else 'Public'
                 chat = await event.get_chat()
                 parsed['source'] = getattr(chat, 'title', 'Channel')
            else:
                 parsed['source'] = 'Saved/Private'
                 parsed['type'] = 'Manual'

            is_new = save_signal_to_db(parsed)
            await broadcast_signal(parsed)
            if is_new: await send_telegram_alert(parsed)

    @client.on(events.NewMessage)
    async def new_msg(e): await process_event(e)

    @client.on(events.MessageEdited)
    async def edit_msg(e): await process_event(e)
    
    # --- NEW: SYNC DELETIONS FROM TELEGRAM ---
    @client.on(events.MessageDeleted)
    async def deleted_msg(event):
        # event.deleted_ids is a list of message IDs that were deleted
        # event.chat_id might be None if it's a channel, so we check carefully
        if not event.chat_id: return
        
        clean_id = get_clean_id(event.chat_id)
        if clean_id in valid_channels_set:
            for msg_id in event.deleted_ids:
                unique_id = f"tg_{clean_id}_{msg_id}"
                logger.info(f"🗑️ Sync: Deleting signal {unique_id} (Source deleted)")
                delete_signal(unique_id)
                await broadcast_signal(unique_id, delete_action=True)

    logging.getLogger("websockets.server").setLevel(logging.ERROR)
    async with websockets.serve(websocket_handler, "0.0.0.0", PORT, process_request=health_check, ping_interval=20, ping_timeout=20):
        await client.run_until_disconnected()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
