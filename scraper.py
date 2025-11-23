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
# Use .get() with defaults to prevent "KeyError" crashes
API_ID = int(os.environ.get('API_ID', 18384173))
API_HASH = os.environ.get('API_HASH', 'bb8b0e6fba49bd873f68ac98547ded2b')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8215053396:AAHhwbn74Bfzv-tvf7oVHNQwb3K54f-8qyo')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '943672693')
SESSION_STRING = os.environ.get('SESSION_STRING', '') 
MONGO_URI = os.environ.get('MONGO_URI') 
PORT = int(os.environ.get("PORT", 8765))

# Safety: Extract Bot ID to prevent the bot from scraping itself
try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
except (AttributeError, IndexError, ValueError):
    BOT_ID = 0

# Channels to monitor
# Note: We will normalize these IDs dynamically
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

# --- MONGODB CONNECTION (Robust) ---
if not MONGO_URI:
    logger.critical("❌ FATAL: MONGO_URI is missing from Environment Variables.")
    sys.exit(1)

try:
    # tlsCAFile=certifi.where() is REQUIRED for Render/Heroku SSL connections
    mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
    db = mongo_client["crypto_scraper"]
    signals_collection = db["signals"]
    # Force a connection attempt to fail early if creds are wrong
    mongo_client.admin.command('ping')
    logger.info("✅ MongoDB Atlas Connected Successfully")
except Exception as e:
    logger.critical(f"❌ MongoDB Connection Failed: {e}")
    sys.exit(1)

# Set of active WebSocket clients
connected_clients = set()

# --- REGEX PATTERNS (Refined for "Lazy" Editing) ---
PATTERNS = {
    # Matches: "BTC/USDT", "BTCUSDT", "#BTC", "BTC Long"
    'pair': r'(?:\#|\$)?([A-Z]{2,6}\/?[A-Z]{2,6})(?:\s+)?(?:Long|Short)?', 

    # Matches: "Long", "Short", "BUY", "SELL" (Case insensitive)
    'direction': r'\b(Long|Short|Buy|Sell)\b',

    # Matches: "Entry: 1.23 - 1.25", "Buy Zone: 1.23", "Entry 1.23"
    'entry': r'(?:Entry|Buy|EP|Enter)(?:\s*(?:Zone|Range|Price|Target)?)?[\s:-]*([0-9\.,\s\-]+)',

    # Matches: "Targets: 1, 2, 3", "TP: 1.2", "TP1: 1.2", "Take Profit: 1.2"
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|T\.P)[\s\n:-]*([0-9\.,\s\-/✅]+)',

    # Matches: "Lev: 20x", "Leverage: Cross 20x", "20x"
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s]*((?:Cross|Iso|Isolated)?\s*[0-9]+x?)',
}

BLACKLIST_PAIRS = {'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY', 'RESULTS', 'FEEDBACK'}

# --- DATABASE FUNCTIONS ---

def save_signal_to_db(signal_data):
    """
    Upsert signal. Uses the unique 'id' field to prevent duplicates.
    """
    if not signal_data or 'id' not in signal_data: return
    try:
        signals_collection.update_one(
            {'id': signal_data['id']}, 
            {'$set': signal_data}, 
            upsert=True
        )
    except PyMongoError as e:
        logger.error(f"⚠️ DB Save Error: {e}")

def get_recent_history(limit=50):
    """
    Fetch history. 
    CRITICAL: Projection {'_id': 0} ensures we don't get non-serializable ObjectIds.
    """
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except PyMongoError as e:
        logger.error(f"⚠️ DB Fetch Error: {e}")
        return []

def delete_signal(signal_id):
    try:
        signals_collection.delete_one({'id': str(signal_id)})
        logger.info(f"🗑️ Deleted signal {signal_id}")
    except PyMongoError as e:
        logger.error(f"⚠️ DB Delete Error: {e}")

# --- PARSING ENGINE (Fail-Safe) ---
def parse_signal(text, timestamp=None, custom_id=None):
    if not text: return None
    
    # Pre-cleaning
    clean_text = text.replace('**', '').replace('__', '').replace('`', '').strip()
    
    # 1. Check Pair (The minimum requirement)
    pair_match = re.search(PATTERNS['pair'], clean_text, re.IGNORECASE)
    if not pair_match: return None 
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3: return None

    # 2. Determine ID and Timestamp
    ts = timestamp if timestamp else time.time()
    # Use custom_id if provided (e.g., from Telegram msg ID), else fallback to time
    sig_id = str(custom_id) if custom_id else str(int(ts * 1000))

    signal = {
        'id': sig_id,
        'pair': raw_pair,
        'raw_text': clean_text,
        'timestamp': ts,
        'status': 'pending'
    }

    # 3. Extract Direction (Safe Check)
    if dir_match := re.search(PATTERNS['direction'], clean_text, re.IGNORECASE):
        signal['direction'] = dir_match.group(1).capitalize()
    elif 'buy' in clean_text.lower(): signal['direction'] = 'Long'
    elif 'sell' in clean_text.lower(): signal['direction'] = 'Short'
    else: signal['direction'] = 'Unknown'

    # 4. Extract Entry
    if entry_match := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        signal['entry'] = entry_match.group(1).strip()
    else: 
        signal['entry'] = 'Market'

    # 5. Extract Targets (Robust split)
    if target_match := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw_targets = target_match.group(1).replace('\n', ' ')
        # Split by comma, slash, or whitespace
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s+', raw_targets) if t.strip()]
    else: 
        signal['targets'] = []

    # 6. Extract Leverage
    if lev_match := re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE):
        content = lev_match.group(0)
        signal['leverage'] = content.split(':', 1)[1].strip() if ':' in content else content
    else:
        signal['leverage'] = 'Standard'

    return signal

# --- HELPER: ID NORMALIZER ---
def get_clean_id(id_value):
    """
    Standardizes Telegram IDs to ensure -100 prefix doesn't break logic.
    Returns the absolute integer ID.
    """
    return abs(int(id_value)) if id_value is not None else 0

# --- BACKFILL (Deterministic) ---
async def perform_backfill(client, valid_channels):
    logger.info("⏳ Starting Backfill...")
    count = 0
    # Create a reverse lookup for VIP status
    vip_ids = set()
    for v in CHANNELS['vip']:
        if isinstance(v, int): vip_ids.add(get_clean_id(v))

    for channel_id in valid_channels:
        try:
            async for message in client.iter_messages(channel_id, limit=50):
                if not message.text or "🔎 _Source:" in message.text: continue
                
                # CRITICAL: Create Deterministic ID (ChannelID_MessageID)
                unique_id = f"tg_{channel_id}_{message.id}"
                
                parsed = parse_signal(
                    message.text, 
                    timestamp=message.date.timestamp(),
                    custom_id=unique_id
                )
                
                if parsed:
                    parsed['type'] = 'VIP' if (channel_id in vip_ids) else 'Public'
                    parsed['source'] = 'Backfill'
                    save_signal_to_db(parsed)
                    count += 1
        except Exception as e:
            logger.error(f"⚠️ Backfill error on {channel_id}: {e}")
            
    logger.info(f"✅ Backfill Done. Synced {count} signals.")

# --- WEBSOCKET & NOTIFICATION ---
async def broadcast_signal(signal_data):
    # Ensure no internal DB fields leak to JSON
    clean_data = {k:v for k,v in signal_data.items() if k != '_id'}
    
    # Save first
    save_signal_to_db(clean_data)
    
    # Broadcast
    if not connected_clients: return
    try:
        message = json.dumps(clean_data)
        # Send to all connected clients, ignoring connection errors
        await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)
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
    
    # Send history (Already cleaned of _id by get_recent_history)
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
                    
                elif action == 'add':
                    logger.info(f"➕ Manual Signal")
                    payload = data.get('payload')
                    # Manual signals need a generated ID if not provided
                    if 'id' not in payload:
                        payload['id'] = f"man_{int(time.time()*1000)}"
                    payload['source'] = 'Manual'
                    
                    await broadcast_signal(payload)
                    await send_telegram_alert(payload)
            except json.JSONDecodeError: pass
    except websockets.exceptions.ConnectionClosed: pass
    except Exception as e: logger.error(f"WebSocket Error: {e}")
    finally:
        connected_clients.remove(websocket)

# --- HEALTH CHECK (Required for Cloud) ---
async def health_check(path, request_headers):
    if path == "/health":
        return http.HTTPStatus.OK, [], b"OK"
    return None

# --- MAIN ---
async def main():
    global client
    
    # Session Management
    if SESSION_STRING:
        logger.info("☁️ Using Session String")
        try:
            client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        except Exception as e:
            logger.critical(f"❌ Session String Invalid: {e}")
            return
    else:
        logger.warning("⚠️ No SESSION_STRING. Using local file (Not recommended for Cloud).")
        client = TelegramClient('scraper_session', API_ID, API_HASH)

    await client.start()
    
    # 1. RESOLVE CHANNELS (Standardized)
    valid_channels_set = set() # Use a set for faster lookups
    all_chat_ids = CHANNELS['public'] + CHANNELS['vip']
    
    logger.info("🔍 Verifying Channels...")
    for chat in all_chat_ids:
        try:
            entity = await client.get_entity(chat)
            # Store the 'clean' ID (no minus sign)
            clean_id = get_clean_id(entity.id)
            valid_channels_set.add(clean_id)
            logger.info(f"   ✅ Verified: {getattr(entity, 'title', chat)} [ID: {clean_id}]")
        except Exception as e: 
            logger.warning(f"   ❌ Failed to access {chat}: {e}")

    # Initial Backfill using the clean IDs
    await perform_backfill(client, valid_channels_set)

    # 2. DEFINED SIGNAL PROCESSOR (To reuse for New & Edit messages)
    async def process_event(event):
        # Ignore bot's own messages
        if event.sender_id == BOT_ID: return 

        # 3. ROBUST ID CHECK
        # We check both the raw ID and the 'clean' ID
        event_clean_id = get_clean_id(event.chat_id)
        
        is_watched_channel = event_clean_id in valid_channels_set
        is_private_chat = event.is_private

        if not (is_watched_channel or is_private_chat):
            return

        # 4. PARSE
        unique_id = f"tg_{event_clean_id}_{event.id}"
        parsed = parse_signal(
            event.text, 
            timestamp=event.date.timestamp(),
            custom_id=unique_id
        )
        
        if parsed:
            # Determine Source
            if is_watched_channel:
                 # Calculate VIP status dynamically using clean IDs
                 vip_clean_ids = [get_clean_id(x) for x in CHANNELS['vip']]
                 parsed['type'] = 'VIP' if (event_clean_id in vip_clean_ids) else 'Public'
                 
                 # Try to get title
                 chat_obj = await event.get_chat()
                 parsed['source'] = getattr(chat_obj, 'title', 'Channel')
            else:
                 parsed['source'] = 'Saved/Private'
                 parsed['type'] = 'Manual'

            logger.info(f"🚀 Signal Detected: {parsed['pair']} ({parsed['direction']})")
            await broadcast_signal(parsed)
            await send_telegram_alert(parsed)

    # 5. REGISTER HANDLERS FOR BOTH NEW AND EDITED MESSAGES
    @client.on(events.NewMessage)
    async def new_message_handler(event):
        await process_event(event)

    @client.on(events.MessageEdited)
    async def edited_message_handler(event):
        # This catches signals that start as "Loading..." and are edited later
        await process_event(event)

    logger.info(f"🚀 Server Active on port {PORT}")
    # Ping interval keeps connection alive on Cloud
    async with websockets.serve(websocket_handler, "0.0.0.0", PORT, process_request=health_check, ping_interval=20, ping_timeout=20):
        await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
