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
from datetime import datetime, timedelta
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
mongo_client = None
signals_collection = None
deleted_collection = None

try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
    ADMIN_ID = int(BOT_CHAT_ID) # Only allow commands from this ID
except (AttributeError, IndexError, ValueError):
    BOT_ID = 0
    ADMIN_ID = 0

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
    'pair_strict': r'(?:\#|\$)?([A-Z0-9]{2,8}(?:/[A-Z0-9]{2,8})?)',
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    'entry': r'(?:Entry|Buy|EP|Enter|Price)(?:\s*(?:Zone|Range|Price|Target|at)?)?[\s:-]*([0-9\.,\s\-]+)',
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|Take\s*Profit|T\.P)[\s\n:-]*([0-9\.,\s\-/✅]+)',
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s]*((?:Cross|Iso|Isolated)?\s*[0-9]+x?)',
}

# --- HELPER FUNCTIONS ---

def is_deleted(signal_id):
    if deleted_collection is None: return False
    return deleted_collection.find_one({'id': signal_id}) is not None

def save_signal_to_db(signal_data):
    if signals_collection is None: return False
    if not signal_data or 'id' not in signal_data: return False
    if is_deleted(signal_data['id']): return False
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
    if signals_collection is None: return []
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except PyMongoError: return []

def delete_signal(signal_id):
    if signals_collection is None: return
    try:
        signals_collection.delete_one({'id': str(signal_id)})
        deleted_collection.update_one(
            {'id': str(signal_id)},
            {'$set': {'id': str(signal_id), 'deleted_at': time.time()}},
            upsert=True
        )
        logger.info(f"🗑️ Deleted signal {signal_id}")
    except PyMongoError: pass

# --- PARSING ENGINE ---
def parse_signal(text, timestamp=None, custom_id=None):
    if not text: return None
    clean_text = text.replace('**', '').replace('__', '').replace('`', '').strip()
    
    pair_match = re.search(PATTERNS['pair_strict'], clean_text, re.IGNORECASE)
    if not pair_match: return None
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3: return None
    
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
        if d == 'Buy': signal['direction'] = 'Long'
        elif d == 'Sell': signal['direction'] = 'Short'
        else: signal['direction'] = d
    else:
        signal['direction'] = 'Unknown'

    if entry_match := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        signal['entry'] = entry_match.group(1).strip()
    else:
        signal['entry'] = 'Market'

    if target_match := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw_targets = target_match.group(1).replace('\n', ' ')
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s\-\s|\s+', raw_targets) if t.strip() and t.strip() not in ['-', 'TP']]
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

async def perform_backfill(client, valid_channels):
    while signals_collection is None:
        await asyncio.sleep(1)
        
    logger.info("⏳ Backfilling history (Background Task)...")
    count = 0
    vip_clean_ids = set()
    for v in CHANNELS['vip']:
        if isinstance(v, int): vip_clean_ids.add(get_clean_id(v))

    for channel_id in valid_channels:
        try:
            async for message in client.iter_messages(channel_id, limit=50):
                if not message.text or "🔎 _Source:" in message.text: continue
                unique_id = f"tg_{channel_id}_{message.id}"
                if is_deleted(unique_id): continue
                
                parsed = parse_signal(message.text, timestamp=message.date.timestamp(), custom_id=unique_id)
                if parsed:
                    parsed['type'] = 'VIP' if (channel_id in vip_clean_ids) else 'Public'
                    parsed['source'] = 'Backfill'
                    save_signal_to_db(parsed)
                    count += 1
        except Exception: pass
    logger.info(f"✅ Backfill Done. Synced {count} signals.")

async def broadcast_signal(signal_data, delete_action=False):
    if not connected_clients: return
    try:
        msg = json.dumps({"action": "delete", "id": signal_data}) if delete_action else json.dumps({k:v for k,v in signal_data.items() if k != '_id'})
        await asyncio.gather(*[client.send(msg) for client in connected_clients], return_exceptions=True)
    except Exception: pass

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID: 
        logger.warning("⚠️ Missing BOT_TOKEN or BOT_CHAT_ID. Alert skipped.")
        return

    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    targets_str = "\n".join([f"   🎯 {t}" for t in signal['targets']])
    
    msg = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
           f"📥 **Entry:** {signal['entry']}\n"
           f"⚙️ **Lev:** {signal['leverage']}\n\n"
           f"**Targets:**\n{targets_str}\n\n"
           f"🔎 _Source: {signal.get('source', 'Unknown')}_")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": BOT_CHAT_ID, "text": msg, "parse_mode": "Markdown"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"❌ Alert Failed: {await response.text()}")
    except Exception as e:
        logger.error(f"❌ Alert Exception: {e}")

async def websocket_handler(websocket):
    logger.info("✅ Dashboard Connected")
    connected_clients.add(websocket)
    
    if signals_collection is not None:
        history = get_recent_history(50)
        for old_signal in reversed(history):
            await websocket.send(json.dumps(old_signal))
        
    try:
        async for message in websocket:
            if signals_collection is None: continue 
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
    except Exception: pass
    finally:
        connected_clients.remove(websocket)

# --- MAIN ---
async def main():
    global mongo_client, signals_collection, deleted_collection
    
    logger.info(f"🚀 Starting Server on port {PORT}...")
    server = await websockets.serve(
        websocket_handler, 
        "0.0.0.0", 
        PORT, 
        ping_interval=20, 
        ping_timeout=20
    )

    async def bootstrap_app():
        global mongo_client, signals_collection, deleted_collection
        
        logger.info("🔌 Connecting to MongoDB...")
        if not MONGO_URI: return
        try:
            mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
            db = mongo_client["crypto_scraper"]
            signals_collection = db["signals"]
            deleted_collection = db["deleted_signals"]
            await asyncio.to_thread(mongo_client.admin.command, 'ping')
            logger.info("✅ MongoDB Connected")
        except Exception as e:
            logger.critical(f"❌ DB Fail: {e}")
            return

        if not SESSION_STRING: return
        try:
            client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            logger.info("🔌 Connecting to Telegram...")
            await client.start()
            logger.info("✅ Telegram Connected")
            
            valid_channels_set = set()
            for chat in CHANNELS['public'] + CHANNELS['vip']:
                try:
                    entity = await client.get_entity(chat)
                    valid_channels_set.add(get_clean_id(entity.id))
                except: pass

            # --- SIGNAL HANDLER ---
            async def process_event(event):
                global MSGS_SCANNED
                if event.sender_id == BOT_ID: return 
                
                # Ignore processing if Paused (unless it's your Saved Messages)
                if SCRAPER_PAUSED and not event.is_private: return

                clean_id = get_clean_id(event.chat_id)
                if clean_id not in valid_channels_set and not event.is_private: return
                
                MSGS_SCANNED += 1
                unique_id = f"tg_{clean_id}_{event.id}"
                if is_deleted(unique_id): return
                
                parsed = parse_signal(event.text, timestamp=event.date.timestamp(), custom_id=unique_id)
                if parsed:
                    if clean_id in valid_channels_set:
                        vip_ids = [get_clean_id(x) for x in CHANNELS['vip']]
                        parsed['type'] = 'VIP' if (clean_id in vip_ids) else 'Public'
                        chat_obj = await event.get_chat()
                        parsed['source'] = getattr(chat_obj, 'title', 'Channel')
                    else:
                        parsed['source'] = 'Saved/Private'; parsed['type'] = 'Manual'
                    
                    is_new = save_signal_to_db(parsed)
                    await broadcast_signal(parsed)
                    if is_new: 
                        logger.info(f"🔔 New Signal: {parsed['pair']}")
                        await send_telegram_alert(parsed)

            # --- ADMIN COMMAND HANDLER ---
            @client.on(events.NewMessage(pattern='/'))
            async def admin_handler(event):
                global SCRAPER_PAUSED
                
                # Security Check: Only accept commands from ADMIN_ID
                if event.sender_id != ADMIN_ID: return

                cmd = event.text.split()[0].lower()
                
                if cmd == '/status':
                    uptime = str(timedelta(seconds=int(time.time() - START_TIME)))
                    status_emoji = "🔴 PAUSED" if SCRAPER_PAUSED else "🟢 ACTIVE"
                    db_status = "✅ Connected" if mongo_client else "❌ Disconnected"
                    msg = (
                        f"🤖 **System Status**\n\n"
                        f"📡 **State:** {status_emoji}\n"
                        f"⏱ **Uptime:** {uptime}\n"
                        f"📨 **Scanned:** {MSGS_SCANNED} msgs\n"
                        f"💾 **Database:** {db_status}\n"
                        f"🌐 **Clients:** {len(connected_clients)}"
                    )
                    await event.respond(msg)
                
                elif cmd == '/pause':
                    SCRAPER_PAUSED = True
                    await event.respond("⏸ **Scraper PAUSED.** No new channel signals will be processed.")
                    
                elif cmd == '/resume':
                    SCRAPER_PAUSED = False
                    await event.respond("▶️ **Scraper RESUMED.** Listening for signals...")
                    
                elif cmd == '/help':
                    msg = (
                        "🛠 **Admin Commands**\n\n"
                        "`/status` - View system health & stats\n"
                        "`/pause` - Stop processing incoming signals\n"
                        "`/resume` - Resume processing\n"
                        "`/add BTC Long 95000` - Add manual signal"
                    )
                    await event.respond(msg)

                # Manual Add: /add BTC Long 95000
                elif cmd == '/add':
                    try:
                        parts = event.text.split()
                        # Expected: /add PAIR DIR PRICE
                        if len(parts) < 4:
                            await event.respond("⚠️ Usage: `/add BTC Long 95000`")
                            return
                        
                        payload = {
                            'id': f"man_{int(time.time()*1000)}",
                            'pair': parts[1].upper(),
                            'direction': parts[2].capitalize(),
                            'entry': parts[3],
                            'targets': ['Open'],
                            'leverage': 'Manual',
                            'source': 'Telegram Admin',
                            'type': 'Manual',
                            'timestamp': time.time(),
                            'status': 'pending'
                        }
                        save_signal_to_db(payload)
                        await broadcast_signal(payload)
                        await event.respond(f"✅ **Added:** {payload['pair']} {payload['direction']}")
                    except Exception as e:
                        await event.respond(f"❌ Error: {str(e)}")

            client.add_event_handler(process_event, events.NewMessage)
            client.add_event_handler(process_event, events.MessageEdited)
            
            async def del_msg(event):
                if not event.chat_id: return
                clean_id = get_clean_id(event.chat_id)
                if clean_id in valid_channels_set:
                    for msg_id in event.deleted_ids:
                        unique_id = f"tg_{clean_id}_{msg_id}"
                        delete_signal(unique_id)
                        await broadcast_signal(unique_id, delete_action=True)

            client.add_event_handler(del_msg, events.MessageDeleted)
            await perform_backfill(client, valid_channels_set)
            await client.run_until_disconnected()
        except Exception as e:
            logger.error(f"Telegram Error: {e}")

    asyncio.create_task(bootstrap_app())
    await asyncio.get_running_loop().create_future()

if __name__ == '__main__':
    loggers = ["websockets", "websockets.server", "websockets.protocol", "websockets.asyncio.server", "asyncio"]
    for l in loggers: logging.getLogger(l).setLevel(logging.CRITICAL)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
