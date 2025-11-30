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
from motor.motor_asyncio import AsyncIOMotorClient

# --- CONFIGURATION ---
API_ID = int(os.environ.get('API_ID', 0))
API_HASH = os.environ.get('API_HASH', '')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '')
SESSION_STRING = os.environ.get('SESSION_STRING', '')
MONGO_URI = os.environ.get('MONGO_URI', '')
PORT = int(os.environ.get("PORT", 8765))

# --- CHANNELS (Your VIP IDs) ---
CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'],
    'vip': [-1002138095358, -1001905653511] 
}

# --- LOGGING ---
logging.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- GLOBAL STATE ---
connected_clients = set()
clients_lock = asyncio.Lock()
mongo_client = None
signals_collection = None
deleted_collection = None
START_TIME = time.time()
MSGS_SCANNED = 0

# --- CONSTANTS & REGEX ---
BLACKLIST_PAIRS = {'CHAT', 'START', 'JOIN', 'VIP', 'ADMIN', 'SIGNAL', 'TODAY', 'RESULTS', 'FEEDBACK', 'RISK'}

PATTERNS = {
    # PAIR: Matches "PARTI/USDT", "BTCUSDT"
    'pair_strict': r'(?:⚡|🔥|\#|\$)?\s*([A-Z0-9]{2,8}(?:[/-][A-Z0-9]{2,8})?)',
    # ENTRY: Matches "Sell / Short Above - 0.1424", "Entry market price"
    'entry': r'(?:Entry|Buy|Sell|Short|Long|EP|Enter|Price|Above|Below|At)[\s\w/\-]*(?:market\s*price|market|cmp|current|price|zone|range|target|at|above|below)?[\s:\-]*(market\s*price|market|cmp|current|[0-9\.,\s\-]+)',
    # DIRECTION: Matches "Long", "Sell / Short"
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    # TARGETS: Matches numbers, "wait"
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|Take\s*Profit|T\.P)[\s\n:-]*(wait|wating|[0-9\.,\s\-/✅%]+)',
    # LEVERAGE: Matches "10X", "Cross"
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s\-]*(?:Cross|Iso|Isolated)?\s*([0-9]+[xX]|Low\s*margin|High\s*leverage)',
    # STOP LOSS: Matches "SL-"
    'stop_loss': r'(?:SL|Stop\s*Loss)[\s:-]*(wait|wating|[0-9\.]+)'
}

# --- HELPER FUNCTIONS ---
def get_clean_id(id_value):
    if id_value is None: return 0
    try: return abs(int(id_value))
    except: return 0

async def save_signal_to_db(signal_data):
    if not signals_collection: return False
    try:
        # Check if deleted
        if deleted_collection:
            if await deleted_collection.find_one({'id': signal_data['id']}): return False
        
        existing = await signals_collection.find_one({'id': signal_data['id']})
        if existing and signal_data.get('source') == 'Backfill' and existing.get('source') != 'Backfill':
            signal_data['source'] = existing['source']
            
        await signals_collection.update_one({'id': signal_data['id']}, {'$set': signal_data}, upsert=True)
        return existing is None
    except Exception as e:
        logger.error(f"DB Error: {e}")
        return False

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID: return
    
    emoji = "🟢" if signal.get('direction') == 'Long' else "🔴"
    targets = "   ⏳ TP: Wait" if 'Wait' in signal['targets'] else "\n".join([f"   🎯 {t}" for t in signal['targets']])
    sl = f"\n🛑 **SL:** {signal.get('stop_loss', 'N/A')}" if signal.get('stop_loss') else ""
    
    msg = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
           f"📥 **Entry:** {signal['entry']}\n"
           f"⚙️ **Lev:** {signal['leverage']}"
           f"{sl}\n\n"
           f"**Targets:**\n{targets}\n\n"
           f"🔎 _Source: {signal.get('source', 'Unknown')}_")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={"chat_id": BOT_CHAT_ID, "text": msg, "parse_mode": "Markdown"}) as resp:
            if resp.status != 200: logger.error(f"Telegram Alert Failed: {await resp.text()}")

async def broadcast_signal(signal):
    async with clients_lock:
        if not connected_clients: return
        msg = json.dumps({k:v for k,v in signal.items() if k != '_id'})
        await asyncio.gather(*[c.send(msg) for c in connected_clients], return_exceptions=True)

# --- PARSING ENGINE ---
def parse_signal(text, timestamp=None, custom_id=None):
    if not text: return None
    text = unicodedata.normalize('NFKC', text)
    clean_text = text.replace('**', '').replace('__', '').replace('`', '').strip()
    
    pair_match = re.search(PATTERNS['pair_strict'], clean_text, re.IGNORECASE)
    if not pair_match: return None
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3: return None
    
    # LOOSE FILTER: If it has "USD", "BTC" OR "Long/Short" keyword, we accept it
    is_major = any(x in raw_pair for x in ['USD', 'BTC', 'ETH', 'SOL', 'BNB'])
    has_dir = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    if not is_major and not has_dir: return None

    signal = {
        'id': str(custom_id or int(time.time()*1000)),
        'pair': raw_pair,
        'raw_text': clean_text,
        'timestamp': timestamp or time.time(),
        'status': 'pending'
    }

    # Parsing Fields
    if dir_m := re.search(PATTERNS['direction'], clean_text, re.IGNORECASE):
        d = dir_m.group(1).capitalize()
        signal['direction'] = 'Long' if d in ['Buy','Long'] else ('Short' if d in ['Sell','Short'] else d)
    else: signal['direction'] = 'Unknown'

    if ent_m := re.search(PATTERNS['entry'], clean_text, re.IGNORECASE):
        raw = ent_m.group(1).strip().lstrip('-').strip()
        signal['entry'] = 'Market' if any(x in raw.lower() for x in ['market','cmp','current']) else raw
    else: signal['entry'] = 'Market'

    if trg_m := re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL):
        raw = trg_m.group(1).replace('\n', ' ')
        signal['targets'] = ['Wait'] if 'wait' in raw.lower() or 'wating' in raw.lower() else [t.strip() for t in re.split(r'\/|,|\s\-\s|\s+', raw) if t.strip() and t.strip() not in ['-','TP','%']]
    else: signal['targets'] = []

    if lev_m := re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE):
        signal['leverage'] = lev_m.group(1).strip()
    else: signal['leverage'] = 'Standard'

    if sl_m := re.search(PATTERNS['stop_loss'], clean_text, re.IGNORECASE):
        signal['stop_loss'] = sl_m.group(1).strip()

    return signal

# --- SERVER & MAIN ---
async def health_check(c, r):
    if r.path in ["/health", "/"]: return http.HTTPStatus.OK, [], b"OK"
    return None

async def websocket_handler(ws):
    async with clients_lock: connected_clients.add(ws)
    try:
        if signals_collection:
            cursor = signals_collection.find({}, {'_id':0}).sort("timestamp", -1).limit(50)
            for s in reversed(await cursor.to_list(length=50)): await ws.send(json.dumps(s))
        await ws.wait_closed()
    except: pass
    finally: 
        async with clients_lock: connected_clients.discard(ws)

async def main():
    global mongo_client, signals_collection, deleted_collection
    
    # 1. TEST REGEX ON STARTUP
    test_msg = """PARTI/USDT
    🔴 Sell / Short Above - 0.1424
    LEVERAGE - 10X or X5 (Cross)
    TP-wait
    SL- 0.15900"""
    logger.info("🧪 RUNNING STARTUP REGEX TEST...")
    test_res = parse_signal(test_msg)
    if test_res:
        logger.info(f"✅ TEST PASSED! Parsed: {test_res['pair']} {test_res['direction']} Entry: {test_res['entry']} SL: {test_res.get('stop_loss')}")
    else:
        logger.critical("❌ TEST FAILED! Regex did not catch sample signal.")

    # 2. Start Server
    logger.info(f"🚀 Server starting on port {PORT}...")
    server = await websockets.serve(websocket_handler, "0.0.0.0", PORT, process_request=health_check, ping_interval=20, ping_timeout=20)

    # 3. Bootstrap Telegram & DB
    if MONGO_URI:
        try:
            mongo_client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
            db = mongo_client["crypto_scraper"]
            signals_collection = db["signals"]
            deleted_collection = db["deleted_signals"]
            await signals_collection.create_index("id", unique=True)
            logger.info("✅ MongoDB Connected")
        except Exception as e: logger.critical(f"DB Error: {e}")

    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        
        # BRUTE FORCE CHANNEL ADDING
        valid_ids = set()
        for c in CHANNELS['public'] + CHANNELS['vip']:
            clean = get_clean_id(c)
            valid_ids.add(clean)
        
        @client.on(events.NewMessage)
        async def handler(event):
            global MSGS_SCANNED
            MSGS_SCANNED += 1
            
            # DEBUG LOG: Print EVERY message from a watched channel
            clean_id = get_clean_id(event.chat_id)
            if clean_id in valid_ids or event.is_private:
                logger.info(f"📨 Received from {clean_id}: {event.text[:50]}...")
                
                parsed = parse_signal(event.text, timestamp=event.date.timestamp(), custom_id=f"tg_{clean_id}_{event.id}")
                if parsed:
                    logger.info(f"🚀 SIGNAL DETECTED: {parsed['pair']}")
                    parsed['source'] = "VIP Channel" if clean_id in {get_clean_id(x) for x in CHANNELS['vip']} else "Public"
                    is_new = await save_signal_to_db(parsed)
                    await broadcast_signal(parsed)
                    if is_new: await send_telegram_alert(parsed)
                else:
                    logger.warning(f"⚠️ FAILED TO PARSE: {event.text[:30]}...")
        
        logger.info("🔌 Connecting Telegram...")
        await client.start()
        logger.info("✅ Telegram Connected & Listening!")
        await client.run_until_disconnected()

if __name__ == '__main__':
    # Silence websockets, but keep our logs
    logging.getLogger("websockets").setLevel(logging.ERROR)
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
