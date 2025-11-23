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
    'pair_strict': r'(?:\#|\$)?([A-Z0-9]{2,8}(?:/[A-Z0-9]{2,8})?)',
    'direction': r'\b(Long|Short|Buy|Sell)\b',
    'entry': r'(?:Entry|Buy|EP|Enter)(?:\s*(?:Zone|Range|Price|Target)?)?[\s:-]*([0-9\.,\s\-]+)',
    'targets': r'(?:Target\s*s?|TP\s*s?|Profit|T\.P)[\s\n:-]*([0-9\.,\s\-/✅]+)',
    'leverage': r'(?:Lev(?:erage)?\s*|Margin\s*)?[:\s]*((?:Cross|Iso|Isolated)?\s*[0-9]+x?)',
}

# --- DATABASE FUNCTIONS ---

def is_deleted(signal_id):
    """Check if this ID was previously deleted"""
    return deleted_collection.find_one({'id': signal_id}) is not None

def save_signal_to_db(signal_data):
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
    try:
        cursor = signals_collection.find({}, {'_id': 0}).sort("timestamp", -1).limit(limit)
        return list(cursor)
    except PyMongoError as e:
        logger.error(f"⚠️ DB Fetch Error: {e}")
        return []

def delete_signal(signal_id):
    try:
        signals_collection.delete_one({'id': str(signal_id)})
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

# --- BACKFILL ---
async def perform_backfill(client, valid_channels):
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
        except Exception as e:
            logger.error(f"⚠️ Backfill error on {channel_id}: {e}")
            
    logger.info(f"✅ Backfill Done. Synced {count} signals.")

# --- WEBSOCKET ---
async def broadcast_signal(signal_data, delete_action=False):
    if not connected_clients: return
    try:
        if delete_action:
            msg = json.dumps({"action": "delete", "id": signal_data})
        else
