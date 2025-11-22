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
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- CONFIGURATION (SECURE CLOUD MODE) ---
# Use os.environ.get to safely read from Render Environment Variables
API_ID = int(os.environ.get('API_ID', 18384173))
API_HASH = os.environ.get('API_HASH', 'bb8b0e6fba49bd873f68ac98547ded2b')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8215053396:AAHhwbn74Bfzv-tvf7oVHNQwb3K54f-8qyo')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '943672693')
SESSION_STRING = os.environ.get('SESSION_STRING', '') 

# Render assigns a PORT. Default to 8765 for local testing.
PORT = int(os.environ.get("PORT", 8765))
DB_FILE = 'signals.json'

# Extract Bot ID for Loop Protection (Safely)
try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
except:
    BOT_ID = 0

# Channels to monitor
CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'], 
    'vip': [-1002138095358] 
}

# --- SETUP ---
logging.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

connected_clients = set()
signal_history = []

# --- SMART REGEX PATTERNS ---
PATTERNS = {
    'pair': r'[⚡️#$]?\s*([A-Z0-9]{2,10}\/?[A-Z0-9]{2,10})\s*[⚡️#$]?', 
    'entry': r'(?:Entry|Buy|Enter)(?:\s*Zone|e)?[\s:-]*(.*)', 
    'direction': r'\b(Long|Short)\b', 
    'targets': r'(?:Targets?|TPs?|Profit)[\s\n:]+([0-9%./\s]+)', 
    'leverage': r'(?:Leverage|Low margin|laverage).*',
}

BLACKLIST_PAIRS = {
    'CHAT', 'START', 'JOIN', 'PREMIUM', 'VIP', 'CHANNEL', 'ADMIN', 
    'SUPPORT', 'PROMO', 'DISCOUNT', 'LIFETIME', 'RESULTS', 'PROFIT',
    'FEEDBACK', 'CONTACT', 'MESSAGE', 'SIGNAL', 'TODAY', 'UPDATE'
}

# --- DATABASE FUNCTIONS ---
def load_history():
    global signal_history
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    signal_history = data
                    logger.info(f"📂 Database loaded: {len(signal_history)} past signals.")
        except Exception as e:
            logger.error(f"⚠️ Error loading database: {e}")

def save_history():
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(signal_history, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"⚠️ Error saving database: {e}")

def delete_signal(signal_id):
    global signal_history
    initial_len = len(signal_history)
    signal_history = [s for s in signal_history if s['id'] != str(signal_id)]
    if len(signal_history) < initial_len:
        save_history()
        logger.info(f"🗑️ Deleted signal {signal_id}")
        return True
    return False

# --- PARSING LOGIC ---
def parse_signal(text):
    if not text: return None
    
    # LOOP FIX: Ignore messages containing our bot's signature
    if "🔎 _Source:" in text: 
        return None

    clean_text = text.replace('**', '').replace('__', '').replace('`', '').strip()
    
    pair_match = re.search(PATTERNS['pair'], clean_text, re.IGNORECASE)
    if not pair_match: return None 
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3 or len(raw_pair) > 10: return None

    signal = {
        'id': str(int(time.time() * 1000)),
        'pair': raw_pair,
        'raw_text': clean_text,
        'timestamp': time.time()
    }

    components_found = 0
    dir_match = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    if dir_match:
        signal['direction'] = dir_match.group(1).capitalize()
        components_found += 1
    else:
        if 'buy' in clean_text.lower(): signal['direction'] = 'Long'; components_found += 1
        elif 'sell' in clean_text.lower(): signal['direction'] = 'Short'; components_found += 1
        else: signal['direction'] = 'Unknown'

    entry_match = re.search(PATTERNS['entry'], clean_text, re.IGNORECASE)
    if entry_match:
        signal['entry'] = entry_match.group(1).strip()
        components_found += 1
    else:
        signal['entry'] = 'Market'

    target_match = re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL)
    if target_match:
        raw_targets = target_match.group(1).replace('\n', ' ')
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s+', raw_targets) if t.strip()]
        if signal['targets']: components_found += 1
    else:
        signal['targets'] = []

    lev_match = re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE)
    if lev_match:
        content = lev_match.group(0).strip()
        signal['leverage'] = content.split(':', 1)[1].strip() if ':' in content else content
        components_found += 1
    else:
        signal['leverage'] = 'Standard'

    if components_found == 0: return None
    return signal

async def broadcast_signal(signal_data):
    if any(s['id'] == signal_data['id'] for s in signal_history): return
    signal_history.insert(0, signal_data)
    if len(signal_history) > 50: signal_history.pop()
    save_history()

    if not connected_clients: return
    message = json.dumps(signal_data)
    await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

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
    emoji = "🟢" if signal['direction'] == 'Long' else "🔴"
    targets_str = "\n".join([f"   🎯 {t}" for t in signal['targets']])
    message = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
               f"📥 **Entry:** {signal['entry']}\n"
               f"⚙️ **Lev:** {signal['leverage']}\n\n"
               f"**Targets:**\n{targets_str}\n\n"
               f"🔎 _Source: {signal.get('source', 'Manual/Unknown')}_")
    await asyncio.to_thread(send_via_http, BOT_TOKEN, BOT_CHAT_ID, message)

async def websocket_handler(websocket):
    logger.info("✅ New Dashboard Connected!")
    connected_clients.add(websocket)
    if signal_history:
        for old_signal in reversed(signal_history):
            await websocket.send(json.dumps(old_signal))
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                action = data.get('action')
                if action == 'delete':
                    delete_signal(data.get('id'))
                elif action == 'add':
                    logger.info(f"➕ Manual Signal Received")
                    payload = data.get('payload')
                    payload['source'] = 'Manual Dashboard'
                    await send_telegram_alert(payload) 
                    await broadcast_signal(payload)
            except json.JSONDecodeError: pass
    except websockets.exceptions.ConnectionClosed: pass
    finally:
        connected_clients.remove(websocket)
        logger.info("❌ Dashboard Disconnected")

# --- HEALTH CHECK HANDLER (Fixes 400 Bad Request on Cloud) ---
async def health_check(path, request_headers):
    if path == "/health":
        return http.HTTPStatus.OK, [], b"OK"
    return None

async def main():
    load_history()
    global client 
    
    # --- CLOUD LOGIN LOGIC ---
    if SESSION_STRING:
        logger.info("☁️ Starting with Cloud Session String...")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        logger.warning("⚠️ NO SESSION_STRING FOUND. Attempts to use local file (will fail on cloud).")
        client = TelegramClient('scraper_session', API_ID, API_HASH)
        
    await client.start()
    
    logger.info("🔍 Resolving Channel IDs...")
    valid_channels = []
    for chat in CHANNELS['public'] + CHANNELS['vip']:
        try:
            entity = await client.get_entity(chat)
            valid_channels.append(entity.id)
            logger.info(f"   ✅ Listening: {getattr(entity, 'title', chat)}")
        except: pass

    @client.on(events.NewMessage)
    async def handler(event):
        sender = await event.get_chat()
        if sender and sender.id == BOT_ID: return # Ignore bot messages

        is_watched = event.chat_id in valid_channels or event.is_private
        if not is_watched: return

        parsed = parse_signal(event.text)
        if parsed:
            print(f"✅ Parsed: {parsed['pair']} ({parsed['direction']})")
            parsed['type'] = 'VIP' if (sender.id in CHANNELS['vip']) else 'Public'
            parsed['source'] = getattr(sender, 'title', 'Unknown')
            await broadcast_signal(parsed)
            await send_telegram_alert(parsed)

    # Binds to 0.0.0.0 for Cloud Access using Render's PORT
    logger.info(f"🚀 WebSocket Server running on 0.0.0.0:{PORT}")
    
    # NOTE: process_request is crucial for cloud load balancers
    async with websockets.serve(websocket_handler, "0.0.0.0", PORT, process_request=health_check, ping_interval=None, ping_timeout=None):
        print("🤖 Scraper Running...")
        await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
