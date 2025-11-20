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
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- CONFIGURATION ---
# Get these from Environment Variables (for security) or hardcode them
API_ID = int(os.environ.get('API_ID', '12345678')) 
API_HASH = os.environ.get('API_HASH', 'YOUR_API_HASH')
BOT_TOKEN = os.environ.get('BOT_TOKEN', 'YOUR_BOT_TOKEN')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', 'YOUR_CHAT_ID')
SESSION_STRING = os.environ.get('SESSION_STRING', '') # The long string from generate_session.py

# Port must be read from env for Render
PORT = int(os.environ.get("PORT", 8765))

# Database (In memory for free cloud tiers that wipe files, or persistent if volume attached)
# Ideally use a real DB (Mongo/Postgres) for cloud, but list works for runtime history
signal_history = []
connected_clients = set()

# Channels to monitor
CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'], 
    'vip': [-1002138095358] 
}

# --- SETUP ---
logging.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- SMART REGEX PATTERNS (Same as before) ---
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

# --- PARSING LOGIC ---
def parse_signal(text):
    if not text: return None
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

    dir_match = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    if dir_match:
        signal['direction'] = dir_match.group(1).capitalize()
    else:
        if 'buy' in clean_text.lower(): signal['direction'] = 'Long'
        elif 'sell' in clean_text.lower(): signal['direction'] = 'Short'
        else: signal['direction'] = 'Unknown'
    
    # Check if direction unknown but we have pair - stricter check
    if signal['direction'] == 'Unknown':
         # Fallback: Look for 'Long' or 'Short' in raw text even without boundary
         if 'long' in clean_text.lower(): signal['direction'] = 'Long'
         elif 'short' in clean_text.lower(): signal['direction'] = 'Short'

    entry_match = re.search(PATTERNS['entry'], clean_text, re.IGNORECASE)
    signal['entry'] = entry_match.group(1).strip() if entry_match else 'Market'

    target_match = re.search(PATTERNS['targets'], clean_text, re.IGNORECASE | re.DOTALL)
    if target_match:
        raw_targets = target_match.group(1).replace('\n', ' ')
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s+', raw_targets) if t.strip()]
    else:
        signal['targets'] = []
    
    # Must have targets to be a signal (spam filter)
    if not signal['targets']: return None

    lev_match = re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE)
    if lev_match:
        content = lev_match.group(0).strip()
        signal['leverage'] = content.split(':', 1)[1].strip() if ':' in content else content
    else:
        signal['leverage'] = 'Standard'

    return signal

async def broadcast_signal(signal_data):
    if any(s['id'] == signal_data['id'] for s in signal_history): return
    signal_history.insert(0, signal_data)
    if len(signal_history) > 50: signal_history.pop()
    
    if not connected_clients: return
    message = json.dumps(signal_data)
    await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

# --- TELEGRAM BOT API SENDER ---
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
                if data.get('action') == 'add':
                    payload = data.get('payload')
                    payload['source'] = 'Manual Dashboard'
                    await send_telegram_alert(payload) 
                    await broadcast_signal(payload)
                elif data.get('action') == 'delete':
                    # Just remove from memory in cloud
                    sid = data.get('id')
                    global signal_history
                    signal_history = [s for s in signal_history if s['id'] != str(sid)]
            except json.JSONDecodeError: pass
    except websockets.exceptions.ConnectionClosed: pass
    finally:
        connected_clients.remove(websocket)

async def main():
    global client
    
    if SESSION_STRING:
        logger.info("🔐 Using String Session from Env Var")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        logger.info("⚠️ No SESSION_STRING found. Trying local file (will fail on cloud)")
        client = TelegramClient('scraper_session', API_ID, API_HASH)
        
    await client.start()
    
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
        is_watched = event.chat_id in valid_channels or event.is_private
        if not is_watched: return

        parsed = parse_signal(event.text)
        if parsed:
            parsed['type'] = 'VIP' if (sender.id in CHANNELS['vip']) else 'Public'
            parsed['source'] = getattr(sender, 'title', 'Unknown')
            logger.info(f"✅ Signal: {parsed['pair']}")
            await broadcast_signal(parsed)
            await send_telegram_alert(parsed)

    # Binds to 0.0.0.0 for Cloud access
    logger.info(f"🚀 Server running on 0.0.0.0:{PORT}")
    async with websockets.serve(websocket_handler, "0.0.0.0", PORT, ping_interval=None, ping_timeout=None):
        await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass