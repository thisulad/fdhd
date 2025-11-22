import re
import asyncio
import logging
import json
import time
import websockets
import sys
import os
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- CONFIGURATION (SECURE CLOUD) ---
# We read these from the Environment Variables
API_ID = int(os.environ.get('API_ID', 18384173))
API_HASH = os.environ.get('API_HASH', 'bb8b0e6fba49bd873f68ac98547ded2b')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '') # Empty default for safety
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID', '')
SESSION_STRING = os.environ.get('SESSION_STRING', '') 

# Render assigns a random port
PORT = int(os.environ.get("PORT", 8765))
DB_FILE = 'signals.json'

# Extract Bot ID for Loop Protection
try:
    BOT_ID = int(BOT_TOKEN.split(':')[0])
except:
    BOT_ID = 0

# Channels
CHANNELS = {
    'public': ['Binancesignalwithishara', 'me'], 
    'vip': [-1002138095358] 
}

logging.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

connected_clients = set()
signal_history = []

# --- PATTERNS ---
PATTERNS = {
    'pair': r'[⚡️#$]?\s*([A-Z0-9]{2,10}\/?[A-Z0-9]{2,10})\s*[⚡️#$]?', 
    'entry': r'(?:Entry|Buy|Enter)(?:\s*Zone|e)?[\s:-]*(.*)', 
    'direction': r'\b(Long|Short)\b', 
    'targets': r'(?:Targets?|TPs?|Profit)[\s\n:]+([0-9%./\s]+)', 
    'leverage': r'(?:Leverage|Low margin|laverage).*',
}
BLACKLIST_PAIRS = {'CHAT', 'START', 'JOIN', 'VIP', 'ADMIN', 'SUPPORT', 'PROFIT', 'MESSAGE'}

# --- FUNCTIONS ---
def load_history():
    global signal_history
    # Note: On free Render, this wipes on restart. 
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list): signal_history = data
        except: pass

def save_history():
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(signal_history, f, indent=4, ensure_ascii=False)
    except: pass

def delete_signal(signal_id):
    global signal_history
    initial_len = len(signal_history)
    signal_history = [s for s in signal_history if s['id'] != str(signal_id)]
    if len(signal_history) < initial_len:
        save_history()
        logger.info(f"🗑️ Deleted signal {signal_id}")

def parse_signal(text):
    if not text: return None
    # LOOP FIX: Ignore our own bot messages
    if "🔎 _Source:" in text: return None

    clean_text = text.replace('**', '').replace('__', '').strip()
    pair_match = re.search(PATTERNS['pair'], clean_text, re.IGNORECASE)
    if not pair_match: return None 
    
    raw_pair = pair_match.group(1).upper().replace('/', '')
    if raw_pair in BLACKLIST_PAIRS or len(raw_pair) < 3: return None

    signal = {'id': str(int(time.time()*1000)), 'pair': raw_pair, 'raw_text': clean_text, 'timestamp': time.time()}
    
    found = 0
    d_m = re.search(PATTERNS['direction'], clean_text, re.IGNORECASE)
    if d_m: signal['direction'] = d_m.group(1).capitalize(); found+=1
    else:
        if 'buy' in clean_text.lower(): signal['direction']='Long'; found+=1
        elif 'sell' in clean_text.lower(): signal['direction']='Short'; found+=1
        else: signal['direction']='Unknown'

    e_m = re.search(PATTERNS['entry'], clean_text, re.IGNORECASE)
    if e_m: signal['entry']=e_m.group(1).strip(); found+=1
    else: signal['entry']='Market'

    t_m = re.search(PATTERNS['targets'], clean_text, re.IGNORECASE|re.DOTALL)
    if t_m: 
        signal['targets'] = [t.strip() for t in re.split(r'\/|,|\s+', t_m.group(1).replace('\n',' ')) if t.strip()]
        if signal['targets']: found+=1
    else: signal['targets']=[]

    l_m = re.search(PATTERNS['leverage'], clean_text, re.IGNORECASE)
    if l_m: 
        c = l_m.group(0).strip()
        signal['leverage'] = c.split(':',1)[1].strip() if ':' in c else c
        found+=1
    else: signal['leverage']='Standard'

    return signal if found > 0 else None

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
    except: pass

async def send_telegram_alert(signal):
    if not BOT_TOKEN or not BOT_CHAT_ID: return
    emoji = "🟢" if signal['direction'] == 'Long' else "🔴"
    targets_str = "\n".join([f"   🎯 {t}" for t in signal['targets']])
    msg = (f"⚡ **{signal['pair']}** {emoji} **{signal['direction'].upper()}**\n\n"
           f"📥 **Entry:** {signal['entry']}\n⚙️ **Lev:** {signal['leverage']}\n\n"
           f"**Targets:**\n{targets_str}\n\n🔎 _Source: {signal.get('source', 'Manual')}_")
    await asyncio.to_thread(send_via_http, BOT_TOKEN, BOT_CHAT_ID, msg)

async def websocket_handler(websocket):
    logger.info("✅ Dashboard Connected")
    connected_clients.add(websocket)
    if signal_history:
        for s in reversed(signal_history): await websocket.send(json.dumps(s))
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get('action') == 'delete': delete_signal(data.get('id'))
                elif data.get('action') == 'add':
                    logger.info(f"➕ Manual Signal")
                    payload = data.get('payload')
                    payload['source'] = 'Manual'
                    await send_telegram_alert(payload) 
                    await broadcast_signal(payload)
            except: pass
    except: pass
    finally: connected_clients.remove(websocket)

async def main():
    global client
    load_history()
    
    if SESSION_STRING:
        logger.info("☁️ Starting Cloud Session")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        logger.warning("⚠️ NO SESSION STRING FOUND. Check Environment Variables.")
        # Fallback to local for testing, but this fails on cloud usually
        client = TelegramClient('scraper_session', API_ID, API_HASH)
        
    await client.start()
    logger.info("✅ Telegram Login Successful")

    @client.on(events.NewMessage)
    async def handler(event):
        sender = await event.get_chat()
        # 1. Ignore Bot Messages
        if sender and sender.id == BOT_ID: return
        
        is_watched = event.is_private or (event.chat_id in CHANNELS['vip']) or (getattr(sender, 'username', '') in CHANNELS['public'])
        if not is_watched: return

        parsed = parse_signal(event.text)
        if parsed:
            parsed['type'] = 'VIP' if (event.chat_id in CHANNELS['vip']) else 'Public'
            parsed['source'] = getattr(sender, 'title', 'Unknown')
            
            logger.info(f"🚀 Signal: {parsed['pair']}")
            await broadcast_signal(parsed)
            await send_telegram_alert(parsed)

    logger.info(f"🚀 Server running on 0.0.0.0:{PORT}")
    async with websockets.serve(websocket_handler, "0.0.0.0", PORT, ping_interval=None, ping_timeout=None):
        await client.run_until_disconnected()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
```

---

### Phase 2: Deploy to Render (The Safe Way)

1.  **Push to GitHub:**
    * Upload `scraper.py` and `requirements.txt` to a GitHub repository.

2.  **Create Render Service:**
    * Log in to [Render.com](https://render.com).
    * Click **New +** -> **Web Service**.
    * Select your GitHub repo.

3.  **Basic Settings:**
    * **Name:** `signal-scraper-bot`
    * **Runtime:** `Python 3`
    * **Build Command:** `pip install -r requirements.txt`
    * **Start Command:** `python scraper.py`

4.  **Secure Environment Variables (The Most Important Part):**
    * Scroll down to the **Environment Variables** section.
    * Click **"Add Environment Variable"** for each of these:

| Key | Value |
| :--- | :--- |
| `API_ID` | `18384173` |
| `API_HASH` | `bb8b0e6fba49bd873f68ac98547ded2b` |
| `BOT_TOKEN` | `8215053396:AAHhwbn74Bfzv-tvf7oVHNQwb3K54f-8qyo` |
| `BOT_CHAT_ID` | `943672693` |
| `SESSION_STRING` | *(Paste the long string you got from Phase 1 Step 2)* |
| `PYTHONUNBUFFERED`| `1` |

5.  **Deploy:**
    * Click **Create Web Service**.
    * Wait for the deploy logs to finish. It should say "Server running on 0.0.0.0...".

---

### Phase 3: Connect Dashboard

1.  Render will give you a URL (e.g., `https://signal-scraper-bot.onrender.com`).
2.  Open your `index.html` on your computer.
3.  Find the config line and paste your Render URL (change `https` to `wss`):
    ```javascript
    const CLOUD_WS_URL = "wss://signal-scraper-bot.onrender.com";
