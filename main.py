import os
import time
import requests

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

def send(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": CHAT_ID,
        "text": msg
    })

def get_price(symbol):
    url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={symbol}"
    headers = {
        "Authorization": f"Bearer {os.environ['UPSTOX_TOKEN']}",
        "Accept": "application/json"
    }
    r = requests.get(url, headers=headers)
    return r.json()

send("✅ EMA Alert Bot Started!")

while True:
    try:
        send("🔄 Checking NIFTY 50, BANKNIFTY & SENSEX...")
        time.sleep(60)
    except Exception as e:
        send(str(e))
        time.sleep(60)
