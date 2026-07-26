import os
import requests
from datetime import datetime

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_message(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": CHAT_ID,
        "text": msg
    })

if __name__ == "__main__":
    send_message(
        f"""🚀 EMA ALERT BOT STARTED

✅ GitHub Action Running
🕒 {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
"""
    )
