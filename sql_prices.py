import sqlite3
import asyncio
import json
import websockets
from datetime import datetime

class MarketDB:
    def __init__(self, db_name="crypto_trading.db"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                price REAL
            )
        ''')
        self.conn.commit()

    def save_price(self, symbol, price):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute('INSERT INTO price_history (timestamp, symbol, price) VALUES (?, ?, ?)', 
                            (ts, symbol, price))
        self.conn.commit()

async def collect_and_save():
    db = MarketDB()
    url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    
    async with websockets.connect(url) as ws:
        print("--- Data Collection Started (Saving to SQL) ---")
        for _ in range(20):
            msg = await ws.recv()
            data = json.loads(msg)
            price = float(data['c'])
            
            db.save_price("BTC/USDT", price)
            print(f"Saved: ${price:,.2f}")

if __name__ == "__main__":
    asyncio.run(collect_and_save())