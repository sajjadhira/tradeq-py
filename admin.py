import requests
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
from pymongo.mongo_client import MongoClient
from datetime import datetime, timedelta
import time

# MongoDB connection and settings retrieval
client = MongoClient('mongodb+srv://next:N5QW2JOkbXyu9QCN@pybi.q2us1py.mongodb.net/?retryWrites=true&w=majority&appName=PyBI')
database = client["btrader"]
settings = database["settings"]

settings_data = settings.find_one({"id": 1})

ip = settings_data['ip']
base_coin = settings_data['base_coin']
pair = settings_data['pair']
admin_binance_api_key = settings_data['admin_binance_api_key']
admin_binance_api_secret = settings_data['admin_binance_api_secret']

if ip == "":
    response = requests.get('https://ipinfo.io')
    data = response.json()
    ip = data['ip']
    settings.update_one({"id": 1}, {"$set": {"ip": ip}})

# Initialize the Binance client
client = Client(admin_binance_api_key, admin_binance_api_secret)

# Fetch and store LOT_SIZE filter information
exchange_info = client.get_symbol_info(pair)
lot_size = [f for f in exchange_info['filters'] if f['filterType'] == 'LOT_SIZE'][0]

min_qty = float(lot_size['minQty'])
max_qty = float(lot_size['maxQty'])
step_size = float(lot_size['stepSize'])
settings.update_one({"id": 1}, {"$set": {"min_qty": min_qty, "max_qty": max_qty, "step_size": step_size}})

# Function to handle incoming WebSocket messages
def handle_socket_message(msg):
    date_time = datetime.now()
    if msg['e'] == 'executionReport':
        trade_id = msg['i']
        status = msg['X']
        price = msg['p']
        quantity = msg['q']
        side = msg['S']
        type = msg['o']
        
        trades = database["trades"]
        find_trade = trades.find_one({"trade_id": trade_id})

        if status == "NEW" and type == "LIMIT" and find_trade is None:
            balance = client.get_asset_balance(asset=base_coin)
            amount_usd = float(price) * float(quantity)
            amount_with_this_trade = float(balance['free']) + amount_usd
            percent = (amount_usd * 100) / amount_with_this_trade

            data = {
                "trade_id": trade_id,
                "admin_trade_id": trade_id,
                "status": status,
                "pair": pair,
                "price": price,
                "quantity": quantity,
                "percent": percent,
                "role": "admin",
                "side": side,
                "type": type,
                "result": "pending",
                "user_id": 1,
                "user_name": "Admin",
                "created_at": date_time,
            }
            trades.insert_one(data)
            print(f"{status} {type} {side}:  {trade_id} # {pair} {price}")

        elif status == "CANCELED" and type == "LIMIT" and find_trade is not None and find_trade['result'] == "pending":
            trades.update_one({"trade_id": trade_id}, {"$set": {"status": "CANCELED", "result": "canceled"}})
            trades.update_many({"admin_trade_id": trade_id}, {"$set": {"status": "CANCELED", "result": "canceled"}})
            print(f"{status} {type} {side}:  {trade_id} # {pair} {price}")

        elif status == "FILLED" and type == "LIMIT" and find_trade is not None and find_trade['result'] == "pending":
            trades.update_one({"trade_id": trade_id}, {"$set": {"status": "FILLED", "result": "filled"}})
            trades.update_many({"admin_trade_id": trade_id}, {"$set": {"status": "FILLED", "result": "filled"}})
            print(f"{status} {type} {side}:  {trade_id} # {pair} {price}")

# Function to start WebSocket and reconnect with timeout
def start_websocket():
    max_reconnect_time = timedelta(minutes=5)  # Set maximum reconnection time (e.g., 5 minutes)
    start_time = datetime.now()

    while True:
        try:
            twm = ThreadedWebsocketManager(api_key=admin_binance_api_key, api_secret=admin_binance_api_secret)
            twm.start()

            # Start the user data stream to listen for order updates
            twm.start_user_socket(callback=handle_socket_message)

            # Keep the WebSocket running
            twm.join()
        except Exception as e:
            print(f"WebSocket connection failed: {e}. Attempting to reconnect...")

        if datetime.now() - start_time > max_reconnect_time:
            print("Maximum reconnection time exceeded. Exiting.")
            break

        time.sleep(5)  # Wait before attempting to reconnect

# Start the WebSocket connection
start_websocket()