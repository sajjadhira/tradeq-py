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
if 'last_update_time' in settings_data:
    last_update_time = settings_data['last_update_time']
else:
    last_update_time = None

# if last update time is more than 1 hour, update the IP address
if last_update_time is None or datetime.now() - last_update_time > timedelta(hours=1):
    response = requests.get('https://ipinfo.io')
    data = response.json()
    ip = data['ip']
    settings.update_one({"id": 1}, {"$set": {"ip": ip, "last_update_time": datetime.now()}})

# Initialize the Binance client
client = Client(admin_binance_api_key, admin_binance_api_secret)

# Fetch and store LOT_SIZE filter information
exchange_info = client.get_symbol_info(pair)
lot_size = [f for f in exchange_info['filters'] if f['filterType'] == 'LOT_SIZE'][0]

min_qty = float(lot_size['minQty'])
max_qty = float(lot_size['maxQty'])
step_size = float(lot_size['stepSize'])
# get the min notional value
# Fetch and store LOT_SIZE filter information if available
# Initialize min_notional
min_notional = None

# Check for LOT_SIZE filter
lot_size_filter = [f for f in exchange_info['filters'] if f['filterType'] == 'LOT_SIZE']
price_filter = [f for f in exchange_info['filters'] if f['filterType'] == 'PRICE_FILTER']

if lot_size_filter and price_filter:
    min_qty = float(lot_size_filter[0]['minQty'])
    min_price = float(price_filter[0]['minPrice'])
    # Approximate min_notional by multiplying min_qty and min_price
    min_notional = min_qty * min_price
    # print(f"Calculated min_notional for {pair} using LOT_SIZE and PRICE_FILTER: {min_notional}")
else:
    print(f"MIN_NOTIONAL filter is not available for this symbol {pair}.")
    exit()


    
settings.update_one({"id": 1}, {"$set": {"min_qty": min_qty, "max_qty": max_qty, "step_size": step_size, "min_notional": min_notional}})

# Function to handle incoming WebSocket messages
def handle_socket_message(msg):
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if msg['e'] == 'executionReport':
        trade_id = msg['i']
        status = msg['X']
        price = float(msg['p']) if msg['p'] else None
        quantity = float(msg['q']) if msg['q'] else None
        side = msg['S']
        type = msg['o']
        trade_pair = msg['s']
        
        trades = database["trades"]
        find_trade = trades.find_one({"trade_id": trade_id})

        # do stuff with the OCO order
        if type == "OCO" and find_trade is None:
           
            # Extract OCO-specific fields with proper handling
            stop_price = float(msg.get('P', 0))  # Stop price for the stop-limit leg
            limit_price = float(msg.get('L', 0))  # Limit price for the limit leg
            stop_order_id = msg.get('C')  # ID of the stop-limit order
            limit_order_id = msg.get('r')  # ID of the limit order
            order_id = msg.get('i')  # The main order ID

            # Make sure stop_price and limit_price are handled as floats
            stop_price = stop_price if stop_price != 0 else None
            limit_price = limit_price if limit_price != 0 else None
            
            # Fetch balance for the base asset (assumes client and base_coin are set up)
            balance = client.get_asset_balance(asset=base_coin)
            amount_usd = float(price) * float(quantity)
            amount_with_this_trade = float(balance['free']) + amount_usd
            percent = (amount_usd * 100) / amount_with_this_trade
            
            # Create a new data entry for the trade in the database
            data = {
                "trade_id": trade_id,
                "admin_trade_id": trade_id,
                "status": "NEW",
                "pair": trade_pair,
                "price": price,
                "quantity": quantity,
                "percent": percent,
                "role": "admin",
                "side": side,
                "type": type,
                "result": "pending",
                "stop_price": stop_price,
                "limit_price": limit_price,
                "stop_order_id": stop_order_id,
                "limit_order_id": limit_order_id,
                "order_id": order_id,
                "stop_order_status": "NEW",
                "limit_order_status": "NEW",
                "user_id": 1,
                "user_name": "Admin",
                "created_at": date_time,
            }
            
            # Insert trade record into the 'trades' collection
            trades.insert_one(data)
        # do stuff with the market order
        elif type == "MARKET" and find_trade is None:
            balance = client.get_asset_balance(asset=base_coin)
            amount_usd = float(price) * float(quantity)
            amount_with_this_trade = float(balance['free']) + amount_usd
            percent = (amount_usd * 100) / amount_with_this_trade

            data = {
                "trade_id": trade_id,
                "admin_trade_id": trade_id,
                "status": "NEW",
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
        elif status == "NEW" and type == "LIMIT" and find_trade is None:
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
