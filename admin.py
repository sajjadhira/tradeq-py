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
# base_coin = settings_data['base_coin']
# pair = settings_data['pair']
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


stable_coins = ['USDT', 'FDUSD']

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
        pair = msg['s']
        
        
        base_coin = next((stable_coin for stable_coin in stable_coins if stable_coin in pair), None)
        
        trades = database["trades"]
        find_trade = trades.find_one({"trade_id": trade_id})
       
     
        # Handle TRAILING_STOP_LIMIT orders
        if type == "TRAILING_STOP_LIMIT" and find_trade is None:
            # Calculate user balance and percent
            balance = client.get_asset_balance(asset=base_coin)
            amount_usd = float(price) * float(quantity)
            amount_with_this_trade = float(balance['free']) + amount_usd
            percent = (amount_usd * 100) / amount_with_this_trade
            
            # Extract trailing limit specifics (callback rate, stop price, and limit price)
            callback_rate = float(msg.get('AP', 0))  # Callback rate percentage (trailing percent)
            stop_price = float(msg.get('P', 0))  # Stop price for the trailing limit order
            limit_price = float(msg.get('L', 0))  # Limit price to place order after trigger
            
  
            # Handle TRADE FILLED or PARTIALLY FILLED
            if find_trade and status in ['FILLED', 'PARTIALLY_FILLED']:
                
                # get the closed price
                closed_price = float(msg['L']) if msg['L'] else 0
                
                trades.update_many(
                    {"admin_trade_id": trade_id},
                    {
                        "$set": {
                            "status": "FILLED",
                            "result": "filled",
                            "price": closed_price,
                            "updated_at": date_time
                        }
                    }
                )
                print(f"Trade {trade_id} filled: {quantity} {pair} at {price}")

            # Handle TRADE CANCELLED
            elif find_trade and status == 'CANCELED':
                trades.update_many(
                    {"admin_trade_id": trade_id},
                    {
                        "$set": {
                            "status": "CANCELLED",
                            "result": "cancelled",
                            "updated_at": date_time
                        }
                    }
                )
                print(f"Trade {trade_id} cancelled")
                
            elif find_trade and status == 'REJECTED':
                trades.update_many(
                    {"admin_trade_id": trade_id},
                    {
                        "$set": {
                            "status": "CANCELLED",
                            "result": "cancelled",
                            "updated_at": date_time
                        }
                    }
                )
                print(f"Trade {trade_id} rejected")
                
            elif status == "NEW":
                # Insert the new trade record into the database
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
                    "callback_rate": callback_rate,  # Trailing stop callback rate
                    "stop_price": stop_price,  # Stop price if available
                    "limit_price": limit_price,  # Limit price for limit order
                    "result": "pending",
                    "user_id": 1,
                    "user_name": "Admin",
                    "created_at": date_time,
                }
                trades.insert_one(data)
                print(f"{status} {type} {side}: {trade_id} # {pair} at {price} with trailing stop of {callback_rate}% and stop price {stop_price}, limit price {limit_price}")
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
            
            # Track filled status and detect whether stop or limit leg was filled
            if status == "FILLED":
                execution_price = float(msg['L']) if msg['L'] else 0  # Last executed price
                if order_id == stop_order_id:                    
                    trades.update_many({"admin_stop_order_id": stop_order_id}, {"$set": {"status": "FILLED", "result": "filled_confirmed", "stop_order_status": "FILLED", "price": execution_price}})
                elif order_id == limit_order_id:
                    trades.update_many({"limit_order_status": limit_order_id}, {"$set": {"status": "FILLED", "result": "filled", "limit_order_status": "FILLED", "price": execution_price}})
                else:
                    trades.update_many({"admin_trade_id": trade_id}, {"$set": {"status": "FILLED", "result": "filled"}})
                    print(f"Order filled at price: {execution_price}")
                    
            elif status == "CANCELED":
                if order_id == stop_order_id:
                    trades.update_many({"admin_stop_order_id": stop_order_id}, {"$set": {"status": "CANCELED", "result": "canceled", "stop_order_status": "CANCELED"}})
                elif order_id == limit_order_id:
                    trades.update_many({"limit_order_status": limit_order_id}, {"$set": {"status": "CANCELED", "result": "canceled", "limit_order_status": "CANCELED"}})
                else:
                    trades.update_many({"admin_trade_id": trade_id}, {"$set": {"status": "CANCELED", "result": "canceled"}})
                    print(f"Order canceled: {trade_id}")
                    
            elif status == "REJECTED":
                if order_id == stop_order_id:
                    trades.update_many({"admin_stop_order_id": stop_order_id}, {"$set": {"status": "CANCELED", "result": "canceled", "stop_order_status": "canceled"}})
                elif order_id == limit_order_id:
                    trades.update_many({"limit_order_status": limit_order_id}, {"$set": {"status": "CANCELED", "result": "canceled", "limit_order_status": "canceled"}})
                else:
                    trades.update_many({"admin_trade_id": trade_id}, {"$set": {"status": "CANCELED", "result": "canceled"}})
                    print(f"Order rejected: {trade_id}")
                    
            elif status == "NEW":
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
                    "pair": pair,
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
