import requests
from binance.client import Client
from binance.helpers import round_step_size
from pymongo.mongo_client import MongoClient
from datetime import datetime
import traceback
import time

client = MongoClient('')
database = client["btrader"]
settings = database["settings"]
settings_data = settings.find_one({"id": 1})

ip = settings_data['ip']
base_coin = settings_data['base_coin']
pair = settings_data['pair']

# commission percentage
default_affiliate_commission = float(settings_data['default_affiliate_commission'])
admin_affiliate_commission = float(settings_data['admin_affiliate_commission'])
developer_revenue_percent = float(settings_data['developer_revenue_percent'])
superadmin_revenue_percent = float(settings_data['superadmin_revenue_percent'])
default_fee = float(settings_data['default_fee'])

min_qty = float(settings_data['min_qty'])
max_qty = float(settings_data['max_qty'])
step_size = float(settings_data['step_size'])

while True:
    try:
        trades = database["trades"]
        users = database["users"]

        find_trade = trades.find_one({"role": "admin", "result": "pending", "status": "NEW"}, sort=[("created_at", -1)])
        if find_trade:
            admin_trade_id = find_trade['admin_trade_id']
            percent = float(find_trade['percent'])
            admin_price = float(find_trade['price'])
            all_users = users.find({"role": "user", "status": {"$in": [1, "1"]}})

            for user in all_users:
                user_status = int(user.get('status', 0)) if type(user.get('status', 0)) != int else user['status']
                if user_status == 0:
                    continue

                if not trades.find_one({"admin_trade_id": admin_trade_id, "user_id": user['id']}):
                    api_key, api_secret = user.get("binance_api_key", ""), user.get("binance_api_secret", "")
                    user_fuel = float(user.get("fuel", 0))

                    if not api_key or not api_secret:
                        message = f"Binance API key and secret not found for {user['name']}. Trace: Line 86"
                        users.update_one({"id": user['id']}, {"$set": {"message": message}})
                        continue

                    if user_fuel < 1:
                        message = f"Insufficient fuel, can't take trade, please reload fuel balance."
                        users.update_one({"id": user['id']}, {"$set": {"message": message}})
                        continue

                    try:
                        client = Client(api_key, api_secret)

                        if find_trade['side'] == "BUY":
                            last_trade = trades.find_one({"pair": find_trade['pair'], "user_id": user['id'], "side": "BUY", "result": "pending"}, sort=[("created_at", -1)])
                            if last_trade:
                                continue

                            user_balance = client.get_asset_balance(asset=base_coin)
                            balance = float(user_balance['free'])
                            percent_balance = balance * percent / 100
                            asset_quantity = percent_balance / admin_price
                            quantity = max(min_qty, min(max_qty, (asset_quantity // step_size) * step_size))
                            quantity = round_step_size(quantity, step_size)

                            if balance < 10:
                                message = f"You have insufficient balance to buy {quantity} {find_trade['pair']}. Your current free {base_coin} balance is ${balance}"
                                users.update_one({"id": user['id']}, {"$set": {"message": message, "status": 0}})
                                continue

                        elif find_trade['side'] == "SELL":
                            last_trade = trades.find_one({"pair": find_trade['pair'], "user_id": user['id'], "side": "SELL", "result": "pending"}, sort=[("created_at", -1)])
                            if last_trade:
                                continue

                            last_buy_order_for_this_pair_and_user = trades.find_one({"pair": find_trade['pair'], "user_id": user['id'], "side": "BUY", "result": "filled_confirmed"}, sort=[("created_at", -1)])
                            if last_buy_order_for_this_pair_and_user is None:
                                continue

                            quantity = float(last_buy_order_for_this_pair_and_user['quantity'])
                            quantity = round_step_size(quantity, step_size)

                            onlyasset = find_trade['pair'].replace(base_coin, "")
                            user_balance_for_current_asset = client.get_asset_balance(asset=onlyasset)
                            available_asset_balance = float(user_balance_for_current_asset['free'])

                            if quantity > available_asset_balance:
                                message = f"You have sold {find_trade['pair']} manually, cannot sell {quantity}, marked as closed."
                                users.update_one({"id": user['id']}, {"$set": {"message": message}})
                                trades.update_one({"trade_id": last_buy_order_for_this_pair_and_user['trade_id']}, {"$set": {"status": "MANUALLY CLOSED", "result": "filled_confirmed"}})
                                continue
                        else:
                            continue

                        order = client.create_order(
                            symbol=find_trade['pair'],
                            side=find_trade['side'],
                            type=find_trade['type'],
                            quantity=quantity,
                            price=find_trade['price'],
                            timeInForce="GTC"
                        )
                        order_id, order_status = order['orderId'], order['status']

                        data = {
                            "trade_id": order_id,
                            "admin_trade_id": admin_trade_id,
                            "status": order_status,
                            "pair": find_trade['pair'],
                            "price": find_trade['price'],
                            "quantity": quantity,
                            "role": "user",
                            "side": find_trade['side'],
                            "type": find_trade['type'],
                            "result": "pending",
                            "user_id": user['id'],
                            "user_name": user['name'],
                            "admin_id": int(user.get("admin_id", 0)),
                            "affiliated_by": int(user.get("affiliated_by", 0)),
                            "profit": 0,
                            "fee": 0,
                            "created_at": datetime.now()
                        }
                        trades.insert_one(data)
                        print(f"Trade opened {order_id} for user {user['name']}")

                        get_client_balance = client.get_asset_balance(asset=base_coin)
                        user_balance = float(get_client_balance['free'])
                        users.update_one({"id": user['id']}, {"$set": {"trading_balance": user_balance}})

                    except Exception as e:
                        trace = traceback.format_exc()
                        message = f"{str(e)} for user {user['name']}"
                        users.update_one({"id": user['id']}, {"$set": {"message": message}})
                        if "API-key format invalid" in str(e):
                            users.update_one({"id": user['id']}, {"$set": {"status": 0}})
                        continue
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        time.sleep(5)
        continue
