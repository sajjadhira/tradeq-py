import requests
from binance.client import Client
from binance.helpers import round_step_size
from pymongo.mongo_client import MongoClient
from datetime import datetime
import traceback
import time

# Database initialization
client = MongoClient('mongodb+srv://next:N5QW2JOkbXyu9QCN@pybi.q2us1py.mongodb.net/?retryWrites=true&w=majority&appName=PyBI')
database = client["btrader"]
settings_data = database["settings"].find_one({"id": 1})

# Settings retrieval
ip = settings_data['ip']
base_coin = settings_data['base_coin']
pair = settings_data['pair']
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

        # Get the latest pending admin trade
        find_trade = trades.find_one(
            {"role": "admin", "result": "pending", "status": "NEW"},
            sort=[("created_at", -1)]
        )
        if not find_trade:
            time.sleep(5)
            continue

        admin_trade_id = find_trade['admin_trade_id']
        percent = float(find_trade['percent'])
        admin_price = float(find_trade['price'])

        all_users = users.find({"role": "user", "status": {"$in": [1, "1"]}})

        for user in all_users:
            user_status = int(user.get('status', 0))
            if user_status == 0:
                continue

            # Check if trade already exists for user
            if trades.find_one({"admin_trade_id": admin_trade_id, "user_id": user['id']}):
                continue

            api_key, api_secret = user.get("binance_api_key"), user.get("binance_api_secret")
            user_fuel = float(user.get("fuel", 0))

            if not api_key or not api_secret:
                users.update_one(
                    {"id": user['id']},
                    {"$set": {"message": f"Binance API key and secret not found for {user['name']}. Trace: Line 86"}}
                )
                continue

            if user_fuel < 1:
                users.update_one(
                    {"id": user['id']},
                    {"$set": {"message": "Insufficient fuel, can't take trade, please reload fuel balance."}}
                )
                continue

            try:
                client = Client(api_key, api_secret)

                # Determine trade side and validate trade eligibility
                trade_side = find_trade['side']
                last_trade = trades.find_one(
                    {"pair": find_trade['pair'], "user_id": user['id'], "side": trade_side, "result": "pending"},
                    sort=[("created_at", -1)]
                )

                if trade_side == "BUY":
                    if last_trade:
                        continue

                    user_balance = float(client.get_asset_balance(asset=base_coin)['free'])
                    if user_balance < 10:
                        users.update_one(
                            {"id": user['id']},
                            {"$set": {"message": f"You have insufficient balance to buy {quantity} {find_trade['pair']}. Your current free {base_coin} balance is ${user_balance}", "status": 0}}
                        )
                        continue

                    asset_quantity = (user_balance * percent / 100) / admin_price
                    quantity = round_step_size(
                        max(min_qty, min(max_qty, (asset_quantity // step_size) * step_size)),
                        step_size
                    )

                elif trade_side == "SELL":
                    if last_trade or not trades.find_one(
                        {"pair": find_trade['pair'], "user_id": user['id'], "side": "BUY", "result": "filled_confirmed"},
                        sort=[("created_at", -1)]
                    ):
                        continue

                    quantity = float(last_trade['quantity'])
                    quantity = round_step_size(quantity, step_size)

                    asset_symbol = find_trade['pair'].replace(base_coin, "")
                    available_balance = float(client.get_asset_balance(asset=asset_symbol)['free'])

                    if quantity > available_balance:
                        users.update_one(
                            {"id": user['id']},
                            {"$set": {"message": f"You have sold {find_trade['pair']} manually, cannot sell {quantity}, marked as closed."}}
                        )
                        trades.update_one(
                            {"trade_id": last_trade['trade_id']},
                            {"$set": {"status": "MANUALLY CLOSED", "result": "filled_confirmed"}}
                        )
                        continue

                else:
                    continue

                # Place the order
                order = client.create_order(
                    symbol=find_trade['pair'],
                    side=trade_side,
                    type=find_trade['type'],
                    quantity=quantity,
                    price=find_trade['price'],
                    timeInForce="GTC"
                )

                # Record trade data
                trades.insert_one({
                    "trade_id": order['orderId'],
                    "admin_trade_id": admin_trade_id,
                    "status": order['status'],
                    "pair": find_trade['pair'],
                    "price": find_trade['price'],
                    "quantity": quantity,
                    "role": "user",
                    "side": trade_side,
                    "type": find_trade['type'],
                    "result": "pending",
                    "user_id": user['id'],
                    "user_name": user['name'],
                    "admin_id": int(user.get("admin_id", 0)),
                    "affiliated_by": int(user.get("affiliated_by", 0)),
                    "profit": 0,
                    "fee": 0,
                    "created_at": datetime.now()
                })
                print(f"Trade opened {order['orderId']} for user {user['name']}")

                # Update user balance
                updated_balance = float(client.get_asset_balance(asset=base_coin)['free'])
                users.update_one({"id": user['id']}, {"$set": {"trading_balance": updated_balance}})

            except Exception as e:
                error_msg = f"{str(e)} for user {user['name']}"
                trace = traceback.format_exc()
                users.update_one({"id": user['id']}, {"$set": {"message": error_msg}})
                if "API-key format invalid" in str(e):
                    users.update_one({"id": user['id']}, {"$set": {"status": 0}})
                print(trace)
                continue

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        time.sleep(5)
        continue
