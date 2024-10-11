import requests
from binance.client import Client
from binance.helpers import round_step_size
from pymongo.mongo_client import MongoClient
from datetime import datetime
# backtrace
import traceback
import time

client = MongoClient('mongodb+srv://next:N5QW2JOkbXyu9QCN@pybi.q2us1py.mongodb.net/?retryWrites=true&w=majority&appName=PyBI')

database = client["btrader"]
settings = database["settings"]

settings_data = settings.find_one({"id": 1})

ip = settings_data['ip']
base_coin =  settings_data['base_coin']
pair = settings_data['pair']

# commission parcentage
default_affiliate_commission = float(settings_data['default_affiliate_commission'])
admin_affiliate_commission = float(settings_data['admin_affiliate_commission'])
developer_revenue_percent = float(settings_data['developer_revenue_percent'])
superadmin_revenue_percent = float(settings_data['superadmin_revenue_percent'])
superadmin_revenue_percent = float(settings_data['superadmin_revenue_percent'])


min_qty = float(settings_data['min_qty'])
max_qty = float(settings_data['max_qty'])
step_size = float(settings_data['step_size'])

while True:
    trades = database["trades"]
    users = database["users"]
    
    # find pending admin new trades
    # find_trade = trades.find_one({"role": "admin", "result": "pending", "status": "NEW"}, sort=[("created_at", -1)])
    # find last trade by admin
    find_trade = trades.find_one({"role": "admin", "result": "pending", "status": "NEW"}, sort=[("created_at", -1)])
    if find_trade is not None:
        admin_trade_id = find_trade['admin_trade_id']
        percent = float(find_trade['percent'])
        admin_price = float(find_trade['price'])
        
        #  find active users and insert trade
        # all_users = users.find({"role": "user", "status": 1})
        # all users by role and status 1 or "1"
        all_users = users.find({"role": "user", "status": {"$in": [1, "1"]}})

        for user in all_users:
            
            if 'status' not in user:
                status = 0
                users.update_one({"id": user['id']}, {"$set": {"status": status}})
            else:
                user_status = int(user['status'])
                
            if user_status == 0:
                continue
            
            find_existing_trade = trades.find_one({"admin_trade_id": admin_trade_id, "user_id": user['id']})
            if find_existing_trade is None:
                # do binance trade
                if 'binance_api_key' in user:
                    api_key = user["binance_api_key"]
                else:
                    api_key = ""
                    
                if 'binance_api_secret' in user:
                    api_secret = user["binance_api_secret"]
                else:
                    api_secret = ""
                
                if 'fuel' in user:
                    user_fuel = float(user['fuel'])
                else:
                    user_fuel = 0
                    
                if api_key == "" or api_secret == "":
                    # set user message
                    # trace = traceback.print_exc()
                    message = f"Binance API key and secret not found."
                    users.update_one({"id": user['id']}, {"$set": {"message": message, "status": 0}})
                    continue
                
                if user_fuel < 1:
                    # update user with a log
                    # trace = traceback.print_exc()
                    message = f"Insufficient fuel, cant take trade, please reload fuel balance."
                    users.update_one({"id": user['id']}, {"$set": {"message": message}})
                    continue
                
                try:
                    client = Client(api_key, api_secret)
                    
                    if find_trade['side'] == "BUY":
                    
                        print(f"{find_trade['side']} trade for user {user['name']}")
                    # check user last trade if last trade side is buy then ignore this trade
                        last_trade = trades.find_one({"pair": find_trade['pair'], "user_id": user['id'], "side": "BUY", "result": "filled_confirmed"}, sort=[("created_at", -1)])
                        if last_trade is not None:
                            continue
                        
                        
                    # get user balance
                        user_balance = client.get_asset_balance(asset=base_coin)
                        # split balance
                        balance = float(user_balance['free'])
                        # balance parcent
                        percent_balance = balance * percent / 100
                        asset_quantity = percent_balance / admin_price
                        # quantity = "{:0.0{}f}".format(asset_quantity, price_precision)
                        quantity = max(min_qty, min(max_qty, (asset_quantity // step_size) * step_size))
                        quantity = round_step_size(quantity, step_size)

                        if balance < 10:
                            # trace = traceback.print_exc()
                            print(f"User {user['name']} has insufficient balance to buy {quantity} {find_trade['pair']} current free {base_coin} balance {balance}")
                            users.update_one({"id": user['id']}, {"$set": {"message": f"You have insufficient balance to buy {quantity} {find_trade['pair']}. Your current free {base_coin} balance is ${balance}", "status": 0}})
                            continue
                        
                    elif find_trade['side'] == "SELL":
                        
                        print(f"{find_trade['side']} trade for user {user['name']}")
                        
                        # check user last trade if last trade side is sell then ignore this trade
                        last_trade = trades.find_one({"pair": find_trade['pair'], "user_id": user['id'], "side": "SELL", "result": "filled_confirmed"}, sort=[("created_at", -1)])
                        if last_trade is not None:
                            continue
                        last_buy_order_for_this_pair_and_user = trades.find_one({"pair": find_trade['pair'], "user_id": user['id'], "side": "BUY", "result": "filled_confirmed"}, sort=[("created_at", -1)])                        
                        if last_buy_order_for_this_pair_and_user is None:
                            # trace = traceback.print_exc()
                            print(f"Last buy order not found for user {user['name']}")
                            continue
                        quantity = float(last_buy_order_for_this_pair_and_user['quantity'])
                        quantity = round_step_size(quantity, step_size)
                        
                        # first check if user has enough quantity to sell
                        user_balance_for_current_asset = client.get_asset_balance(asset=find_trade['pair'])
                        available_asset_balance = float(user_balance_for_current_asset['free'])
                        
                        if quantity > available_asset_balance:
                            # trace = traceback.print_exc()
                            print(f"User {user['name']} has insufficient balance to sell {quantity} {find_trade['pair']}")
                            users.update_one({"id": user['id']}, {"$set": {"message": f"You have sold {find_trade['pair']} manually and we cannot sell {quantity}, so your trade is marked as close from our end automatically."}})
                            trades.update_one({"trade_id": last_buy_order_for_this_pair_and_user['trade_id']}, {"$set": {"status": "MANUALLY CLOSED","result": "filled_confirmed"}})
                            continue
                        
                    else:
                        continue

 
                    
                    order = client.create_order(symbol=find_trade['pair'], side=find_trade['side'], type=find_trade['type'], quantity=quantity, price=find_trade['price'], timeInForce="GTC")
                    order_id = order['orderId']
                    order_status = order['status']
                    # asset_quantity = 115
                    
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
                        "result" : "pending",
                        "user_id": user['id'],
                        "user_name": user['name'],
                        "profit": 0,
                        "fee": 0,
                        "created_at": datetime.now()
                    }
                    trades.insert_one(data)
                    print(f"Trade opened {order_id} for user {user['name']}")
                    # update user balance
                    get_client_balance = client.get_asset_balance(asset=base_coin)
                    user_balance = float(get_client_balance['free'])
                    users.update_one({"id": user['id']}, {"$set": {"trading_balance": user_balance}})
                    
                except Exception as e:
                    # update user with a log
                    # trace = traceback.print_exc()
                    message = f"{str(e)} for user {user['name']}"
                    users.update_one({"id": user['id']}, {"$set": {"message": message}})
                    
                    if "API-key format invalid" in str(e):
                        users.update_one({"id": user['id']}, {"$set": {"status": 0}})
                        
                    print(message)
                    time.sleep(5)
                    continue
                
            time.sleep(5)
                
    
    # find filled user trades by admin
    # filled_admin_trade = trades.find_one({"role":"admin","result": "filled", "status": "FILLED"})
    # if filled_admin_trade is not None:
        
        # admin_trade_id = filled_admin_trade['admin_trade_id']
    filled_trades = trades.find({"role":"user","result": "filled", "status": "FILLED"})
    for filled_trade in filled_trades:
        # find user trade
        user_info = users.find_one({"id": filled_trade['user_id']})
        if user_info is None:
            continue
        
        if 'binance_api_key' not in user_info:
            api_key = user_info['binance_api_key']
        else:
            api_key = ""
            
        if 'binance_api_secret' not in user_info:
            api_secret = user_info['binance_api_secret']
        else:
            api_secret = ""
            
        if api_key == "" or api_secret == "":
            # trace = traceback.print_exc()
            message = f"Binance API key and secret not found"
            users.update_one({"id": user_info['id']}, {"$set": {"message": message, "status": 0}})
            print(message)
            continue
        
        try:
            # sale_quantity = "{:0.0{}f}".format(filled_trade['quantity'], price_precision)
            # sale_quantity = max(min_qty, min(max_qty, (float(filled_trade['quantity']) // step_size) * step_size))
            # sale_quantity = round_step_size(sale_quantity, step_size)
            client = Client(api_key, api_secret)
            # order = client.create_order(symbol=filled_trade['pair'], side=filled_trade['side'], type=filled_trade['type'], quantity=sale_quantity, price=filled_trade['price'], timeInForce="GTC")
            
            last_buy_order_for_this_pair_and_user = trades.find_one({"pair": filled_trade['pair'], "user_id": filled_trade['user_id'], "side": "BUY", "result": "filled_confirmed"})
            
            if last_buy_order_for_this_pair_and_user is not None and filled_trade['side'] == "SELL":

                last_buy_price = float(last_buy_order_for_this_pair_and_user['price'])
                last_sell_price = float(filled_trade['price'])
                last_sell_quantity = float(filled_trade['quantity'])
                commission = 0
                profit = (last_sell_price - last_buy_price) * last_sell_quantity
                if profit > 0:
                    # deduct 25% of profit as commission and update user fuel
                    user_fuel = float(user_info['fuel'])
                    commission = profit * 0.25
                    user_fuel -= commission
                    
                    # distribution of commission
                    
                    
                    # add commission to affiliated user
                    if user_info["role"] == "user" and user_info["affiliated_by"] != "":
                        affiliate_user = users.find_one({"id": user_info["affiliated_by"]})
                        if affiliate_user is not None:
                            affiliate_user_balance = float(affiliate_user['balance'])
                            # now give the affiliate user default_affiliate_commission% of the commission
                            default_affiliate_commission = commission * default_affiliate_commission / 100
                            affiliate_user_balance += default_affiliate_commission
                            users.update_one({"id": affiliate_user['id']}, {"$set": {"balance": affiliate_user_balance}})
                            
                            
                    # add commission to admin
                    if user_info["role"] == "user" and user_info["admin_id"] != "":
                        admin_user = users.find_one({"id": user_info["admin_id"]})
                        if admin_user is not None:
                            admin_user_balance = float(admin_user['balance'])
                            # now give the affiliate user default_affiliate_commission% of the commission
                            admin_affiliate_commission = commission * admin_affiliate_commission / 100
                            admin_user_balance += admin_affiliate_commission
                            users.update_one({"id": admin_user['id']}, {"$set": {"balance": admin_user_balance}})
                            
                    # add commission to superadmin
                    superadmin_user = users.find_one({"role": "superadmin"})
                    if superadmin_user is not None:
                        superadmin_user_balance = float(superadmin_user['balance'])
                        # now give the affiliate user default_affiliate_commission% of the commission
                        superadmin_revenue = commission * superadmin_revenue_percent / 100
                        superadmin_user_balance += superadmin_revenue
                        users.update_one({"id": superadmin_user['id']}, {"$set": {"balance": superadmin_user_balance}})
                        
                    # add commission to developer
                    developer_user = users.find_one({"email": "sajjad.hira12@gmail.com"})
                    if developer_user is not None:
                        developer_user_balance = float(developer_user['balance'])
                        # now give the affiliate user default_affiliate_commission% of the commission
                        developer_revenue = commission * developer_revenue_percent / 100
                        developer_user_balance += developer_revenue
                        users.update_one({"id": developer_user['id']}, {"$set": {"balance": developer_user_balance}})
                        
                        
                        
                    # commission system done
                        
                        
                        
                    
                    # update user fuel
                    get_client_balance = client.get_asset_balance(asset=base_coin)
                    user_balance = float(get_client_balance['free'])
                    users.update_one({"id": user_info['id']}, {"$set": {"fuel": user_fuel, "trading_balance": user_balance}})
                
                trades.update_one({"trade_id": filled_trade['trade_id']}, {"$set": {"profit": profit, "fee": commission}})
            if 'profit' not in user_info:
                previous_profit = 0
            else:
                previous_profit = float(user_info['profit'])
            if profit > 0:
                new_profit = previous_profit + profit
                users.update_one({"id": user_info['id']}, {"$set": {"profit": new_profit}})
                # print(f"Profit : ${profit} for user {user_info['name']}")
            trades.update_one({"trade_id": filled_trade['trade_id']}, {"$set": {"result": "filled_confirmed"}})
        except Exception as e:
            # trades.update_one({"trade_id": filled_trade['trade_id']}, {"$set": {"result": "filled_confirmed"}})
            # update user with a log
            # trace = traceback.print_exc()
            message = f"{str(e)} for user {user_info['name']}"
            users.update_one({"id": user_info['id']}, {"$set": {"message": message}})
            trades.update_one({"trade_id": filled_trade['trade_id']}, {"$set": {"message": message}})
            
            if "API-key format invalid" in str(e):
                users.update_one({"id": user_info['id']}, {"$set": {"status": 0}})
                
            print(message)
            time.sleep(5)
            continue
        
                
    # # find pending user cacled trades
    # find_cancled_trade = trades.find_one({"role": "admin", "result": "canceled", "status": "CANCELED"})
    
    # if find_cancled_trade is not None:
    #     # admin trade id
    #     admin_trade_id = find_cancled_trade['admin_trade_id']
        # user trade id
    trades_to_cancel = trades.find({"role": "user", "result": "canceled", "status": "CANCELED"})
    for cancel_trade in trades_to_cancel:
        # find user trade
        tarde_id = cancel_trade['trade_id']
        cancel_trade_info = trades.find_one({"trade_id": tarde_id})
        user_info = users.find_one({"id": cancel_trade['user_id']})
        if user_info is None:
            print(f"User not found for trade {tarde_id}")
            continue
        
        if 'binance_api_key' in user_info:
            api_key = user_info['binance_api_key']
        else:
            api_key = ""
            
        if 'binance_api_secret' in user_info:
            api_secret = user_info['binance_api_secret']
        else:
            api_secret = ""
            
            
        if api_key == "" or api_secret == "":
            message = "Binance API key and secret not found"
            users.update_one({"id": user_info['id']}, {"$set": {"message": message, "status": 0}})
            print(message)
            continue
        
        try:
            client = Client(api_key, api_secret)
            client.cancel_order(symbol=str(cancel_trade['pair']), orderId=int(cancel_trade["trade_id"]))
            get_client_balance = client.get_asset_balance(asset=base_coin)
            user_balance = float(get_client_balance['free'])
            users.update_one({"id": user_info['id']}, {"$set": {"trading_balance": user_balance}})
            trades.update_one({"trade_id": tarde_id}, {"$set": {"result": "cancele_confirmed"}})
            print(f"Trade canceled {tarde_id} for user {user_info['name']}")
        except Exception as e:
            # update user with a log
            # trades.update_one({"trade_id": tarde_id}, {"$set": {"result": "cancele_confirmed"}})
            # trace = traceback.print_exc()
            message = f"{str(e)} for user {user_info['name']}"
            users.update_one({"id": user_info['id']}, {"$set": {"message": message}})
            trades.update_one({"trade_id": tarde_id}, {"$set": {"message": message}})
            
            if "API-key format invalid" in str(e):
                users.update_one({"id": user_info['id']}, {"$set": {"status": 0}})
                
            if "Unknown order sent" in str(e):
                trades.update_one({"trade_id": tarde_id}, {"$set": {"result": "cancele_confirmed"}})
                
            print(message)
            time.sleep(5)
            continue
            
        # update admin trade status to completed
        # trades.update_one({"admin_trade_id": admin_trade_id}, {"$set": {"result": "cancele_confirmed"}})
            
        
        
            
            