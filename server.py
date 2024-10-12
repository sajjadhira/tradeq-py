from binance.client import Client
from pymongo.mongo_client import MongoClient
# use flask to create a web server
from flask import Flask, request, jsonify
import socket
client = MongoClient('mongodb+srv://next:N5QW2JOkbXyu9QCN@pybi.q2us1py.mongodb.net/?retryWrites=true&w=majority&appName=PyBI')

database = client["btrader"]
settings = database["settings"]
settings_data = settings.find_one({"id": 1})
base_coin =  settings_data['base_coin']
app = Flask(__name__)

def get_server_ip():
    return socket.gethostbyname(socket.gethostname())


# routw to post with binance api key and secret
@app.route('/api/connection-check/binance', methods=['POST'])
def connection_check():
    data = request.get_json()
    api_key = data['api_key']
    api_secret = data['api_secret']
    try:
        client = Client(api_key, api_secret)
        user_balance_for_current_asset = client.get_asset_balance(asset=base_coin)
        free_balance = float(user_balance_for_current_asset['free'])
        
        return jsonify({'status': 'success', 'message': f'Connection successful, your {base_coin} free balance is ${free_balance}'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    
    # if get_server_ip() with 127 then port will be 5000 otherwise 80':
    # port = 5000 if get_server_ip().startswith('127') else 80
    app.run(host='0.0.0.0', port=80)