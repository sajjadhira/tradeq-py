
from pymongo.mongo_client import MongoClient
from datetime import datetime

client = MongoClient('mongodb+srv://next:N5QW2JOkbXyu9QCN@pybi.q2us1py.mongodb.net/?retryWrites=true&w=majority&appName=PyBI')

database = client["btrader"]
trades = database["trades"]

for trade in trades.find():
    created_at = trade["created_at"]
    # Check if created_at is a string or datetime
    if isinstance(created_at, str):
        # Convert string to datetime
        created_at = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
    elif isinstance(created_at, datetime):
        # Format datetime to the desired string format
        created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
    
    # update the trade
    trades.update_one({"_id": trade["_id"]}, {"$set": {"created_at": created_at}})