from pymongo import MongoClient

def get_mongo_connection():
    try:
        # Connect to local MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        
        # Select database
        db = client["loan_etl"]
        
        return db   # ✅ return db so main.py can use it
    except Exception as e:
        print("❌ Error connecting to MongoDB:", e)
        return None
