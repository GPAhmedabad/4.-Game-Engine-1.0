import pymongo

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "golf_ball_db"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

print("--- COLLECTIONS ---")
for col in db.list_collection_names():
    print(f"Collection: {col}")
    doc = db[col].find_one()
    print(f"  Data: {doc}")
    print("-" * 20)
