import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["golf_ball_db"]

# 1. Clear the assignments collection of all old/duplicate entries
print("🧹 Cleaning up assignments collection...")
db["assignments"].delete_many({})

# 2. Clear any individual ball collections that have technical names or old player info if desired
# (We'll leave the ball device collections alone as they are the primary data source)

print("✅ Cleanup complete. Database is now empty and ready for fresh, single-entry assignments.")
