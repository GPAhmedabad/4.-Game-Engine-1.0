import pymongo
from pprint import pprint

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["golf_ball_db"]

print("--- Current Assignments in DB ---")
assignments = list(db["assignments"].find({}))
for a in assignments:
    pprint(a)

if len(assignments) > 1:
    print(f"\n⚠️ WARNING: Found {len(assignments)} entries. Checking for duplicate ball IDs...")
    ball_ids = [a.get("ball_id") for a in assignments]
    if len(ball_ids) != len(set(ball_ids)):
        print("❌ ERROR: Duplicate ball IDs found in the assignments log!")
    else:
        print("✅ No duplicate ball IDs, but multiple balls are registered.")
else:
    print("\n✅ Database is clean (1 or 0 entries).")
