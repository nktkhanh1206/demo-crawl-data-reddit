import requests
import time
from pymongo import MongoClient
from datetime import datetime

# ================= CONFIG =================
WINDOW_SIZE = 500
UPDATE_INTERVAL = 60  # seconds
USE_API_UPDATE = True  # bật/tắt update từ Reddit

# ================= MONGO =================
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]

raw_collection = db["posts"]
rt_collection = db["posts_realtime"]

rt_collection.create_index("id", unique=True)
rt_collection.create_index("created_utc")
    
# ================= HEADERS =================
HEADERS = {
    "User-Agent": "Mozilla/5.0 realtime-bot/2.0"
}


# ================= INIT LOAD =================
def init_load():
    print("🚀 INIT FROM RAW")

    raw_count = raw_collection.count_documents({})
    print("📦 RAW COUNT:", raw_count)

    docs = list(
        raw_collection.find({
            "created_utc": {"$exists": True, "$ne": None}
        })
        .sort("created_utc", -1)
        .limit(WINDOW_SIZE)
    )

    inserted = 0

    for doc in docs:
        try:
            raw = doc.get("raw", {})

            rt_collection.update_one(
                {"id": doc["id"]},
                {"$set": {
                    "id": doc["id"],
                    "subreddit": doc.get("subreddit"),
                    "created_utc": doc.get("created_utc"),
                    "score": raw.get("score", 0),
                    "num_comments": raw.get("num_comments", 0),
                    "updated_at": datetime.utcnow()
                }},
                upsert=True
            )
            inserted += 1
        except Exception as e:
            print("❌ INIT ERROR:", e)

    print("✅ INIT DONE:", inserted)


# ================= SYNC FROM RAW =================
def sync_new_from_raw():
    print("📥 Sync new posts from RAW")

    latest = rt_collection.find_one(sort=[("created_utc", -1)])
    latest_time = latest["created_utc"] if latest else 0

    new_docs = raw_collection.find({
        "created_utc": {"$gt": latest_time}
    }).sort("created_utc", -1)

    added = 0

    for doc in new_docs:
        try:
            raw = doc.get("raw", {})

            rt_collection.update_one(
                {"id": doc["id"]},
                {"$set": {
                    "id": doc["id"],
                    "subreddit": doc.get("subreddit"),
                    "created_utc": doc.get("created_utc"),
                    "score": raw.get("score", 0),
                    "num_comments": raw.get("num_comments", 0),
                    "updated_at": datetime.utcnow()
                }},
                upsert=True
            )
            added += 1
        except Exception as e:
            print("❌ SYNC ERROR:", e)

    print("➕ Added from RAW:", added)


# ================= UPDATE FROM API =================
def update_existing():
    if not USE_API_UPDATE:
        return

    print("🔄 Updating realtime posts (API)...")

    docs = list(rt_collection.find({}, {"id": 1}))

    for doc in docs:
        post_id = doc["id"]

        url = f"https://www.reddit.com/comments/{post_id}.json"

        try:
            res = requests.get(url, headers=HEADERS, timeout=10)

            if res.status_code == 429:
                print("⛔ RATE LIMIT → sleep 5s")
                time.sleep(5)
                continue

            if res.status_code != 200:
                continue

            post_data = res.json()[0]["data"]["children"][0]["data"]

            rt_collection.update_one(
                {"id": post_id},
                {
                    "$set": {
                        "score": post_data.get("score", 0),
                        "num_comments": post_data.get("num_comments", 0),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            print("❌ UPDATE ERROR:", e)


# ================= SLIDING WINDOW =================
def enforce_window():
    total = rt_collection.count_documents({})

    if total <= WINDOW_SIZE:
        return

    remove_count = total - WINDOW_SIZE

    print(f"🧹 Removing {remove_count} old posts...")

    old_posts = rt_collection.find().sort("created_utc", 1).limit(remove_count)
    ids = [p["id"] for p in old_posts]

    rt_collection.delete_many({"id": {"$in": ids}})


# ================= MAIN LOOP =================
def run():
    print("🔥 MODULE 02 — REALTIME START")

    try:
        client.server_info()
        print("✅ Mongo Connected")
    except Exception as e:
        print("❌ Mongo Error:", e)
        return

    # FORCE INIT để đảm bảo có data
    init_load()

    while True:
        print("\n===== UPDATE CYCLE =====")

        sync_new_from_raw()
        update_existing()
        enforce_window()

        total = rt_collection.count_documents({})
        print("📊 REALTIME SIZE:", total)

        time.sleep(UPDATE_INTERVAL)


# ================= START =================
if __name__ == "__main__":
    run()