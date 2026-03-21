import requests
import time
from pymongo import MongoClient
from datetime import datetime

# ================= CONFIG =================
SUBREDDIT = "technology"
WINDOW_SIZE = 500
UPDATE_INTERVAL = 60  # 1 phút

# ================= MONGO =================
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]

raw_collection = db["posts_raw"]          # dữ liệu gốc
rt_collection = db["posts_realtime"]      # dữ liệu realtime

rt_collection.create_index("id", unique=True)

# ================= HEADERS =================
HEADERS = {
    "User-Agent": "Mozilla/5.0 realtime-bot/2.0"
}

# ================= FETCH =================
def fetch_new_posts():
    url = f"https://www.reddit.com/r/{SUBREDDIT}/new.json?limit=100"

    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            return []
        data = res.json()
        return [p["data"] for p in data["data"]["children"]]
    except:
        return []

# ================= FORMAT =================
def format_post(post):
    return {
        "id": post["id"],
        "subreddit": post.get("subreddit"),
        "title": post.get("title"),
        "content": post.get("selftext"),
        "score": post.get("score"),
        "num_comments": post.get("num_comments"),
        "created_utc": post.get("created_utc"),
        "updated_at": datetime.utcnow(),
        "payload": post
    }

# ================= INIT LOAD =================
def init_load():
    print("🚀 INIT LOAD FROM RAW")

    docs = list(raw_collection.find().sort("created_utc", -1).limit(WINDOW_SIZE))

    inserted = 0
    for doc in docs:
        try:
            rt_collection.insert_one(doc)
            inserted += 1
        except:
            continue

    print("✅ Loaded into realtime:", inserted)

# ================= UPDATE =================
def update_existing():
    print("🔄 Updating realtime posts...")

    docs = list(rt_collection.find({}, {"id": 1}))

    for doc in docs:
        post_id = doc["id"]

        url = f"https://www.reddit.com/comments/{post_id}.json"

        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            if res.status_code != 200:
                continue

            post_data = res.json()[0]["data"]["children"][0]["data"]

            rt_collection.update_one(
                {"id": post_id},
                {
                    "$set": {
                        "score": post_data.get("score"),
                        "num_comments": post_data.get("num_comments"),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
        except:
            continue

# ================= ADD NEW =================
def add_new_posts():
    print("🆕 Adding new posts...")

    posts = fetch_new_posts()
    added = 0

    for post in posts:
        if rt_collection.find_one({"id": post["id"]}):
            continue

        try:
            formatted = format_post(post)

            # lưu vào realtime
            rt_collection.insert_one(formatted)

            # lưu luôn vào raw (nếu chưa có)
            raw_collection.update_one(
                {"id": post["id"]},
                {"$setOnInsert": formatted},
                upsert=True
            )

            added += 1
        except:
            continue

    print("➕ Added:", added)

# ================= WINDOW =================
def enforce_window():
    total = rt_collection.count_documents({})

    if total <= WINDOW_SIZE:
        return

    remove_count = total - WINDOW_SIZE

    print(f"🧹 Removing {remove_count} old posts...")

    old_posts = rt_collection.find().sort("created_utc", 1).limit(remove_count)
    ids = [p["id"] for p in old_posts]

    rt_collection.delete_many({"id": {"$in": ids}})

# ================= MAIN =================
def run():
    print("🔥 REALTIME PROCESSOR START")

    if rt_collection.count_documents({}) == 0:
        init_load()

    while True:
        print("\n===== UPDATE CYCLE =====")

        update_existing()
        add_new_posts()
        enforce_window()

        print("📊 REALTIME SIZE:", rt_collection.count_documents({}))
        time.sleep(UPDATE_INTERVAL)

# ================= START =================
if __name__ == "__main__":
    try:
        client.server_info()
        print("✅ Mongo Connected")
    except Exception as e:
        print("❌ Mongo Error:", e)
        exit()

    run()