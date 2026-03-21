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
collection = db["realtime_posts"]

collection.create_index("id", unique=True)

# ================= HEADERS =================
HEADERS = {
    "User-Agent": "Mozilla/5.0 realtime-bot/2.0"
}

# ================= FETCH NEW POSTS =================
def fetch_new_posts():
    url = f"https://www.reddit.com/r/{SUBREDDIT}/new.json?limit=100"

    try:
        res = requests.get(url, headers=HEADERS, timeout=10)

        if res.status_code != 200:
            print("❌ Fetch error:", res.status_code)
            return []

        data = res.json()
        return [p["data"] for p in data["data"]["children"]]

    except Exception as e:
        print("❌ Fetch exception:", e)
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
    print("🚀 INIT LOAD...")

    posts = fetch_new_posts()
    inserted = 0

    for post in posts:
        try:
            collection.insert_one(format_post(post))
            inserted += 1

            if inserted >= WINDOW_SIZE:
                break

        except:
            continue

    print("✅ Loaded:", inserted)

# ================= UPDATE POSTS =================
def update_existing_posts():
    print("🔄 Updating posts...")

    docs = list(collection.find({}, {"id": 1}))

    for doc in docs:
        post_id = doc["id"]

        url = f"https://www.reddit.com/comments/{post_id}.json"

        try:
            res = requests.get(url, headers=HEADERS, timeout=10)

            if res.status_code != 200:
                continue

            data = res.json()
            post_data = data[0]["data"]["children"][0]["data"]

            collection.update_one(
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

# ================= ADD NEW POSTS =================
def add_new_posts():
    print("🆕 Adding new posts...")

    posts = fetch_new_posts()
    added = 0

    for post in posts:
        if collection.find_one({"id": post["id"]}):
            continue

        try:
            collection.insert_one(format_post(post))
            added += 1
        except:
            continue

    print("➕ Added:", added)

# ================= ENFORCE WINDOW =================
def enforce_window():
    total = collection.count_documents({})

    if total <= WINDOW_SIZE:
        return

    remove_count = total - WINDOW_SIZE

    print(f"🧹 Removing {remove_count} old posts...")

    old_posts = collection.find().sort("created_utc", 1).limit(remove_count)

    ids = [p["id"] for p in old_posts]

    collection.delete_many({"id": {"$in": ids}})

# ================= MAIN LOOP =================
def run():
    print("🔥 REAL-TIME SYSTEM START")

    if collection.count_documents({}) == 0:
        init_load()

    while True:
        print("\n===== UPDATE CYCLE =====")

        update_existing_posts()   # update score + comments
        add_new_posts()           # thêm bài mới
        enforce_window()          # giữ đúng 500 bài

        total = collection.count_documents({})
        print("📊 TOTAL:", total)

        print(f"⏳ Sleep {UPDATE_INTERVAL}s...\n")
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