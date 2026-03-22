import time
import threading
from pymongo import MongoClient
from datetime import datetime

# ================= CONFIG =================
MODE = "fast"  # fast | timeline | multi
SOURCE = "realtime"  # raw | realtime

SPEED = 10
BATCH_SIZE = 100
WORKERS = 10

# ================= MONGO =================
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]

raw_collection = db["posts_raw"]
rt_collection = db["posts_realtime"]

collection = rt_collection if SOURCE == "realtime" else raw_collection


# ================= FORMAT EVENT =================
def format_event(doc):
    return {
        "event_type": "REPLAY_POST",
        "id": doc.get("id"),
        "subreddit": doc.get("subreddit"),
        "score": doc.get("score") or doc.get("raw", {}).get("score"),
        "num_comments": doc.get("num_comments") or doc.get("raw", {}).get("num_comments"),
        "created_utc": doc.get("created_utc"),
        "timestamp": datetime.utcnow()
    }


# ================= SEND =================
def send_event(event):
    print(f"📡 {event['id']} | 👍 {event['score']} | 💬 {event['num_comments']}")


# ================= FAST =================
def replay_fast():
    print("🚀 FAST REPLAY START")

    cursor = collection.find().batch_size(BATCH_SIZE)

    count = 0

    for doc in cursor:
        event = format_event(doc)
        send_event(event)
        count += 1

    print(f"🔥 DONE FAST | TOTAL: {count}")


# ================= TIMELINE =================
def replay_timeline():
    print("⏱️ TIMELINE REPLAY START")

    docs = list(collection.find().sort("created_utc", 1))

    if not docs:
        print("❌ No data")
        return

    start_time = docs[0]["created_utc"]

    for doc in docs:
        event_time = doc["created_utc"]
        delay = (event_time - start_time) / SPEED

        time.sleep(max(delay, 0))

        event = format_event(doc)
        send_event(event)

        start_time = event_time

    print("🔥 DONE TIMELINE")


# ================= MULTI THREAD =================
def replay_multithread():
    print("⚡ MULTI-THREAD REPLAY START")

    docs = list(collection.find())
    total = len(docs)

    print("TOTAL:", total)

    def worker(chunk):
        for doc in chunk:
            event = format_event(doc)
            send_event(event)

    chunk_size = max(1, total // WORKERS)
    threads = []

    for i in range(WORKERS):
        start = i * chunk_size
        end = start + chunk_size
        chunk = docs[start:end]

        t = threading.Thread(target=worker, args=(chunk,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("🔥 DONE MULTI")


# ================= MAIN =================
def run():
    print("🎬 MODULE 03 — REPLAY START")

    try:
        client.server_info()
        print("✅ Mongo Connected")
    except Exception as e:
        print("❌ Mongo Error:", e)
        return

    print(f"📦 SOURCE: {SOURCE}")

    if MODE == "fast":
        replay_fast()

    elif MODE == "timeline":
        replay_timeline()

    elif MODE == "multi":
        replay_multithread()


# ================= START =================
if __name__ == "__main__":
    run()