import asyncio
import aiohttp
import random
from datetime import datetime
from pymongo import MongoClient

# ================= LOAD CONFIG =================
def load_config(path="config.properties"):
    config = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
    return config

# ================= USER AGENT =================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Referer": "https://www.reddit.com/"
    }

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
        "timestamp": datetime.fromtimestamp(post["created_utc"]),
    }

# ================= SAVE (UPSERT) =================
def save(post, collection):
    try:
        collection.update_one(
            {"id": post["id"]},
            {"$set": format_post(post)},
            upsert=True
        )
        return 1
    except Exception as e:
        print("❌ SAVE ERROR:", e)
        return 0

# ================= FETCH =================
async def fetch(session, subreddit, after):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"

    if after:
        url += f"&after={after}"

    try:
        async with session.get(url, headers=get_headers(), timeout=10) as res:
            print(f"[{subreddit}] STATUS:", res.status)

            if res.status == 429:
                print("⛔ RATE LIMIT → sleep 15s")
                await asyncio.sleep(15)
                return [], after

            if res.status != 200:
                return [], after

            data = await res.json()
            posts = [p["data"] for p in data["data"]["children"]]
            next_after = data["data"].get("after")

            print(f"[{subreddit}] FETCHED:", len(posts))

            return posts, next_after

    except Exception as e:
        print(f"[{subreddit}] ERROR:", e)
        await asyncio.sleep(5)
        return [], after

# ================= MAIN =================
async def run():
    cfg = load_config()

    SUBREDDITS = cfg["subreddits"].split(",")
    TARGET = int(cfg["target_posts"])
    MAX_CONCURRENT = int(cfg["max_concurrent"])
    MIN_DELAY = float(cfg["min_delay"])
    MAX_DELAY = float(cfg["max_delay"])

    # Mongo
    client = MongoClient(cfg["mongo_uri"])
    db = client[cfg["db_name"]]
    collection = db[cfg["collection_name"]]

    total = collection.count_documents({})
    print("🚀 START FROM:", total)

    after_map = {sub: None for sub in SUBREDDITS}
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as session:
        while total < TARGET:

            tasks = []

            for sub in SUBREDDITS:
                async def bound_fetch(s=sub):
                    async with sem:
                        return s, *await fetch(session, s, after_map[s])

                tasks.append(bound_fetch())

            results = await asyncio.gather(*tasks)

            for sub, posts, next_after in results:
                after_map[sub] = next_after

                for post in posts:
                    total += save(post, collection)

                    if total % 100 == 0:
                        print("🔥 TOTAL:", total)

                    if total >= TARGET:
                        break

            sleep_time = random.uniform(MIN_DELAY, MAX_DELAY)
            print(f"⏳ Sleep {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

    print("🎉 DONE:", total)

# ================= RUN =================
if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n🛑 STOPPED")