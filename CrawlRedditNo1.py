import asyncio
import aiohttp
import random
import sys
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

# ================= CONFIG (TỐI ƯU CHO r/all) =================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "reddit_db"
POST_COL = "posts_raw"
CMT_COL = "comments_raw"

# Danh sách lấy diện rộng
SUBREDDITS = ["all", "gaming", "news", "technology", "funny", "askreddit", "worldnews", "science"]

def get_headers():
    """User-Agent xịn để giả lập trình duyệt, né lỗi 403/429"""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    return {
        "User-Agent": random.choice(uas),
        "Accept": "application/json",
        "Referer": "https://www.google.com/"
    }

# ================= CRAWL LOGIC = :rocket: =================
async def fetch_comments(session, post_id, db):
    """Lấy comment qua JSON API [1]"""
    url = f"https://www.reddit.com/comments/{post_id}.json?limit=20"
    try:
        await asyncio.sleep(random.uniform(1.0, 2.0)) # Nghỉ Jitter né 429
        async with session.get(url, headers=get_headers(), timeout=10) as res:
            if res.status == 200:
                data = await res.json()
                raw_cmts = data[1]["data"]["children"]
                count = 0
                for c in raw_cmts:
                    if c["kind"] == "t1":
                        c_data = c["data"]
                        doc = {
                            "id": c_data["id"], "post_id": post_id,
                            "body": c_data.get("body", ""), "author": c_data.get("author"),
                            "score": c_data.get("score", 0), "created_utc": c_data.get("created_utc"),
                            "crawl_time": datetime.now(timezone.utc)
                        }
                        await db[CMT_COL].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
                        count += 1
                return count
            elif res.status == 429: return -429
    except: pass
    return 0

async def run():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # ÉP BUỘC TẠO INDEX (Giúp Database hiện hồn trong Compass ngay)
    await db[POST_COL].create_index("id", unique=True)
    await db[CMT_COL].create_index("id", unique=True)

    print(f"🚀 [Module 01] Đang lấy API r/all... Đang nạp vào {DB_NAME}")

    async with aiohttp.ClientSession() as session:
        while True:
            for sub in SUBREDDITS:
                print(f"\n📡 Đang quét chuyên mục: r/{sub}")
                url = f"https://www.reddit.com/r/{sub}/hot.json?limit=10"
                
                try:
                    async with session.get(url, headers=get_headers()) as res:
                        if res.status == 429:
                            print("⚠️ Lỗi 429! Reddit bảo nghỉ ngơi. Dừng 60s...")
                            await asyncio.sleep(60)
                            continue
                        
                        if res.status != 200: continue
                        
                        posts = (await res.json())["data"]["children"]
                        for p in posts:
                            p_data = p["data"]
                            p_id = p_data["id"]
                            
                            post_doc = {
                                "id": p_id, "subreddit": p_data["subreddit"],
                                "title": p_data["title"], "score": p_data["score"],
                                "num_comments": p_data["num_comments"],
                                "created_utc": p_data["created_utc"],
                                "crawl_time": datetime.now(timezone.utc)
                            }
                            # GHI VÀO DB
                            await db[POST_COL].update_one({"id": p_id}, {"$set": post_doc}, upsert=True)
                            
                            # LẤY CMT
                            cmt_count = await fetch_comments(session, p_id, db)
                            if cmt_count == -429:
                                print("\n🛑 Chạm giới hạn 429 khi lấy comment. Nghỉ 30s...")
                                await asyncio.sleep(30)
                            
                            sys.stdout.write(f"\r✅ Xong: {p_id} | +{cmt_count if cmt_count > 0 else 0} cmt")
                            sys.stdout.flush()
                            
                            # NGHỈ GIỮA CÁC BÀI (QUAN TRỌNG NHẤT)
                            await asyncio.sleep(random.uniform(3, 6))

                except Exception as e:
                    print(f"\n❌ Lỗi: {e}")
                    await asyncio.sleep(10)
            
            print("\n😴 Hoàn tất 1 vòng. Nghỉ 20s trước vòng mới...")
            await asyncio.sleep(20)

if __name__ == "__main__":
    asyncio.run(run())