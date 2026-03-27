import asyncio
import aiohttp
import random
import sys
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient

# ================= CẤU HÌNH (ĐÃ TỐI ƯU ĐỂ KHÔNG BỊ CHẶN) =================
WINDOW_SIZE = 500          # Giữ tối đa 500 bài trong bảng Realtime
UPDATE_INTERVAL = 5        # Nghỉ 5 giây mỗi chu kỳ (Né lỗi 429)
MAX_CONCURRENT_API = 2     # Chỉ chạy 2 luồng song song (Né lỗi 403)

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "reddit_db"
RAW_COL = "posts_raw"
RT_COL = "posts_realtime"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}

# Kết nối Database
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
raw_collection = db[RAW_COL]
rt_collection = db[RT_COL]

# ================= 1. SYNC ÉP BUỘC (ĐỂ ĐUỔI KỊP MODULE 1) =================
async def sync_new_from_raw():
    """Lấy bài từ Raw sang Realtime dựa trên crawl_time (thời gian cào)"""
    # Lấy bài mới nhất trong Realtime để làm mốc (dựa trên crawl_time của chính nó)
    latest_doc = await rt_collection.find_one(sort=[("crawl_time", -1)])
    latest_sync_time = latest_doc["crawl_time"] if latest_doc and "crawl_time" in latest_doc else None

    # Tìm bài trong Raw có crawl_time mới hơn mốc đã sync
    query = {"crawl_time": {"$gt": latest_sync_time}} if latest_sync_time else {}
    cursor = raw_collection.find(query).sort("crawl_time", 1)
    new_docs = await cursor.to_list(length=100)

    added = 0
    for doc in new_docs:
        if not doc.get("title"): continue
        
        # Đồng bộ sang bảng Realtime
        await rt_collection.update_one(
            {"id": doc["id"]},
            {"$set": {
                "id": doc["id"],
                "subreddit": doc.get("subreddit"),
                "title": doc.get("title"),
                "created_utc": doc.get("created_utc"),
                "score": doc.get("score", 0),
                "num_comments": doc.get("num_comments", 0),
                "updated_at": datetime.now(timezone.utc),
                "crawl_time": doc.get("crawl_time") # Lưu lại mốc để lần sau so sánh
            }},
            upsert=True
        )
        added += 1
    
    if added > 0:
        print(f"📥 [Sync] Đã nạp +{added} bài viết mới từ Raw sang Realtime.")
    else:
        # Nếu không có bài mới theo mốc thời gian, kiểm tra tổng số lượng để chắc chắn
        total_raw = await raw_collection.count_documents({})
        total_rt = await rt_collection.count_documents({})
        if total_rt < total_raw:
            print(f"📡 Đang cưỡng ép đồng bộ... (Raw: {total_raw} | Realtime: {total_rt})")
            cursor = raw_collection.find().sort("crawl_time", -1).limit(100)
            all_recent = await cursor.to_list(length=100)
            for d in all_recent:
                await rt_collection.update_one({"id": d["id"]}, {"$set": d}, upsert=True)
            print(f"✅ Đã ép đồng bộ xong.")

# ================= 2. UPDATE CHỈ SỐ (REALTIME) =================
async def fetch_updated_stats(session, sem, post_id):
    url = f"https://www.reddit.com/comments/{post_id}.json?limit=1"
    async with sem:
        try:
            await asyncio.sleep(random.uniform(1.5, 3.0)) # Nghỉ né Bot detection
            async with session.get(url, headers=get_headers(), timeout=12) as res:
                if res.status == 200:
                    data = await res.json()
                    p_info = data[0]["data"]["children"][0]["data"]
                    await rt_collection.update_one(
                        {"id": post_id},
                        {"$set": {
                            "score": p_info.get("score", 0),
                            "num_comments": p_info.get("num_comments", 0),
                            "updated_at": datetime.now(timezone.utc)
                        }}
                    )
                    return "OK"
                return f"ERR_{res.status}"
        except: return "FAIL"

async def update_existing_realtime():
    # Chỉ update 15 bài mới nhất để tránh bị Reddit chặn IP (429)
    cursor = rt_collection.find().sort("created_utc", -1).limit(15)
    docs = await cursor.to_list(length=15) 
    if not docs: return

    sem = asyncio.Semaphore(MAX_CONCURRENT_API)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_updated_stats(session, sem, d["id"]) for d in docs]
        results = await asyncio.gather(*tasks)
    
    success = results.count("OK")
    print(f"🔄 [Update] Đã nhảy số cho {success}/15 bài đang hot.")

# ================= 3. GIỚI HẠN DỮ LIỆU =================
async def enforce_window():
    total = await rt_collection.count_documents({})
    if total <= WINDOW_SIZE: return
    remove_count = total - WINDOW_SIZE
    cursor = rt_collection.find({}, {"id": 1}).sort("created_utc", 1).limit(remove_count)
    old_posts = await cursor.to_list(length=remove_count)
    ids = [p["id"] for p in old_posts]
    await rt_collection.delete_many({"id": {"$in": ids}})

# ================= MAIN =================
async def run():
    print("🔥 MODULE 02: REALTIME ENGINE...")
    await rt_collection.create_index("id", unique=True)
    await rt_collection.create_index("crawl_time")

    while True:
        try:
            await sync_new_from_raw()
            await update_existing_realtime()
            await enforce_window()
            
            print(f"✅ Chu kỳ hoàn tất lúc {datetime.now().strftime('%H:%M:%S')}. Nghỉ {UPDATE_INTERVAL}s...")
            await asyncio.sleep(UPDATE_INTERVAL)
        except Exception as e:
            print(f"❌ Lỗi hệ thống: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n🛑 Module 02 đã dừng.")