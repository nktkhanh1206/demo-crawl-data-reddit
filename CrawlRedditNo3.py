import asyncio
import random
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone

# ================= CẤU HÌNH (CONFIG) =================
MODE = "timeline"       # timeline | fast
SPEED = 10              # Gấp 10 lần tốc độ thực tế
FETCH_INTERVAL = 15     # Cập nhật đợt mới sau mỗi 15s

# ĐỊA CHỈ PHẢI KHỚP VỚI COMPASS
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "reddit_db"        # Sửa lại cho khớp ảnh Compass
RT_COL = "posts_realtime" 

# ================= KẾT NỐI DB =================
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
rt_collection = db[RT_COL]

# ================= ĐỊNH DẠNG SỰ KIỆN =================
def format_event(doc):
    return {
        "event_type": "TREND_SIGNAL",
        "id": doc.get("id"),
        "subreddit": doc.get("subreddit"),
        "title": doc.get("title"),
        "score": doc.get("score", 0),
        "num_comments": doc.get("num_comments", 0),
        "created_utc": doc.get("created_utc"),
        "replay_at": datetime.now(timezone.utc)
    }

async def emit_event(event):
    """Giả lập đẩy dữ liệu vào luồng phân tích của Module 04"""
    sys.stdout.write(
        f"\r📡 [REPLAY] {event['replay_at'].strftime('%H:%M:%S')} | "
        f"r/{event['subreddit']} | 👍 {event['score']} | 💬 {event['num_comments']}          "
    )
    sys.stdout.flush()

# ================= CHẾ ĐỘ TIMELINE =================
async def replay_timeline():
    print(f"⏱️ [Timeline] Đang quét 50 bài mới nhất từ {RT_COL}...")
    
    # Lấy 50 bài mới nhất để replay nhịp độ thực tế
    cursor = rt_collection.find().sort("created_utc", 1).limit(50)
    docs = await cursor.to_list(length=50)
    
    if not docs:
        print("⚠️ Kho Realtime đang trống. Hãy đợi Module 02 nạp hàng!")
        return

    last_event_time = docs[0]["created_utc"]

    for doc in docs:
        event_time = doc["created_utc"]
        # Tính wait_time, giới hạn tối đa 2s để tránh bị treo nếu data quá thưa
        diff = (event_time - last_event_time) / SPEED
        wait_time = min(max(0, diff), 2.0) 

        if wait_time > 0:
            await asyncio.sleep(wait_time)

        event = format_event(doc)
        await emit_event(event)
        last_event_time = event_time
    print("\n✅ Đã phát lại xong đợt này.")

# ================= CHẾ ĐỘ FAST =================
async def replay_fast():
    print("🚀 [Fast] Đang xả dữ liệu tốc độ cao...")
    cursor = rt_collection.find().sort("created_utc", -1).limit(50)
    docs = await cursor.to_list(length=50)
    for doc in docs:
        event = format_event(doc)
        await emit_event(event)
        await asyncio.sleep(0.1) # Xả nhanh nhưng vẫn nghỉ để ko treo CPU
    print("\n✅ Xả xong.")

# ================= RUN =================
async def run():
    print("🎬 MODULE 03: ASYNC REPLAY ENGINE STARTED")
    
    # Kiểm tra kết nối DB trước khi chạy
    try:
        await client.admin.command('ping')
        print(f"🔗 Đã kết nối MongoDB: {DB_NAME}")
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return

    while True:
        if MODE == "timeline":
            await replay_timeline()
        else:
            await replay_fast()
            
        await asyncio.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n🛑 Dừng Replay.")