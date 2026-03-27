import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta, timezone

# ================= CONFIG (KHỚP VỚI COMPASS) =================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "reddit_db"        # SỬA LẠI CHO KHỚP ẢNH
RT_COL = "posts_realtime" 

# Hệ số ưu tiên (Weighting)
SCORE_WEIGHT = 1.0
COMMENT_WEIGHT = 2.5  
GRAVITY = 1.8         

# ================= KẾT NỐI =================
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
collection = db[RT_COL]

# ================= CÔNG THỨC TRENDING (PIPELINE) =================
def get_trending_pipeline(limit=10):
    now = datetime.now(timezone.utc)
    
    return [
        # 1. Lấy dữ liệu 24h qua cho tập mẫu rộng hơn
        {"$match": {
            "created_utc": {"$gte": int((now - timedelta(hours=24)).timestamp())}
        }},
        # 2. Tính toán tuổi bài viết (Phòng hờ lỗi kiểu dữ liệu)
        {"$addFields": {
            "age_hours": {
                "$divide": [
                    {"$subtract": [
                        now, 
                        {"$toDate": {"$multiply": [{"$toLong": "$created_utc"}, 1000]}}
                    ]},
                    1000 * 60 * 60
                ]
            }
        }},
        # 3. Thuật toán Reddit Velocity Score
        # Formula: (Score + Comments * 2.5) / (Age + 2)^1.8
        {"$addFields": {
            "velocity_score": {
                "$divide": [
                    {"$add": ["$score", {"$multiply": ["$num_comments", COMMENT_WEIGHT]}]},
                    {"$pow": [{"$add": ["$age_hours", 2]}, GRAVITY]}
                ]
            }
        }},
        {"$sort": {"velocity_score": -1}},
        {"$limit": limit},
        # 4. Định dạng đầu ra
        {"$project": {
            "subreddit": 1, "title": 1, "score": 1, "num_comments": 1, 
            "velocity_score": 1,
            "age_min": {"$round": [{"$multiply": ["$age_hours", 60]}, 0]}
        }}
    ]

# ================= PHÂN TÍCH SUBREDDIT =================
def get_sub_analytics_pipeline(limit=5):
    return [
        {"$group": {
            "_id": "$subreddit",
            "active_posts": {"$sum": 1},
            "total_engagement": {"$sum": {"$add": ["$score", "$num_comments"]}},
            "avg_score": {"$avg": "$score"}
        }},
        {"$addFields": {
            # Chỉ số 'hotness' dựa trên mật độ bài viết và tương tác trung bình
            "hotness_index": {"$multiply": ["$active_posts", "$avg_score"]}
        }},
        {"$sort": {"hotness_index": -1}},
        {"$limit": limit}
    ]

# ================= RUN =================
async def run_analytics():
    print(f"\n🧠 [BRAIN] PHÂN TÍCH XU HƯỚNG [{datetime.now().strftime('%H:%M:%S')}]")
    print("=" * 70)
    
    try:
        # 1. Top Trending
        trending_posts = await collection.aggregate(get_trending_pipeline(5)).to_list(length=5)
        print("🚀 TOP 5 BÀI VIẾT ĐANG 'VIRAL' MẠNH NHẤT:")
        for i, p in enumerate(trending_posts, 1):
            title = p.get('title', '...')[:55]
            print(f" {i}. [{p['subreddit']}] {title}")
            print(f"    ⚡ Score: {p['velocity_score']:.2f} | 👍 {p['score']} | 💬 {p['num_comments']} | ⏱️ {int(p['age_min'])}m ago")

        # 2. Hot Subreddits
        hot_subs = await collection.aggregate(get_sub_analytics_pipeline(5)).to_list(length=5)
        print("\n🔥 TOP 5 CỘNG ĐỒNG CÓ BIẾN ĐỘNG MẠNH:")
        for s in hot_subs:
            print(f" 📌 r/{s['_id']:<15} | 📝 {s['active_posts']} posts | 📈 Index: {s['hotness_index']:.1f}")

    except Exception as e:
        print(f"❌ Lỗi phân tích: {e}")

async def main():
    print("🎬 MODULE 04 STARTED (Dữ liệu lấy từ posts_realtime)")
    while True:
        await run_analytics()
        await asyncio.sleep(60) # Cập nhật mỗi 1 phút

if __name__ == "__main__":
    asyncio.run(main())