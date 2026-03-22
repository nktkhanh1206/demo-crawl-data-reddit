from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

# ================= CONFIG =================
USE_REALTIME = True  # True: posts_realtime | False: posts_raw

# ================= CONNECT =================
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]

collection = db["posts_realtime"] if USE_REALTIME else db["posts_raw"]


# ================= TIME =================
def get_time_threshold():
    return datetime.now(timezone.utc) - timedelta(hours=24)


# ================= DETECT FIELD =================
def detect_fields():
    doc = collection.find_one()
    if not doc:
        return None, None, None

    # detect time
    if "timestamp" in doc:
        time_field = "timestamp"
    elif "created_utc" in doc:
        time_field = "created_utc"
    else:
        time_field = None

    # detect score/comment
    if "score" in doc:
        score_field = "$score"
        comment_field = "$num_comments"
    else:
        score_field = "$raw.score"
        comment_field = "$raw.num_comments"

    return time_field, score_field, comment_field


TIME_FIELD, SCORE_FIELD, COMMENT_FIELD = detect_fields()


# ================= MATCH =================
def get_match_condition():
    threshold = get_time_threshold()

    if TIME_FIELD == "timestamp":
        return {"timestamp": {"$gte": threshold}}

    elif TIME_FIELD == "created_utc":
        return {"created_utc": {"$gte": int(threshold.timestamp())}}

    return {}


# ================= TIME EXPRESSION =================
def get_time_expr(now):
    if TIME_FIELD == "timestamp":
        return "$timestamp"
    elif TIME_FIELD == "created_utc":
        return {"$toDate": {"$multiply": ["$created_utc", 1000]}}
    return now


# ================= HOT TOPICS =================
def get_hot_topics(limit=10):
    pipeline = [
        {"$match": get_match_condition()},
        {
            "$addFields": {
                "score_val": SCORE_FIELD,
                "comment_val": COMMENT_FIELD
            }
        },
        {
            "$group": {
                "_id": "$subreddit",
                "total_score": {"$sum": "$score_val"},
                "total_comments": {"$sum": "$comment_val"},
                "post_count": {"$sum": 1},
                "top_post_score": {"$max": "$score_val"}
            }
        },
        {
            "$addFields": {
                "topic_score": {
                    "$add": ["$total_score", "$total_comments"]
                },
                "avg_score": {
                    "$divide": ["$total_score", "$post_count"]
                }
            }
        },
        {"$sort": {"topic_score": -1}},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


# ================= TRENDING POSTS =================
def get_trending_posts(limit=10):
    now = datetime.now(timezone.utc)
    time_expr = get_time_expr(now)

    pipeline = [
        {"$match": get_match_condition()},
        {
            "$addFields": {
                "score_val": SCORE_FIELD,
                "comment_val": COMMENT_FIELD
            }
        },
        {
            "$addFields": {
                "age_hours": {
                    "$divide": [
                        {"$subtract": [now, time_expr]},
                        1000 * 60 * 60
                    ]
                }
            }
        },
        {
            "$addFields": {
                "trending_score": {
                    "$divide": [
                        {"$add": ["$score_val", {"$multiply": ["$comment_val", 2]}]},
                        {"$add": ["$age_hours", 2]}
                    ]
                }
            }
        },
        {"$sort": {"trending_score": -1}},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


# ================= PREDICT TOPICS =================
def predict_hot_topics(limit=5):
    now = datetime.now(timezone.utc)
    time_expr = get_time_expr(now)

    pipeline = [
        {"$match": get_match_condition()},
        {
            "$addFields": {
                "score_val": SCORE_FIELD,
                "comment_val": COMMENT_FIELD
            }
        },
        {
            "$addFields": {
                "age_hours": {
                    "$divide": [
                        {"$subtract": [now, time_expr]},
                        1000 * 60 * 60
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$subreddit",
                "growth_score": {
                    "$sum": {
                        "$divide": [
                            {"$add": ["$score_val", "$comment_val"]},
                            {"$add": ["$age_hours", 1]}
                        ]
                    }
                }
            }
        },
        {"$sort": {"growth_score": -1}},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


# ================= PREDICT POSTS =================
def predict_trending_posts(limit=5):
    now = datetime.now(timezone.utc)
    time_expr = get_time_expr(now)

    pipeline = [
        {"$match": get_match_condition()},
        {
            "$addFields": {
                "score_val": SCORE_FIELD,
                "comment_val": COMMENT_FIELD
            }
        },
        {
            "$addFields": {
                "age_hours": {
                    "$divide": [
                        {"$subtract": [now, time_expr]},
                        1000 * 60 * 60
                    ]
                }
            }
        },
        {
            "$addFields": {
                "predict_score": {
                    "$divide": [
                        {"$add": ["$score_val", {"$multiply": ["$comment_val", 2]}]},
                        {"$add": ["$age_hours", 1]}
                    ]
                }
            }
        },
        {"$sort": {"predict_score": -1}},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


# ================= DEBUG =================
def debug_data():
    total = collection.count_documents({})
    recent = collection.count_documents(get_match_condition())

    print("\n📦 TOTAL:", total)
    print("⏰ 24H:", recent)
    print("🧠 TIME FIELD:", TIME_FIELD)


# ================= PRINT =================
def print_hot_topics(data):
    print("\n🔥 HOT TOPICS (24H)\n")

    for i, t in enumerate(data, 1):
        print(f"{i}. 📌 Subreddit: {t['_id']}")
        print(f"   📊 Tổng điểm: {int(t['topic_score'])}")
        print(f"   👍 Score: {int(t['total_score'])}")
        print(f"   💬 Comments: {int(t['total_comments'])}")
        print(f"   🧾 Số bài: {t['post_count']}")
        print(f"   ⭐ Avg Score: {round(t['avg_score'],2)}")
        print(f"   🚀 Top Post Score: {t['top_post_score']}")
        print("   ---------------------------")


def print_trending_posts(data):
    print("\n🚀 TRENDING POSTS (24H)\n")

    for i, p in enumerate(data, 1):
        title = p.get("title") or p.get("raw", {}).get("title", "No Title")

        print(f"{i}. 📝 {title}")
        print(f"   👍 {p.get('score_val', 0)} | 💬 {p.get('comment_val', 0)}")
        print(f"   🔥 Trending Score: {round(p.get('trending_score',0),2)}")
        print("   ---------------------------")


def print_predict_topics(data):
    print("\n🔮 FUTURE HOT TOPICS\n")

    for i, t in enumerate(data, 1):
        print(f"{i}. 📌 {t['_id']}")
        print(f"   📈 Growth Score: {round(t['growth_score'],2)}")
        print("   → Có khả năng bùng nổ 🚀")
        print("   ---------------------------")


def print_predict_posts(data):
    print("\n⚡ FUTURE TRENDING POSTS\n")

    for i, p in enumerate(data, 1):
        print(f"{i}. 🆔 {p.get('id')}")
        print(f"   🔮 Predict Score: {round(p.get('predict_score',0),2)}")
        print("   → Đang tăng rất nhanh ⚡")
        print("   ---------------------------")


# ================= MAIN =================
if __name__ == "__main__":
    print("📊 MODULE 04 — ANALYTICS START")

    try:
        client.server_info()
        print("✅ Mongo Connected")
    except Exception as e:
        print("❌ Mongo Error:", e)
        exit()

    debug_data()

    print_hot_topics(get_hot_topics())
    print_trending_posts(get_trending_posts())
    print_predict_topics(predict_hot_topics())
    print_predict_posts(predict_trending_posts())

    print("\n🎯 DONE")