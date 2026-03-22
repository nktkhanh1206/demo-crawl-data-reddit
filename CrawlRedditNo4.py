from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

# ================= CONNECT =================
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]

collection = db["posts_realtime"]
# collection = db["posts_raw"]

# ================= TIME =================
def get_time_threshold():
    return datetime.now(timezone.utc) - timedelta(hours=24)


# ================= DETECT FIELD =================
def use_timestamp():
    doc = collection.find_one()
    if not doc:
        return "none"

    if "timestamp" in doc:
        return "timestamp"
    elif "created_utc" in doc:
        return "created_utc"
    else:
        return "none"


TIME_FIELD = use_timestamp()


# ================= MATCH CONDITION =================
def get_match_condition():
    threshold = get_time_threshold()

    if TIME_FIELD == "timestamp":
        return {"timestamp": {"$gte": threshold}}

    elif TIME_FIELD == "created_utc":
        unix_time = int(threshold.timestamp())
        return {"created_utc": {"$gte": unix_time}}

    else:
        return {}  # fallback (no filter)


# ================= HOT TOPICS =================
def get_hot_topics(limit=10):
    pipeline = [
        {"$match": get_match_condition()},
        {
            "$group": {
                "_id": "$subreddit",
                "total_comments": {"$sum": "$num_comments"},
                "total_score": {"$sum": "$score"}
            }
        },
        {
            "$addFields": {
                "topic_score": {
                    "$add": ["$total_comments", "$total_score"]
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

    if TIME_FIELD == "timestamp":
        time_expr = "$timestamp"
    elif TIME_FIELD == "created_utc":
        time_expr = {
            "$toDate": {"$multiply": ["$created_utc", 1000]}
        }
    else:
        return []

    pipeline = [
        {"$match": get_match_condition()},
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
                        {"$add": ["$score", {"$multiply": ["$num_comments", 2]}]},
                        {"$add": ["$age_hours", 2]}
                    ]
                }
            }
        },
        {"$sort": {"trending_score": -1}},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


# ================= PREDICT HOT TOPICS =================
def predict_hot_topics(limit=5):
    now = datetime.now(timezone.utc)

    if TIME_FIELD == "timestamp":
        time_expr = "$timestamp"
    elif TIME_FIELD == "created_utc":
        time_expr = {
            "$toDate": {"$multiply": ["$created_utc", 1000]}
        }
    else:
        return []

    pipeline = [
        {"$match": get_match_condition()},
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
                            {"$add": ["$score", "$num_comments"]},
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


# ================= PREDICT TRENDING POSTS =================
def predict_trending_posts(limit=5):
    now = datetime.now(timezone.utc)

    if TIME_FIELD == "timestamp":
        time_expr = "$timestamp"
    elif TIME_FIELD == "created_utc":
        time_expr = {
            "$toDate": {"$multiply": ["$created_utc", 1000]}
        }
    else:
        return []

    pipeline = [
        {"$match": get_match_condition()},
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
                        {"$add": ["$score", {"$multiply": ["$num_comments", 2]}]},
                        {"$add": ["$age_hours", 1]}
                    ]
                }
            }
        },
        {"$sort": {"predict_score": -1}},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


# ================= PRINT =================
def print_hot_topics(data):
    print("\n🔥 HOT TOPICS (24H)")
    for i, t in enumerate(data, 1):
        print(f"{i}. {t['_id']} | Score: {t.get('topic_score', 0)}")


def print_trending_posts(data):
    print("\n🚀 TRENDING POSTS (24H)")
    for i, p in enumerate(data, 1):
        print(f"{i}. {p.get('title', 'No Title')}")
        print(f"   👍 {p.get('score', 0)} | 💬 {p.get('num_comments', 0)} | 🔥 {round(p.get('trending_score', 0),2)}")


def print_predict_topics(data):
    print("\n🔮 PREDICT HOT TOPICS")
    for i, t in enumerate(data, 1):
        print(f"{i}. {t['_id']} | Growth: {round(t.get('growth_score', 0),2)}")


def print_predict_posts(data):
    print("\n⚡ PREDICT TRENDING POSTS")
    for i, p in enumerate(data, 1):
        print(f"{i}. {p.get('title', 'No Title')}")
        print(f"   👍 {p.get('score', 0)} | 💬 {p.get('num_comments', 0)} | 🔮 {round(p.get('predict_score', 0),2)}")


# ================= DEBUG =================
def debug_data():
    total = collection.count_documents({})
    recent = collection.count_documents(get_match_condition())

    print("\n📦 TOTAL DOCS:", total)
    print("⏰ DOCS 24H:", recent)
    print("🧠 USING FIELD:", TIME_FIELD)


# ================= MAIN =================
if __name__ == "__main__":
    print("📊 MODULE 04 — ANALYTICS START")

    try:
        client.server_info()
        print("✅ MongoDB Connected")
    except Exception as e:
        print("❌ MongoDB Error:", e)
        exit()

    debug_data()

    hot_topics = get_hot_topics()
    trending_posts = get_trending_posts()
    predict_topics = predict_hot_topics()
    predict_posts = predict_trending_posts()

    print_hot_topics(hot_topics)
    print_trending_posts(trending_posts)
    print_predict_topics(predict_topics)
    print_predict_posts(predict_posts)

    print("\n🎯 DONE")