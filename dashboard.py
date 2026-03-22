import streamlit as st
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta, timezone

# ================= CONFIG =================
USE_REALTIME = True

# ================= CONNECT =================
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]

collection = db["posts_realtime"] if USE_REALTIME else db["posts_raw"]

st.set_page_config(page_title="Reddit Analytics", layout="wide")

# ================= TIME =================
def get_time_threshold():
    return datetime.now(timezone.utc) - timedelta(hours=24)


# ================= LOAD DATA =================
@st.cache_data(ttl=60)
def load_data():
    threshold = int(get_time_threshold().timestamp())

    data = list(collection.find({
        "created_utc": {"$gte": threshold}
    }))

    return pd.DataFrame(data)


df = load_data()

# ================= TITLE =================
st.title("🔥 Reddit Realtime Dashboard")

if df.empty:
    st.warning("No data available")
    st.stop()

# ================= BASIC METRICS =================
col1, col2, col3 = st.columns(3)

col1.metric("📦 Total Posts", len(df))
col2.metric("👍 Total Score", int(df["score"].sum()))
col3.metric("💬 Total Comments", int(df["num_comments"].sum()))

# ================= HOT TOPICS =================
st.subheader("🔥 Hot Topics")

topic_df = df.groupby("subreddit").agg({
    "score": "sum",
    "num_comments": "sum",
    "id": "count"
}).rename(columns={"id": "post_count"})

topic_df["topic_score"] = topic_df["score"] + topic_df["num_comments"]
topic_df = topic_df.sort_values("topic_score", ascending=False).head(10)

st.dataframe(topic_df)

# ================= TRENDING POSTS =================
st.subheader("🚀 Trending Posts")

df["age_hours"] = (
    (datetime.now(timezone.utc).timestamp() - df["created_utc"]) / 3600
)

df["trending_score"] = (
    (df["score"] + df["num_comments"] * 2) / (df["age_hours"] + 2)
)

trending_df = df.sort_values("trending_score", ascending=False).head(10)

st.dataframe(trending_df[["subreddit", "score", "num_comments", "trending_score"]])

# ================= PREDICT =================
st.subheader("🔮 Future Trends")

df["predict_score"] = (
    (df["score"] + df["num_comments"] * 2) / (df["age_hours"] + 1)
)

predict_df = df.sort_values("predict_score", ascending=False).head(10)

st.dataframe(predict_df[["subreddit", "score", "num_comments", "predict_score"]])

# ================= CHART =================
st.subheader("📊 Score Distribution")

chart_df = df.groupby("subreddit")["score"].sum().sort_values(ascending=False).head(10)
st.bar_chart(chart_df)

# ================= FOOTER =================
st.success("✅ Realtime analytics running...")