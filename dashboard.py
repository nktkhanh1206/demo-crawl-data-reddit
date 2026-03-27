import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh # Thư viện tự làm mới

# ================= 1. CẤU HÌNH (CONFIG) =================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "reddit_db"
RT_COL = "posts_realtime"

st.set_page_config(
    page_title="Reddit Trend Analyzer", 
    layout="wide", 
    page_icon="🔥"
)

# TỰ ĐỘNG LÀM MỚI DASHBOARD MỖI 1 GIÂY (1000ms)
st_autorefresh(interval=1000, key="datarefresh")

# ================= 2. KẾT NỐI & LOAD DATA =================
@st.cache_resource
def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        return client[DB_NAME]
    except Exception as e:
        return None

@st.cache_data(ttl=5) # Cache chỉ tồn tại 5s để luôn có data mới
def load_data():
    db = get_db()
    if db is None: return pd.DataFrame()
    
    # Lấy dữ liệu trong vòng 24h qua
    now_ts = datetime.now(timezone.utc).timestamp()
    threshold = int(now_ts - (24 * 3600))
    
    data = list(db[RT_COL].find({"created_utc": {"$gte": threshold}}))
    if not data: return pd.DataFrame()
    
    df = pd.DataFrame(data)
    for col in ['score', 'num_comments', 'title', 'subreddit', 'created_utc']:
        if col not in df.columns: df[col] = 0
            
    df['score'] = pd.to_numeric(df['score']).fillna(0)
    df['num_comments'] = pd.to_numeric(df['num_comments']).fillna(0)
    
    # Tính toán Trending Score
    df["age_hours"] = (now_ts - df["created_utc"]) / 3600
    df["trending_score"] = (df["score"] + df["num_comments"] * 2.5) / ((df["age_hours"] + 2) ** 1.5)
    return df

# ================= 3. GIAO DIỆN CHÍNH =================
df_raw = load_data()

# --- SIDEBAR (BẢNG ĐIỀU KHIỂN GỌN GÀNG) ---
st.sidebar.header("🛠️ Tùy chọn hiển thị")

if not df_raw.empty:
    all_subs = sorted([str(s) for s in df_raw["subreddit"].unique() if s])
    
    # Bộ lọc đa chọn (Mặc định chọn tất cả)
    selected_subs = st.sidebar.multiselect(
        "🎯 Chọn Subreddits:", 
        options=all_subs, 
        default=all_subs
    )
    
    # Thanh trượt giới hạn bài viết
    top_n = st.sidebar.slider("🔥 Số lượng bài Trending:", 5, 50, 10)
    
    st.sidebar.divider()
    st.sidebar.write("✅ **Chế độ:** Tự động cập nhật (1s)")
    st.sidebar.info(f"Tổng cộng: {len(df_raw)} bài viết từ {len(all_subs)} cộng đồng.")
else:
    st.sidebar.warning("Đang chờ dữ liệu...")
    selected_subs = []
    top_n = 10

# --- LỌC DỮ LIỆU ---
df = df_raw[df_raw["subreddit"].isin(selected_subs)] if selected_subs else df_raw

# --- HIỂN THỊ NỘI DUNG ---
st.title("🔥 Reddit Realtime Trend Dashboard")
# Hiển thị thời gian cập nhật để người dùng thấy nó đang nhảy số
st.write(f"⏱️ Dữ liệu mới nhất lúc: **{datetime.now().strftime('%H:%M:%S')}**")

# 1. METRICS
m1, m2, m3, m4 = st.columns(4)
m1.metric("📦 Tổng bài viết", f"{len(df):,}")
m2.metric("👍 Tổng Score", f"{int(df['score'].sum()):,}")
m3.metric("💬 Tổng Bình luận", f"{int(df['num_comments'].sum()):,}")
m4.metric("📈 Điểm Trend TB", f"{df['trending_score'].mean():.2f}")

st.divider()

# 2. BIỂU ĐỒ
col_left, col_right = st.columns([6, 4])

with col_left:
    st.subheader("📊 Top 10 Tương tác theo Subreddit")
    chart_df = df.groupby("subreddit")["score"].sum().reset_index().sort_values("score", ascending=False).head(10)
    fig_bar = px.bar(
        chart_df, x="subreddit", y="score", color="score",
        template="plotly_dark", height=400, color_continuous_scale="Viridis"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("🍕 Tỷ lệ bài viết")
    fig_pie = px.pie(df, names='subreddit', hole=0.4, template="plotly_dark", height=400)
    st.plotly_chart(fig_pie, use_container_width=True)

# 3. BẢNG DỮ LIỆU TRENDING
st.subheader(f"🚀 Top {top_n} Bài viết bùng nổ nhất")
top_trending = df.sort_values("trending_score", ascending=False).head(top_n)

st.dataframe(
    top_trending[["subreddit", "title", "score", "num_comments", "trending_score"]],
    column_config={
        "subreddit": "Cộng đồng",
        "title": "Tiêu đề bài viết",
        "score": st.column_config.NumberColumn("👍 Score"),
        "num_comments": st.column_config.NumberColumn("💬 Cmt"),
        "trending_score": st.column_config.NumberColumn("Độ nóng 🔥", format="%.2f")
    },
    hide_index=True,
    use_container_width=True
)