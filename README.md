# 🚀 REDDIT DATA PIPELINE: REAL-TIME TREND ANALYZER
==================================================
Hệ thống phân tích dữ liệu Reddit theo thời gian thực, sử dụng kiến trúc Pipeline 
bất đồng bộ để thu thập, xử lý và dự báo xu hướng.

1. MỤC TIÊU HỆ THỐNG
==================================================
- Thu thập dữ liệu bài viết và bình luận từ Reddit API.
- Xử lý dữ liệu gần real-time (Near Real-time) với độ trễ thấp.
- Lưu trữ và quản lý dữ liệu phân cấp (Raw vs Realtime).
- Thuật toán dự báo bài viết Trending dựa trên gia tốc tăng trưởng.

2. KIẾN TRÚC TỔNG QUAN (DECOUPLED ARCHITECTURE)
==================================================
Kiến trúc tách rời giúp các module hoạt động độc lập qua Database trung tâm:

    [Reddit API] 
         ↓ (JSON)
    [Module 01: Ingestion - Async Crawler]
         ↓
    [MongoDB: posts_raw & comments_raw]
         ↓
    [Module 02: Real-time Engine (Sync & Update)]
         ↓
    [MongoDB: posts_realtime (Window: 500)]
         ↓ --------------------------
         ↓                          ↓
    [Module 04: Analytics]     [Module 03: Replay]
    (Trend & Prediction)       (Simulation Layer)
         ↓
    [Module 05: Dashboard]
    (Streamlit UI)

3. CHI TIẾT CÁC THÀNH PHẦN (COMPONENTS)
==================================================

3.1 Module 01 — Data Ingestion (Crawler)
- Công nghệ: aiohttp, asyncio, Motor (Async MongoDB driver).
- Chức năng: Cào đa nguồn (All, News, Gaming...), tách biệt Posts và Comments.
- Cơ chế: Upsert dựa trên ID để tránh trùng lặp dữ liệu.

3.2 Module 02 — Real-time Processing
- Chức năng: Duy trì Sliding Window 500 bài viết mới nhất.
- Tối ưu: Chỉ cập nhật Score/Comments cho các bài viết < 12h để né Rate Limit (429).
- Vai trò: Đảm bảo dữ liệu bảng "Nóng" luôn chính xác.

3.3 Module 03 — Async Replay Streaming
- Chế độ: Timeline (theo thời gian thực) | Fast (tốc độ cao).
- Vai trò: Phát lại luồng dữ liệu để demo và kiểm thử thuật toán.

3.4 Module 04 — Analytics & Prediction
- Thuật toán Gravity: Score = (Upvotes + Cmt * 2.5) / (Age_hours + 2)^1.8
- Vai trò: Tính toán vận tốc (Velocity) để tìm ra bài viết sắp Viral.

3.5 Module 05 — Visualization Dashboard
- Công nghệ: Streamlit, Plotly.
- Vai trò: Giao diện trực quan hóa Dashboard cho người dùng cuối.

4. THIẾT KẾ DỮ LIỆU (DATABASE SCHEMA)
==================================================
- reddit_db.posts_raw    : Kho lưu trữ gốc (Immutable).
- reddit_db.comments_raw : Kho lưu trữ bình luận (Linked by post_id).
- reddit_db.posts_realtime: Buffer lưu trữ 500 bài viết đang hoạt động.

5. TÍNH CHẤT NỔI BẬT
==================================================
- Asynchronous: Hiệu suất cao, không nghẽn mạch khi xử lý lượng lớn dữ liệu.
- Rate-Limit Handling: Tự động điều tiết request để tránh bị Reddit chặn IP.
- Decoupled: Các Module không phụ thuộc trực tiếp, lỗi 1 phần không sập toàn hệ thống.

6. HƯỚNG DẪN CHẠY HỆ THỐNG
==================================================
1. Cài đặt môi trường:
   pip install aiohttp motor pymongo streamlit pandas plotly

2. Khởi động Master Script:
   python RUN_SYSTEM.bat (hoặc chạy từng module theo thứ tự 01 -> 02 -> 04 -> 05)

==================================================
7. TÍNH CHẤT NỔI BẬT (SYSTEM PROPERTIES)
==================================================
- High-Performance Async: Sử dụng asyncio và aiohttp giúp cào hàng trăm bài 
  viết mỗi phút mà không làm treo hệ thống.
- Decoupled Architecture: Các Module giao tiếp qua Database (MongoDB). Nếu 
  Module 01 dừng, Module 04 và Dashboard vẫn hoạt động dựa trên dữ liệu cũ.
- Data Flattening: Phẳng hóa dữ liệu JSON phức tạp của Reddit thành các trường 
  dễ truy vấn (Score, Title, Subreddit) ngay từ đầu phễu.
- Intelligent Rate Limiting: Cơ chế Semaphore và Random Delay giúp "ẩn mình" 
  né tránh thuật toán chống bot của Reddit (HTTP 429).
- Predictive Analytics: Không chỉ thống kê quá khứ, hệ thống còn tính toán 
  gia tốc (Velocity) để dự báo xu hướng tương lai.

==================================================
8. HẠN CHẾ (LIMITATIONS)
==================================================
- Phụ thuộc Polling: Hệ thống vẫn phải "hỏi" Reddit liên tục (Polling) thay 
  vì được Reddit chủ động báo tin (Push/Webhook).
- Giới hạn IP: Nếu chạy quá nhiều luồng song song trên 1 máy tính, Reddit 
  có thể chặn IP tạm thời.
- Độ trễ Real-time: Do chu kỳ quét là 60 giây, dữ liệu trên Dashboard sẽ chậm 
  hơn thực tế trên Reddit khoảng 1 phút.
- Chưa phân tích nội dung sâu: Hiện tại mới chỉ dựa trên các con số (Score, 
  Comments) mà chưa "đọc" hiểu nội dung văn bản bên trong bài viết.

==================================================
9. HƯỚNG NÂNG CẤP (FUTURE UPGRADES)
==================================================
- Tích hợp NLP (Natural Language Processing): Sử dụng thư viện như TextBlob 
  hoặc VADER để phân tích cảm xúc (Sentiment) của các bình luận.
- Hệ thống Cảnh báo (Alerting): Tích hợp Telegram Bot hoặc Email để nhắn tin 
  ngay khi phát hiện một bài viết có Velocity Score vượt ngưỡng 100.
- Mở rộng nguồn tin: Không chỉ Reddit, có thể tích hợp thêm X (Twitter) hoặc 
  Facebook Group vào cùng một Pipeline.
- Lưu trữ lâu dài (Cold Storage): Chuyển các bài viết sau 7 ngày sang 
  BigQuery hoặc S3 để lưu trữ dữ liệu lớn (Big Data) phục vụ training AI.
- Giao diện Live-Update: Sử dụng WebSocket để Dashboard tự nhảy số mà không 
  cần nhấn nút "Refresh".

==================================================
10. KẾT LUẬN (CONCLUSION)
==================================================
Dự án demo-crawl-data-reddit là một minh chứng cho việc xây dựng một luồng 
dữ liệu (Data Pipeline) hoàn chỉnh. Từ việc thu thập thô đến khi trở thành 
các biểu đồ phân tích xu hướng, hệ thống đã giải quyết tốt bài toán về 
hiệu suất, tính toàn vẹn dữ liệu và khả năng mở rộng trong tương lai.