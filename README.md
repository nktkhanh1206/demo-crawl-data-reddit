# demo-crawl-data-reddit
🚀 SYSTEM: REDDIT DATA PIPELINE
==================================================
1. MỤC TIÊU HỆ THỐNG
==================================================
- Thu thập dữ liệu từ Reddit
- Xử lý dữ liệu gần real-time
- Lưu trữ và quản lý dữ liệu hiệu quả
- Hỗ trợ replay để test và phân tích


==================================================
2. KIẾN TRÚC TỔNG QUAN (UPDATED)
==================================================
                Reddit API
                     ↓
        [Module 01 — Crawler]
                     ↓
                MongoDB
              (posts_raw)
                     ↓
        [Module 02 — Realtime]
                     ↓
            (posts_realtime)
                ↓        ↓
     [Module 04]      [Module 03]
      Analytics         Replay


Crawler        Storage     Processing     Analytics / Replay

==================================================
3. THIẾT KẾ DỮ LIỆU
==================================================
posts_raw        : dữ liệu gốc (không thay đổi)
posts_realtime   : dữ liệu realtime (có cập nhật)


==================================================
4. MODULE No.01 — DATA INGESTION (CRAWLER)
==================================================
Chức năng:
- Gọi Reddit API (/new.json)
- Lấy danh sách bài viết
- Lưu vào MongoDB (posts_raw)
- Tránh trùng ID

Quy trình:
1. Request API
2. Parse JSON
3. Format dữ liệu
4. Insert vào DB

Vai trò:
- Data Source Layer


==================================================
5. MODULE No.02 — REAL-TIME PROCESSING
==================================================
Chức năng:
- Xử lý dữ liệu theo chu kỳ 60 giây
- Duy trì tối đa 500 bài viết
- Cập nhật:
  + score
  + num_comments
- Thêm bài mới và xóa bài cũ

Quy trình mỗi chu kỳ:
1. Update dữ liệu cũ
   - Duyệt posts_realtime
   - Gọi API theo id
   - Update score và comments

2. Thêm bài mới
   - Gọi /new.json
   - Insert vào posts_realtime
   - Upsert vào posts_raw

3. Sliding Window
   - Nếu > 500 bài
   - Xóa bài cũ nhất

Vai trò:
- Stream Processing Layer


==================================================
6. MODULE No.03 — REPLAY STREAMING (UPDATED)
==================================================
Chức năng:
- Đọc dữ liệu từ MongoDB
- Phát lại dữ liệu như stream
- Hỗ trợ test và demo

Các mode:
- Fast: phát nhanh nhất
- Timeline: phát theo thời gian
- Multi-thread: phát song song

Input:
- posts_raw hoặc posts_realtime

Output:
- Event stream

Vai trò:
- Simulation Layer (không nằm trong pipeline chính)

==================================================
7. MODULE No.04 — ANALYTICS & PREDICTION
==================================================
Chức năng:
- Phân tích dữ liệu trong 24 giờ gần nhất
- Xác định:
  + Chủ đề hot nhất (Hot Topics)
  + Bài viết trending nhất
- Dự đoán:
  + Xu hướng tiếp theo
  + Bài viết có khả năng viral

Quy trình:
1. Lọc dữ liệu
   - Chỉ lấy bài trong 24h

2. Phân tích chủ đề
   - Group theo subreddit
   - Tổng: score + num_comments

3. Phân tích trending post
   - trending_score = (score + num_comments * 2) / (age_hours + 2)

4. Prediction
   - predict_score = (score + num_comments * 2) / (age_hours + 1)

Output:
- Hot Topics
- Trending Posts
- Predicted Topics
- Predicted Posts

Vai trò:
- Analytics Layer (Consumer)

==================================================
8. LUỒNG DỮ LIỆU (UPDATED)
==================================================
Reddit API
    ↓
Module 01 (Crawler)
    ↓
posts_raw
    ↓
Module 02 (Realtime)
    ↓
posts_realtime
    ↓
        ├── Module 04 (Analytics)
        └── Module 03 (Replay)

==================================================
9. TÍNH CHẤT HỆ THỐNG
==================================================
- Kiểu: Near Real-time
- Cơ chế: Polling
- Database: MongoDB
- Window: 500 bài
- Có hỗ trợ replay


==================================================
10. HẠN CHẾ
==================================================
- Không phải real-time thực sự
- Có thể bị rate limit từ Reddit
- Có độ trễ dữ liệu
- Chưa tối ưu hiệu năng


==================================================
11. HƯỚNG NÂNG CẤP
==================================================
- Async request (aiohttp)
- Bulk write MongoDB
- Kafka (streaming thật)
- WebSocket (realtime frontend)
- Dashboard trực quan


==================================================
12. KẾT LUẬN
==================================================
Hệ thống gồm 3 phần chính:

1. Core Data Pipeline
   - Module 01 (Ingestion)
   - Module 02 (Realtime Processing)
   - MongoDB (Storage)

2. Analytics Layer
   - Module 04 (Trend + Prediction)

3. Supporting Layer
   - Module 03 (Replay / Simulation)

=> Đây là mô hình:
Data Pipeline + Streaming + Analytics System