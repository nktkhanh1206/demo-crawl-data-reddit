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
2. KIẾN TRÚC TỔNG QUAN
==================================================
[Module 01] → [MongoDB] → [Module 02] → [Module 03]

Crawler        Storage       Realtime      Replay


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
6. MODULE No.03 — REPLAY STREAMING
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
- Serving / Simulation Layer


==================================================
7. LUỒNG DỮ LIỆU
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
Module 03 (Replay)


==================================================
8. TÍNH CHẤT HỆ THỐNG
==================================================
- Kiểu: Near Real-time
- Cơ chế: Polling
- Database: MongoDB
- Window: 500 bài
- Có hỗ trợ replay


==================================================
9. HẠN CHẾ
==================================================
- Không phải real-time thực sự
- Có thể bị rate limit từ Reddit
- Có độ trễ dữ liệu
- Chưa tối ưu hiệu năng


==================================================
10. HƯỚNG NÂNG CẤP
==================================================
- Async request (aiohttp)
- Bulk write MongoDB
- Kafka (streaming thật)
- WebSocket (realtime frontend)
- Dashboard trực quan


==================================================
11. KẾT LUẬN
==================================================
Hệ thống gồm 3 tầng:

1. Ingestion Layer   → Module 01
2. Processing Layer  → Module 02
3. Serving Layer     → Module 03

=> Đây là mô hình Data Pipeline + Streaming System cơ bản