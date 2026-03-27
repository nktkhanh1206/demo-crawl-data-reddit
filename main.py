import subprocess
import time
import os
import sys

def start_system():
    # 1. Xác định đường dẫn gốc
    # Nếu bạn chạy file này từ ngoài folder 'demo-crawl-data-reddit'
    base_dir = "demo-crawl-data-reddit"
    
    # Kiểm tra xem folder có tồn tại không, nếu không thì dùng thư mục hiện tại
    if not os.path.exists(base_dir):
        base_dir = "."

    modules = [
        {"name": "Module 01 (Cào tin)", "file": "CrawlRedditNo1.py", "type": "python"},
        {"name": "Module 02 (Lọc tin)", "file": "CrawlRedditNo2.py", "type": "python"},
        {"name": "Module 04 (Phân tích)", "file": "Module04.py", "type": "python"},
        {"name": "Dashboard", "file": "dashboard.py", "type": "streamlit"}
    ]

    processes = []

    print("==================================================")
    print("🛠️  HỆ THỐNG REDDIT REALTIME PIPELINE")
    print("==================================================")
    
    for mod in modules:
        target_file = os.path.join(base_dir, mod["file"])
        
        # Kiểm tra file có tồn tại không trước khi chạy
        if not os.path.exists(target_file):
            print(f"❌ Lỗi: Không tìm thấy file {target_file}")
            continue

        print(f"👉 Đang khởi động {mod['name']}...")

        # Tạo câu lệnh dựa trên loại module
        if mod["type"] == "python":
            cmd = f"python {target_file}"
        else:
            cmd = f"streamlit run {target_file}"

        # Dùng 'start' để mở cửa sổ CMD riêng (Windows)
        # /K giúp giữ cửa sổ lại nếu có lỗi để bạn đọc được log
        full_cmd = f"start \"{mod['name']}\" cmd /k {cmd}"
        
        try:
            p = subprocess.Popen(full_cmd, shell=True)
            processes.append(p)
            print(f"✅ {mod['name']} đã lên nhạc.")
        except Exception as e:
            print(f"❌ Không thể khởi động {mod['name']}: {e}")
            
        time.sleep(3) # Delay để tránh nghẽn Database kết nối

    print("\n✅ TẤT CẢ MODULE ĐÃ SẴN SÀNG!")
    print("--------------------------------------------------")
    print("🚩 Dashboard sẽ mở trên trình duyệt sau vài giây.")
    print("🚩 Đừng tắt cửa sổ main.py này khi hệ thống đang chạy.")
    print("--------------------------------------------------")

    try:
        # Giữ main.py chạy để bạn có thể theo dõi trạng thái tổng quát
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng hệ thống... (Hãy tắt thủ công các cửa sổ CMD con)")

if __name__ == "__main__":
    start_system()