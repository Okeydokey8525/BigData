# Năm mô hình Spark trong Docker trên ba máy Windows

Docker chứa Python 3.11, Spark 3.5.3, Java 17, XGBoost 2.1.4 và SynapseML
1.0.15. Không dùng Python/Spark cài trên Windows. CatBoost đã bỏ.

Mỗi máy cần Docker Desktop đang chạy ở chế độ Linux containers. Chép cùng
phiên bản dự án sang cả ba máy. Chỉ máy Master cần dữ liệu Parquet đầu vào.
Mỗi Worker được cấp 2 CPU và 4 GB RAM; máy Master cần RAM thêm cho Driver/HDFS.

Container dùng Tailscale riêng để các tiến trình Spark quảng bá đúng địa chỉ
Linux có thể truy cập từ máy khác, không cần bật host networking Docker Desktop.
Đăng nhập cùng mạng Tailscale trên cả ba container. Chỉ dùng mạng riêng này.
Tailscale có thể yêu cầu duyệt thiết bị theo chính sách tài khoản.

## Chuẩn bị trên từng máy

Mở PowerShell tại thư mục dự án:

```powershell
cd docker/spark
Copy-Item .env.example .env
```

Đổi NODE_NAME trong `.env` thành `flight-master`, `flight-worker2` hoặc
`flight-worker3` tương ứng. Khởi động mạng và đăng nhập bằng liên kết được in:

```powershell
docker compose up -d network
docker compose exec network tailscale up
docker compose exec network tailscale ip -4
docker compose build worker
```

Lấy IP container trên máy Master, điền vào `MASTER_IP` trong `.env` trên cả ba
máy. Đây là IP container mới, không mặc định dùng IP Windows cũ.

## Bật cụm

Trên máy Master (đồng thời là Worker 1):

```powershell
docker compose --profile master up -d master worker namenode datanode
```

Trên hai máy Worker:

```powershell
docker compose up -d worker
```

Xem `http://<MASTER_IP>:8080`, phải có ba Worker ALIVE. Dừng cụm Spark cũ để
tránh nhầm giữa hai cụm. Không cần gỡ Python hay Spark trên Windows.

## Kiểm tra và huấn luyện trên Master

```powershell
docker compose --profile driver run --rm driver check
docker compose --profile driver run --rm driver train
```

Lệnh train đọc `/workspace/data/cleaned_flight_data_2024.parquet`, không chạy
lại ETL. Nếu dữ liệu Windows là junction trỏ ra ngoài dự án, cần đặt dữ liệu
thật tại thư mục được mount hoặc mount thêm rồi truyền `--parquet`.

Kết quả JSON lưu vào `results/spark/metrics` của dự án trên máy Master.
Các mô hình được lưu HDFS dùng chung tại `hdfs://<MASTER_IP>:9000/flight/models`;
không xóa volume namenode/datanode nếu còn cần mô hình. Đợi HDFS có DataNode
hoạt động trước khi train (UI `http://<MASTER_IP>:9870`).

Phải kiểm tra cả năm trạng thái success; lỗi thư viện native hay mạng vẫn
được ghi thật, không thay bằng số liệu giả. Việc đóng gói không phải bằng
chứng rằng huấn luyện đã thành công trên ba máy thực tế.

Nguồn: https://docs.docker.com/desktop/features/wsl/
https://tailscale.com/kb/1282/docker
