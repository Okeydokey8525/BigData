# HƯỚNG DẪN THIẾT LẬP MÔI TRƯỜNG (ENVIRONMENT SETUP GUIDE)

> Tài liệu này hướng dẫn cách cấu hình môi trường tính toán để chạy Apache Spark (PySpark), Hadoop và các dịch vụ liên quan cho Đồ án.

---

## 1. YÊU CẦU TIỀN ĐỀ (PREREQUISITES)

| Thành phần | Phiên bản khuyến nghị | Mục đích |
| :--- | :--- | :--- |
| **Hệ điều hành** | Windows 10/11 64-bit | Môi trường máy trạm hiện tại |
| **Python** | 3.10 hoặc 3.11 64-bit | Chạy PySpark, FastAPI, phân tích dữ liệu |
| **Java JDK** | OpenJDK 11 hoặc 17 (LTS) | Yêu cầu bắt buộc của Spark và Hadoop |
| **Docker Desktop** | Bản mới nhất (hỗ trợ WSL2) | Dùng để chạy cụm phân tán đa container |

---

## 2. PHƯƠNG ÁN 1: CHẠY PYSPARK CỤC BỘ TRÊN WINDOWS (LOCAL MODE)

Cách này phù hợp để dev nhanh, chạy script tiền xử lý và test thuật toán mà không cần bật cụm container nặng.

### Bước 2.1: Cài đặt Java JDK
1. Tải và cài đặt **Eclipse Temurin OpenJDK 11** hoặc **17**: [Adoptium Downloads](https://adoptium.net/).
2. Thiết lập biến môi trường trên Windows:
   * Biến hệ thống `JAVA_HOME`: `C:\Program Files\Eclipse Adoptium\jdk-17.x.x` (đường dẫn thực tế).
   * Thêm `%JAVA_HOME%\bin` vào biến `Path`.
3. Kiểm tra trong CMD/PowerShell:
   ```powershell
   java -version
   ```

### Bước 2.2: Cấu hình `HADOOP_HOME` và `winutils.exe` cho Windows
Khi chạy Spark trên Windows, Spark cần thư viện nhị phân `winutils.exe` và `hadoop.dll`:
1. Tạo thư mục: `C:\hadoop\bin`.
2. Tải `winutils.exe` và `hadoop.dll` tương ứng với phiên bản Hadoop (ví dụ bản Hadoop 3.3.x) từ repo uy tín [cdarlint/winutils](https://github.com/cdarlint/winutils) bỏ vào `C:\hadoop\bin`.
3. Thiết lập biến môi trường:
   * Biến hệ thống `HADOOP_HOME`: `C:\hadoop`.
   * Thêm `%HADOOP_HOME%\bin` vào `Path`.

### Bước 2.3: Tạo môi trường ảo Python & Cài đặt thư viện
Tại thư mục gốc đồ án:
```powershell
# Tạo môi trường ảo
python -m venv venv

# Kích hoạt môi trường ảo
.\venv\Scripts\Activate.ps1

# Cài đặt các gói cần thiết
pip install pyspark==3.5.1 pyarrow pandas numpy matplotlib seaborn fastapi uvicorn pymongo
```

---

## 3. PHƯƠNG ÁN 2: CHẠY CỤM DOCKER COMPOSE (DISTRIBUTED SIMULATION)

Dành cho kịch bản triển khai cụm phân tán hoàn chỉnh theo đề cương (NameNode, DataNode, Spark Master, Spark Worker, MongoDB):

```yaml
# Ví dụ cấu hình docker-compose.yml dự kiến
version: '3.8'

services:
  # Hadoop NameNode
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - "9870:9870"
    environment:
      - CLUSTER_NAME=flight_cluster
    volumes:
      - hadoop_namenode:/hadoop/dfs/name

  # Hadoop DataNode
  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    restart: always
    ports:
      - "9864:9864"
    volumes:
      - hadoop_datanode:/hadoop/dfs/data

  # Spark Master
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
      - "7077:7077"

  # Spark Worker
  spark-worker:
    image: bitnami/spark:3.5
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2G
    depends_on:
      - spark-master

  # MongoDB Serving
  mongodb:
    image: mongo:6.0
    container_name: mongodb
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

volumes:
  hadoop_namenode:
  hadoop_datanode:
  mongo_data:
```

---

## 4. KHẮC PHỤC CÁC LỖI THƯỜNG GẶP TRÊN WINDOWS

1. **Lỗi `java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows...`:**
   * **Nguyên nhân:** Thiếu `hadoop.dll` trong `C:\Windows\System32` hoặc `C:\hadoop\bin`.
   * **Khắc phục:** Copy file `hadoop.dll` từ `C:\hadoop\bin` thả trực tiếp vào `C:\Windows\System32`.
2. **Lỗi `Py4JJavaError: An error occurred while calling z:org.apache.spark.api.python.PythonUtils.getEncryptionEnabled`:**
   * **Nguyên nhân:** Java JDK không tương thích (dùng Java 19+ hoặc 21 gây xung đột với Spark 3.x).
   * **Khắc phục:** Gỡ hoặc đổi `JAVA_HOME` về JDK 11 hoặc JDK 17 LTS.
3. **Lỗi OutOfMemory (Java Heap Space):**
   * **Khắc phục:** Tăng kích thước bộ nhớ Driver khi khởi tạo `SparkSession`:
     ```python
     spark = SparkSession.builder \
         .appName("FlightDelay") \
         .config("spark.driver.memory", "4g") \
         .config("spark.executor.memory", "4g") \
         .getOrCreate()
     ```
