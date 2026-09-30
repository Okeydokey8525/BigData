"""Module: spark_etl.py
Description: Pipeline Tiền xử lý Dữ liệu Lớn Phân Tán (Distributed Spark ETL)
trên Apache Spark DataFrame từ file Raw CSV (7.07M dòng) sang Parquet phân vùng.
Hỗ trợ cả chế độ chạy Cục bộ (Local Multi-core) và Cụm 3 Laptop qua Tailscale Mesh VPN.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import time
import json
import shutil
import argparse
import psutil

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Thiết lập Hadoop Winutils cho Spark trên Windows
hadoop_dir = os.path.join(BASE_DIR, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["hadoop.home.dir"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType
)

def get_directory_size(path):
    """Tính tổng dung lượng (bytes) của một file hoặc thư mục."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total_size = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def create_spark_session(app_name="FlightDelay_SparkETL", master=None):
    """Khởi tạo SparkSession tối ưu hóa bộ nhớ và shuffle partitions."""
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
        local_ip = os.environ.get("SPARK_LOCAL_IP")
        if local_ip:
            builder = builder.config("spark.driver.host", local_ip) \
                             .config("spark.driver.bindAddress", local_ip)
    else:
        # Chạy local tận dụng tối đa số luồng CPU của máy
        builder = builder.master("local[*]")

    return builder \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def run_spark_etl(
    input_csv,
    output_parquet_dir,
    metrics_json_path,
    master=None,
    partition_col="month"
):
    """Thực thi toàn bộ quy trình tiền xử lý ETL phân tán trên Apache Spark."""
    print("=" * 70)
    print(" BẮT ĐẦU PIPELINE TIỀN XỬ LÝ PHÂN TÁN (SPARK DISTRIBUTED ETL)")
    print("=" * 70)
    print(f" • Nguồn dữ liệu CSV : {input_csv}")
    print(f" • Đích lưu Parquet : {output_parquet_dir}")
    print(f" • Spark Master     : {master if master else 'local[*]'}")
    print(f" • Cột phân vùng    : {partition_col}")

    raw_size_bytes = os.path.getsize(input_csv) if os.path.isfile(input_csv) else 0
    print(f" • Dung lượng file thô: {raw_size_bytes / (1024**2):.2f} MB")

    process = psutil.Process()
    ram_init = process.memory_info().rss / (1024**2)
    start_total_time = time.perf_counter()

    # 1. Khởi tạo SparkSession
    print("\n[*] Đang khởi động Apache SparkSession...")
    spark = create_spark_session(master=master)
    spark.sparkContext.setLogLevel("WARN")
    print(f"[✓] SparkSession đã sẵn sàng (Spark Version: {spark.version})")

    # 2. Đọc file CSV
    print(f"\n[1/4] Đang nạp phân tán dữ liệu từ CSV...")
    t0_read = time.perf_counter()
    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(input_csv)
    raw_rows_count = raw_df.count()
    t_read = time.perf_counter() - t0_read
    ram_after_read = process.memory_info().rss / (1024**2)
    print(f"      -> Nạp thành công {raw_rows_count:,} dòng trong {t_read:.2f}s | RAM Python: {ram_after_read:.1f} MB")

    # 3. Tiền xử lý & Trích xuất đặc trưng bằng PySpark DataFrame API
    print("\n[2/4] Đang làm sạch, ép kiểu và trích xuất 13 đặc trưng bằng PySpark...")
    t0_prep = time.perf_counter()

    # Ép kiểu dữ liệu sang số cho các cột tính toán
    casted_df = raw_df \
        .withColumn("month", F.col("month").cast(IntegerType())) \
        .withColumn("day_of_month", F.col("day_of_month").cast(IntegerType())) \
        .withColumn("day_of_week", F.col("day_of_week").cast(IntegerType())) \
        .withColumn("crs_dep_time", F.col("crs_dep_time").cast(IntegerType())) \
        .withColumn("crs_arr_time", F.col("crs_arr_time").cast(IntegerType())) \
        .withColumn("cancelled", F.coalesce(F.col("cancelled").cast(IntegerType()), F.lit(0))) \
        .withColumn("diverted", F.coalesce(F.col("diverted").cast(IntegerType()), F.lit(0))) \
        .withColumn("arr_delay", F.col("arr_delay").cast(FloatType())) \
        .withColumn("dep_delay", F.coalesce(F.col("dep_delay").cast(FloatType()), F.lit(0.0))) \
        .withColumn("crs_elapsed_time", F.col("crs_elapsed_time").cast(FloatType())) \
        .withColumn("distance", F.col("distance").cast(FloatType())) \
        .withColumn("carrier_delay", F.coalesce(F.col("carrier_delay").cast(FloatType()), F.lit(0.0))) \
        .withColumn("weather_delay", F.coalesce(F.col("weather_delay").cast(FloatType()), F.lit(0.0))) \
        .withColumn("nas_delay", F.coalesce(F.col("nas_delay").cast(FloatType()), F.lit(0.0))) \
        .withColumn("security_delay", F.coalesce(F.col("security_delay").cast(FloatType()), F.lit(0.0))) \
        .withColumn("late_aircraft_delay", F.coalesce(F.col("late_aircraft_delay").cast(FloatType()), F.lit(0.0)))

    # Lọc bỏ chuyến bay bị hủy, đổi hướng và giá trị khuyết thiếu ở arr_delay
    filtered_df = casted_df.filter(
        (F.col("cancelled") == 0) & 
        (F.col("diverted") == 0) & 
        (F.col("arr_delay").isNotNull())
    )

    # Trích xuất đặc trưng thời gian (Giờ & Số phút trong ngày)
    dep_hour_col = F.floor(F.col("crs_dep_time") / 100).cast(IntegerType())
    arr_hour_col = F.floor(F.col("crs_arr_time") / 100).cast(IntegerType())
    dep_min_col = (F.col("crs_dep_time") % 100).cast(IntegerType())
    arr_min_col = (F.col("crs_arr_time") % 100).cast(IntegerType())

    time_df = filtered_df \
        .withColumn("dep_hour", F.when(dep_hour_col < 0, 0).when(dep_hour_col > 23, 23).otherwise(dep_hour_col)) \
        .withColumn("arr_hour", F.when(arr_hour_col < 0, 0).when(arr_hour_col > 23, 23).otherwise(arr_hour_col)) \
        .withColumn("dep_min_of_day", (dep_hour_col * 60 + dep_min_col).cast(IntegerType())) \
        .withColumn("arr_min_of_day", (arr_hour_col * 60 + arr_min_col).cast(IntegerType())) \
        .withColumn(
            "dep_time_of_day",
            F.when((dep_hour_col >= 5) & (dep_hour_col < 12), "Morning")
             .when((dep_hour_col >= 12) & (dep_hour_col < 17), "Afternoon")
             .when((dep_hour_col >= 17) & (dep_hour_col < 22), "Evening")
             .otherwise("Night")
        )

    # Gán nhãn mục tiêu nhị phân (IsDelayed: trễ >= 15 phút)
    labeled_df = time_df.withColumn(
        "is_delayed",
        F.when(F.col("arr_delay") >= 15, 1).otherwise(0).cast(IntegerType())
    )

    # Gán nhãn nguyên nhân trễ đa lớp (Multi-class Delay Cause)
    max_delay_col = F.greatest(
        F.col("carrier_delay"),
        F.col("weather_delay"),
        F.col("nas_delay"),
        F.col("security_delay"),
        F.col("late_aircraft_delay")
    )

    cause_str_col = F.when(F.col("arr_delay") < 15, "OnTime_or_MinorDelay") \
                     .when(max_delay_col <= 0, "Carrier") \
                     .when(F.col("carrier_delay") == max_delay_col, "Carrier") \
                     .when(F.col("weather_delay") == max_delay_col, "Weather") \
                     .when(F.col("nas_delay") == max_delay_col, "NAS") \
                     .when(F.col("security_delay") == max_delay_col, "Security") \
                     .when(F.col("late_aircraft_delay") == max_delay_col, "LateAircraft") \
                     .otherwise("Carrier")

    cause_code_col = F.when(cause_str_col == "OnTime_or_MinorDelay", 0) \
                      .when(cause_str_col == "Carrier", 1) \
                      .when(cause_str_col == "Weather", 2) \
                      .when(cause_str_col == "NAS", 3) \
                      .when(cause_str_col == "Security", 4) \
                      .when(cause_str_col == "LateAircraft", 5) \
                      .otherwise(0).cast(IntegerType())

    final_clean_df = labeled_df \
        .withColumn("delay_cause", cause_str_col) \
        .withColumn("delay_cause_code", cause_code_col)

    # Cache nhẹ để chuẩn bị ghi và đếm dòng
    final_clean_df.persist()
    clean_rows_count = final_clean_df.count()
    filtered_rows_count = raw_rows_count - clean_rows_count
    t_prep = time.perf_counter() - t0_prep
    ram_after_prep = process.memory_info().rss / (1024**2)
    print(f"      -> Tiền xử lý hoàn tất trong {t_prep:.2f}s | Số dòng sạch: {clean_rows_count:,} dòng (Đã lọc: {filtered_rows_count:,})")

    # 4. Ghi phân tán ra định dạng Parquet nén Snappy
    print(f"\n[3/4] Đang ghi phân tán Parquet phân vùng theo '{partition_col}'...")
    t0_write = time.perf_counter()
    if os.path.exists(output_parquet_dir):
        print(f"      -> Xóa thư mục đích cũ: {output_parquet_dir}")
        shutil.rmtree(output_parquet_dir)

    final_clean_df.write \
        .mode("overwrite") \
        .partitionBy(partition_col) \
        .parquet(output_parquet_dir)

    t_write = time.perf_counter() - t0_write
    t_total = time.perf_counter() - start_total_time
    ram_final = process.memory_info().rss / (1024**2)

    parquet_size_bytes = get_directory_size(output_parquet_dir)
    compression_ratio = raw_size_bytes / parquet_size_bytes if parquet_size_bytes > 0 else 0
    throughput = raw_rows_count / t_total if t_total > 0 else 0

    print(f"      -> Ghi đĩa thành công trong {t_write:.2f}s | Dung lượng Parquet: {parquet_size_bytes / (1024**2):.2f} MB")

    # 5. Lưu báo cáo chỉ số Benchmark
    print("\n[4/4] Đang lưu trữ chỉ số thực nghiệm Spark ETL...")
    metrics_data = {
        "execution_mode": "Spark Distributed ETL",
        "spark_master": master if master else "local[*]",
        "raw_csv_path": input_csv,
        "output_parquet_path": output_parquet_dir,
        "input_csv_size_mb": round(raw_size_bytes / (1024**2), 2),
        "output_parquet_size_mb": round(parquet_size_bytes / (1024**2), 2),
        "compression_ratio": round(compression_ratio, 2),
        "total_raw_rows": raw_rows_count,
        "total_cleaned_rows": clean_rows_count,
        "filtered_rows_cancelled_diverted": filtered_rows_count,
        "timings_seconds": {
            "read_csv": round(t_read, 3),
            "feature_engineering_and_cleaning": round(t_prep, 3),
            "write_parquet": round(t_write, 3),
            "total_elapsed": round(t_total, 3)
        },
        "throughput_rows_per_second": round(throughput, 1),
        "ram_telemetry_mb": {
            "initial_ram": round(ram_init, 1),
            "peak_python_driver_ram": round(max(ram_after_read, ram_after_prep, ram_final), 1)
        }
    }

    os.makedirs(os.path.dirname(metrics_json_path), exist_ok=True)
    with open(metrics_json_path, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=4, ensure_ascii=False)
    print(f"[✓] Đã lưu chỉ số vào: {metrics_json_path}")

    # Hiển thị bảng tổng kết
    print("\n" + "=" * 70)
    print(" TỔNG KẾT KẾT QUẢ TIỀN XỬ LÝ PHÂN TÁN (SPARK DISTRIBUTED ETL)")
    print("=" * 70)
    print(f" • Tổng số dòng thô : {raw_rows_count:,} dòng")
    print(f" • Tổng số dòng sạch: {clean_rows_count:,} dòng (Đã lọc: {filtered_rows_count:,})")
    print(f" • Thời gian Đọc CSV: {t_read:.2f}s")
    print(f" • Thời gian Xử lý  : {t_prep:.2f}s")
    print(f" • Thời gian Ghi PQ : {t_write:.2f}s")
    print(f" • TỔNG THỜI GIAN   : {t_total:.2f}s ({t_total/60:.2f} phút)")
    print(f" • Tốc độ xử lý     : {throughput:,.1f} dòng/giây")
    print(f" • Dung lượng CSV   : {raw_size_bytes / (1024**2):.2f} MB -> Parquet: {parquet_size_bytes / (1024**2):.2f} MB (Tỉ lệ nén {compression_ratio:.1f}x)")
    print("=" * 70 + "\n")

    spark.stop()
    return metrics_data

def main():
    parser = argparse.ArgumentParser(description="Apache Spark Distributed ETL Pipeline")
    default_csv = os.path.join(BASE_DIR, "Flight Delay Dataset — 2024/flight_data_2024.csv")
    fallback_sample = os.path.join(BASE_DIR, "Flight Delay Dataset — 2024/flight_data_2024_sample.csv")
    chosen_csv = default_csv if os.path.exists(default_csv) else fallback_sample

    default_out = os.path.join(BASE_DIR, "Flight Delay Dataset — 2024/cleaned_spark_data.parquet")
    default_metrics = os.path.join(BASE_DIR, "results/spark/metrics/spark_etl_7m.json")

    parser.add_argument("--input", type=str, default=chosen_csv, help="Đường dẫn file CSV đầu vào")
    parser.add_argument("--output", type=str, default=default_out, help="Đường dẫn thư mục Parquet đầu ra")
    parser.add_argument("--metrics", type=str, default=default_metrics, help="Đường dẫn file JSON lưu metrics")
    parser.add_argument("--master", type=str, default=None, help="Spark Master URL (spark://IP:7077 hoặc để trống để chạy local[*])")
    parser.add_argument("--partition-col", type=str, default="month", help="Cột dùng để phân vùng Parquet")
    parser.add_argument("--sample", action="store_true", help="Chạy nhanh trên file sample 10k dòng để kiểm thử")

    args = parser.parse_args()

    input_file = fallback_sample if args.sample else args.input

    run_spark_etl(
        input_csv=input_file,
        output_parquet_dir=args.output,
        metrics_json_path=args.metrics,
        master=args.master,
        partition_col=args.partition_col
    )

if __name__ == "__main__":
    main()
