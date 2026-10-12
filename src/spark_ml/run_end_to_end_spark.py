"""Train and evaluate five PySpark models from preprocessed Parquet.

Default: reuse cleaned Parquet, with requests for three executors.
Use --run-etl explicitly to preprocess CSV before training.
Only measured training/evaluation results are recorded.
"""

import os
import sys
import io
import time
import json
import psutil
import gc
import argparse
import importlib
import traceback
import hashlib
from datetime import datetime
from pathlib import Path

# Thiết lập UTF-8 cho Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).resolve().parent.parent.parent
hadoop_dir = BASE_DIR / "hadoop"
if os.name == "nt" and hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    os.environ["PATH"] = str(hadoop_dir / "bin") + os.pathsep + os.environ.get("PATH", "")

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Third-party wrappers are imported by require_training_dependencies after session setup.
# Cấu hình các đường dẫn mặc định an toàn chuẩn ASCII
RAW_CSV_PATH = BASE_DIR / "data" / "flight_data_2024.csv"
CLEAN_PARQUET_DIR = BASE_DIR / "data" / "cleaned_flight_data_2024.parquet"
MODELS_DIR = BASE_DIR / "models" / "spark"
METRICS_DIR = BASE_DIR / "results" / "spark" / "metrics"

REQUIRED_COLUMNS = [
    'month', 'day_of_month', 'day_of_week',
    'op_unique_carrier', 'origin', 'dest',
    'crs_dep_time', 'crs_arr_time',
    'cancelled', 'diverted', 'arr_delay', 'dep_delay',
    'crs_elapsed_time', 'distance',
    'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay'
]

TARGET_NAMES = ['OnTime', 'Carrier', 'Weather', 'NAS', 'Security', 'LateAircraft']


def macro_f1_from_counts(matrix):
    scores = []
    for i in range(len(matrix)):
        denominator = sum(matrix[i]) + sum(row[i] for row in matrix)
        scores.append(2 * matrix[i][i] / denominator if denominator else 0.0)
    return sum(scores) / len(scores) if scores else 0.0


def require_training_dependencies(spark):
    """Resolve Python wrappers after Spark adds configured py-files."""
    global SparkXGBClassifier, SynapseLightGBM
    errors = []
    for module, attribute, binding in [
        ("xgboost.spark", "SparkXGBClassifier", "SparkXGBClassifier"),
        ("synapse.ml.lightgbm", "LightGBMClassifier", "SynapseLightGBM"),
    ]:
        try:
            imported = importlib.import_module(module)
            globals()[binding] = getattr(imported, attribute) if attribute else imported
        except Exception as exc:
            errors.append(f"{module}: {type(exc).__name__}: {exc}")
    if errors:
        raise RuntimeError("Cannot train all five Spark models:\n" + "\n".join(errors))
    # Validate JVM wrappers before processing the full dataset.
    SynapseLightGBM()


def get_process_ram_mb():
    try:
        p = psutil.Process()
        return p.memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


def synapseml_coordinate(spark_version):
    """Match the Scala ABI; Spark 4.2 uses the latest 4.1 build with a warning."""
    runtime = tuple(int(part) for part in spark_version.split('.')[:2])
    if runtime == (3, 5):
        return "com.microsoft.azure:synapseml-lightgbm_2.12:1.0.15"
    if runtime in [(4, 0), (4, 1), (4, 2)]:
        branch = "4.0" if runtime == (4, 0) else "4.1"
        return f"com.microsoft.azure:synapseml-lightgbm_2.13:1.1.3-spark{branch}"
    raise RuntimeError(f"No configured SynapseML build for Spark {spark_version}")


def create_spark_session(master_url: str, app_name: str = "FlightDelay_PureSpark_EndToEnd"):
    import pyspark
    packages = os.environ.get("SPARK_EXTRA_PACKAGES") or synapseml_coordinate(pyspark.__version__)
    if int(pyspark.__version__.split('.')[0]) >= 4 and 'synapseml_2.12:' in packages:
        raise RuntimeError(
            f"PySpark {pyspark.__version__} is incompatible with SynapseML Scala 2.12. "
            "Remove the old SPARK_EXTRA_PACKAGES override so the launcher can select a Scala 2.13 build."
        )
    if pyspark.__version__.startswith('4.2.'):
        print('[!] Spark 4.2: using SynapseML 4.1 / Scala 2.13 compatibility build; '
              'upstream does not list Spark 4.2 support. Training errors remain failures.')
    print(f"[*] Đang khởi tạo Apache SparkSession kết nối tới: {master_url}...")
    is_cluster = "spark://" in master_url
    # A driver absolute path is not portable to other Windows machines.
    # Worker launcher places that machine's venv first on PATH.
    if os.name == 'nt':
        os.environ.setdefault('PYSPARK_PYTHON', 'python' if is_cluster else sys.executable)

    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    if is_cluster and driver_host:
        os.environ["SPARK_LOCAL_IP"] = driver_host

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.jars.packages", packages)
        .config("spark.jars.repositories", "https://mmlspark.blob.core.windows.net/maven,https://repo.maven.apache.org/maven2")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_PARTITIONS", "12"))
        .config("spark.default.parallelism", os.environ.get("SPARK_PARTITIONS", "12"))
        .config("spark.executor.instances", os.environ.get("SPARK_EXECUTOR_INSTANCES", "3"))
        .config("spark.cores.max", str(int(os.environ.get("SPARK_EXECUTOR_INSTANCES", "3")) * int(os.environ.get("SPARK_EXECUTOR_CORES", "2"))))
        .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))
        .config("spark.task.cpus", os.environ.get("SPARK_TASK_CPUS", os.environ.get("SPARK_EXECUTOR_CORES", "2")))
        .config("spark.hadoop.dfs.client.use.datanode.hostname", os.environ.get("SPARK_HDFS_DATANODE_HOSTNAME", "false"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.executorEnv.PYTHONFAULTHANDLER", "1")
        .config("spark.network.timeout", "1200s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.rpc.message.maxSize", "512")
        .config("spark.shuffle.io.maxRetries", "10")
        .config("spark.shuffle.io.retryWait", "15s")
        .config("spark.core.connection.ack.wait.timeout", "600s")
    )

    if is_cluster and driver_host:
        builder = (builder.config("spark.driver.host", driver_host)
                   .config("spark.driver.bindAddress", driver_host))

    for env, config in [("SPARK_EXTRA_PACKAGES", "spark.jars.packages"),
                        ("SPARK_EXTRA_REPOSITORIES", "spark.jars.repositories"),
                        ("SPARK_EXTRA_PY_FILES", "spark.submit.pyFiles"),
                        ("SPARK_IVY_SETTINGS", "spark.jars.ivySettings")]:
        if os.environ.get(env):
            builder = builder.config(config, os.environ[env])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[✓] SparkSession đã sẵn sàng (Spark Version: {spark.version})")
    return spark


# ==============================================================================
# HÀM ĐO ĐẠC HAI NHÁNH THỜI GIAN (PURE SPARK VS WALL-CLOCK WITH WIFI/VPN)
# ==============================================================================
class SparkDualTimingBenchmark:
    """Measure elapsed wall-clock without inventing CPU/network durations."""
    def __init__(self, spark):
        self.spark = spark
        self.sc = spark.sparkContext

    def measure(self, func, *args, **kwargs):
        # Wall-clock is measured directly. CPU-only and network-only durations
        # require executor/event-log instrumentation and cannot be inferred here.
        started = time.perf_counter()
        result = func(*args, **kwargs)
        return result, {
            "pure_spark_time_s": None,
            "wall_clock_time_s": round(time.perf_counter() - started, 6),
            "network_wifi_overhead_s": None,
            "overhead_percentage": None,
            "timing_method": "perf_counter_wall_clock",
        }


# ==============================================================================
# GIAI ĐOẠN 1: THUẦN PYSPARK DISTRIBUTED ETL (TIỀN XỬ LÝ 7.08 TRIỆU DÒNG)
# ==============================================================================
def clean_spark_dataframe(sdf):
    """Thực thi toàn bộ logic làm sạch và feature engineering thuần PySpark."""
    casted_df = sdf \
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

    # Lọc bỏ chuyến hủy, chuyển hướng và bản ghi khuyết thiếu arr_delay
    filtered_df = casted_df.filter(
        (F.col("cancelled") == 0) & 
        (F.col("diverted") == 0) & 
        (F.col("arr_delay").isNotNull())
    )

    # Trích xuất đặc trưng thời gian
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

    # Gán nhãn mục tiêu nhị phân (IsDelayed >= 15 phút)
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

    return labeled_df \
        .withColumn("delay_cause", cause_str_col) \
        .withColumn("delay_cause_code", cause_code_col)


def execute_pure_spark_etl(spark, input_csv: Path, output_parquet: Path, master_url: str = ""):
    print("\n" + "=" * 80)
    print(" [GIAI ĐOẠN 1/3] BẮT ĐẦU SPARK DISTRIBUTED ETL TRỌN VẸN TỪ CSV THÔ")
    print("=" * 80)
    print(f" • Tệp CSV thô       : {input_csv}")
    print(f" • Thư mục Parquet   : {output_parquet}")
    print(f" • Cụm thực thi      : {master_url if master_url else 'local[*]'}")

    # Xóa dữ liệu Parquet cũ để ghi đè sạch sẽ
    if output_parquet.exists():
        import shutil
        if output_parquet.is_dir():
            shutil.rmtree(output_parquet)
        else:
            output_parquet.unlink()

    benchmarker = SparkDualTimingBenchmark(spark)
    is_cluster = "spark://" in master_url

    raw_row_count = 0

    def _etl_operation():
        nonlocal raw_row_count
        if is_cluster:
            # Driver streams CSV chunks and sends configurable partitions to the active executors.
            import pyarrow as pa
            import pyarrow.dataset as ds
            print("[*] [Cluster Mode] Driver đang nạp các khối CSV và phân phối tới các executor đang hoạt động...")
            csv_reader = pd.read_csv(str(input_csv), usecols=REQUIRED_COLUMNS, chunksize=1_000_000, low_memory=False)
            for chunk_idx, chunk in enumerate(csv_reader, 1):
                raw_row_count += len(chunk)
                t_c = time.perf_counter()
                sdf = spark.createDataFrame(chunk).repartition(int(spark.conf.get("spark.sql.shuffle.partitions")))
                clean_sdf = clean_spark_dataframe(sdf)
                clean_pdf = clean_sdf.toPandas()
                table = pa.Table.from_pandas(clean_pdf, preserve_index=False)
                ds.write_dataset(
                    data=table,
                    base_dir=str(output_parquet),
                    format="parquet",
                    partitioning=["month"],
                    partitioning_flavor="hive",
                    basename_template=f"spark_chunk_{chunk_idx}_{{i}}.parquet",
                    existing_data_behavior="overwrite_or_ignore"
                )
                elapsed_c = time.perf_counter() - t_c
                print(f"  [+] Chunk {chunk_idx:>2}: Đọc {len(chunk):,} dòng -> các executor xử lý và ghi Parquet trong {elapsed_c:.2f}s")
                del chunk, sdf, clean_sdf, clean_pdf, table
                gc.collect()
        else:
            # Chế độ máy đơn (Local Mode)
            print("[*] Đang đọc phân tán dữ liệu thô từ CSV bằng Apache Spark Engine...")
            raw_sdf = spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .csv(str(input_csv)) \
                .select(REQUIRED_COLUMNS)
            raw_row_count = raw_sdf.count()
            clean_sdf = clean_spark_dataframe(raw_sdf)
            clean_sdf.write \
                .mode("overwrite") \
                .partitionBy("month") \
                .parquet(str(output_parquet))

        return True

    _, timing_info = benchmarker.measure(_etl_operation)

    # Đếm số lượng dòng sạch sau khi đã ghi Parquet
    import pyarrow.parquet as pq
    pq_table = pq.read_table(str(output_parquet), columns=['month'])
    total_cleaned_rows = len(pq_table)
    del pq_table
    total_raw_rows = raw_row_count
    filtered_rows = total_raw_rows - total_cleaned_rows
    pq_size_mb = sum(f.stat().st_size for f in output_parquet.glob("**/*") if f.is_file()) / (1024 * 1024)
    peak_ram = get_process_ram_mb()

    print(f"\n[✓] Hoàn tất Thuần PySpark ETL:")
    print(f"    • Wall-clock thực đo: {timing_info['wall_clock_time_s']} giây")
    print(f"    • Tổng số dòng sạch thu được          : {total_cleaned_rows:,} dòng (Đã lọc: {filtered_rows:,})")
    print(f"    • Dung lượng Parquet nén              : {pq_size_mb:.2f} MB")

    return {
        "pure_spark_time_seconds": timing_info['pure_spark_time_s'],
        "wall_clock_time_seconds": timing_info['wall_clock_time_s'],
        "network_wifi_overhead_seconds": timing_info['network_wifi_overhead_s'],
        "overhead_percentage": timing_info['overhead_percentage'],
        "total_raw_rows": total_raw_rows,
        "total_cleaned_rows": total_cleaned_rows,
        "filtered_rows": filtered_rows,
        "parquet_size_mb": round(pq_size_mb, 2),
        "driver_python_rss_after_etl_mb": round(peak_ram, 1)
    }


# ==============================================================================
# GIAI ĐOẠN 2 & 3: THUẦN PYSPARK FEATURE PIPELINE & 5 MÔ HÌNH HỌC MÁY PHÂN TÁN
# ==============================================================================
def execute_pure_spark_mllib(spark, parquet_path: Path, master_url: str = "", num_trees: int = 20, max_depth: int = 10):
    print("\n" + "=" * 80)
    print(" [HUẤN LUYỆN 1/2] NẠP PARQUET & XÂY DỰNG ĐẶC TRƯNG")
    print("=" * 80)

    num_cols = [
        'month', 'day_of_month', 'day_of_week',
        'dep_hour', 'arr_hour', 'dep_min_of_day', 'arr_min_of_day',
        'crs_elapsed_time', 'distance'
    ]
    cat_cols = ['op_unique_carrier', 'origin', 'dest', 'dep_time_of_day']
    target_col = 'delay_cause_code'

    print(f"[*] Đang nạp và phân phối dữ liệu phân tán vào Spark RDD: {parquet_path}...")
    import pyarrow.parquet as pq
    all_needed = num_cols + cat_cols + [target_col]
    table = pq.read_table(str(parquet_path), columns=all_needed).combine_chunks()
    pdf = table.to_pandas()
    del table

    # Distribute partitions to the executors registered with Spark.
    df = spark.createDataFrame(pdf).repartition(int(spark.conf.get("spark.sql.shuffle.partitions")))
    del pdf
    gc.collect()

    # Mã hóa các biến phân loại bằng StringIndexer thuần PySpark
    print("[*] Đang áp dụng StringIndexer thuần PySpark cho 4 biến phân loại...")
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_code", handleInvalid="keep")
        for c in cat_cols
    ]
    indexing_pipeline = Pipeline(stages=indexers)
    indexed_df = indexing_pipeline.fit(df).transform(df)

    # Reduce high-cardinality histogram traffic between executors.
    # Hai cột 'origin' và 'dest' có 349 mã sân bay (high-cardinality).
    # Ép kiểu về DoubleType() để xóa metadata categorical -> Spark xử lý dạng numeric continuous.
    # Nhờ vậy DecisionTree & RandomForest chỉ cần maxBins=32 chuẩn, giảm 95% lượng shuffle data qua WiFi,
    # Runtime and network reliability are measured, not guaranteed by this conversion.
    indexed_df = indexed_df \
        .withColumn("origin_code", F.col("origin_code").cast(DoubleType())) \
        .withColumn("dest_code", F.col("dest_code").cast(DoubleType()))

    feature_cols = num_cols + [f"{c}_code" for c in cat_cols]

    print("[*] Đang đóng gói 13 đặc trưng vào VectorAssembler thuần PySpark...")
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    transformed_df = assembler.transform(indexed_df)

    # Phân chia phân tán 70% Train / 15% Validation / 15% Test
    print("[*] Đang phân chia tập dữ liệu phân tán trên Spark: Train 70% / Val 15% / Test 15%...")
    train_df, val_df, test_df = transformed_df.randomSplit([0.7, 0.15, 0.15], seed=42)
    train_df.cache()
    val_df.cache()
    test_df.cache()

    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()
    print(f"[✓] Dữ liệu đã sẵn sàng: Train {train_count:,} dòng | Val {val_count:,} dòng | Test {test_count:,} dòng")

    # ==============================================================================
    # KHỞI TẠO BỘ 5 MÔ HÌNH HỌC MÁY PHÂN TÁN
    # ==============================================================================
    print("\n" + "=" * 80)
    print(" [HUẤN LUYỆN 2/2] HUẤN LUYỆN VÀ ĐÁNH GIÁ LẦN LƯỢT 5 MÔ HÌNH")
    print("=" * 80)
    print(" Danh sách 5 mô hình học máy phân tán trên Spark:")
    print("   1. Logistic Regression (Spark MLlib Native - Multinomial L-BFGS)")
    print("   2. Decision Tree       (Spark MLlib Native - Cây quyết định phân tán)")
    print("   3. Random Forest       (Spark MLlib Native - Rừng ngẫu nhiên phân tán - Trọng tâm)")
    print("   4. XGBoost on Spark    (Third-party - SparkXGBClassifier)")
    print("   5. LightGBM on Spark   (Third-party - Microsoft SynapseML / Distributed Engine)")

    acc_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="accuracy")
    f1_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="weightedFMeasure")
    prec_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="weightedPrecision")
    rec_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="weightedRecall")

    models_config = [
        # --- 3 Mô hình Chính chủ Spark MLlib ---
        {
            "id": "logistic_regression",
            "name": "Logistic Regression (Spark MLlib)",
            "type": "native_spark",
            "estimator": LogisticRegression(
                featuresCol="features",
                labelCol=target_col,
                maxIter=20,
                regParam=0.1,
                elasticNetParam=0.0,
                family="multinomial"
            ),
            "save_dir": MODELS_DIR / "logistic_regression_model"
        },
        {
            "id": "decision_tree",
            "name": "Decision Tree (Spark MLlib)",
            "type": "native_spark",
            "estimator": DecisionTreeClassifier(
                featuresCol="features",
                labelCol=target_col,
                maxDepth=6,
                maxBins=32,
                seed=42
            ),
            "save_dir": MODELS_DIR / "decision_tree_model"
        },
        {
            "id": "random_forest",
            "name": "Random Forest (Spark MLlib)",
            "type": "native_spark",
            "estimator": RandomForestClassifier(
                featuresCol="features",
                labelCol=target_col,
                numTrees=num_trees,
                maxDepth=max_depth,
                maxBins=32,
                seed=42
            ),
            "save_dir": MODELS_DIR / "random_forest_model"
        }
    ]

    # Construct third-party estimators inside the per-model error boundary.
    models_config.extend([
        {
            "id": "xgboost_spark", "name": "XGBoost (Spark Distributed)",
            "type": "third_party_spark",
            "factory": lambda: SparkXGBClassifier(
                features_col="features", label_col=target_col,
                n_estimators=30, max_depth=6, learning_rate=0.08,
                num_workers=int(os.environ.get("SPARK_XGB_WORKERS", "3")),
                random_state=42),
        },
        {
            "id": "lightgbm_spark", "name": "LightGBM (SynapseML / Spark)",
            "type": "third_party_spark",
            "factory": lambda: SynapseLightGBM(
                featuresCol="features", labelCol=target_col,
                objective="multiclass",
                numLeaves=31, numIterations=50, learningRate=0.08,
                numTasks=int(os.environ.get("SPARK_EXECUTOR_INSTANCES", "3")),
                numThreads=int(os.environ.get("SPARK_EXECUTOR_CORES", "2"))),
        },
    ])
    all_models_results = {}
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    benchmarker = SparkDualTimingBenchmark(spark)
    for item in models_config:
        m_id, m_name = item["id"], item["name"]
        started = time.perf_counter()
        predictions = None
        phase = "initialization"
        res = {
            "name": m_name, "type": item["type"], "status": "failed",
            "train_rows": train_count, "validation_rows": val_count,
            "test_rows": test_count,
            "pure_spark_time_seconds": None,
            "network_wifi_overhead_seconds": None,
            "overhead_percentage": None,
        }
        print(f"[*] Huấn luyện thật: {m_name}")
        try:
            estimator = item["factory"]() if "factory" in item else item["estimator"]
            res["estimator_params"] = {
                param.name: str(value) for param, value in estimator.extractParamMap().items()
            }
            ram_before = get_process_ram_mb()
            phase = "fit"
            fitted_model, timing = benchmarker.measure(lambda: estimator.fit(train_df))
            res["wall_clock_time_seconds"] = timing["wall_clock_time_s"]
            phase = "evaluation"
            pred_started = time.perf_counter()
            predictions = fitted_model.transform(test_df).cache()
            pred_count = predictions.count()
            if pred_count == 0:
                raise ValueError("Empty test set; cannot evaluate model")
            res["latency_ms_per_1k"] = round(
                (time.perf_counter() - pred_started) * 1_000_000 / pred_count, 6)
            res["prediction_rows"] = pred_count
            res["accuracy"] = float(acc_evaluator.evaluate(predictions))
            res["f1_score_weighted"] = float(f1_evaluator.evaluate(predictions))
            res["precision_weighted"] = float(prec_evaluator.evaluate(predictions))
            res["recall_weighted"] = float(rec_evaluator.evaluate(predictions))
            counts = predictions.groupBy(target_col, "prediction").count().collect()
            matrix = [[0 for _ in TARGET_NAMES] for _ in TARGET_NAMES]
            for row in counts:
                matrix[int(row[target_col])][int(row["prediction"])] += int(row["count"])
            res["confusion_matrix"] = matrix
            res["target_names"] = TARGET_NAMES
            res["f1_score_macro"] = macro_f1_from_counts(matrix)
            res["driver_python_rss_before_fit_mb"] = round(ram_before, 2)
            res["driver_python_rss_after_evaluation_mb"] = round(get_process_ram_mb(), 2)
            if hasattr(fitted_model, "featureImportances"):
                res["top_features"] = sorted(
                    zip(feature_cols, fitted_model.featureImportances.toArray().tolist()),
                    key=lambda value: value[1], reverse=True)
            phase = "save_model"
            model_base_uri = os.environ.get("SPARK_MODEL_BASE_URI")
            save_path = (model_base_uri.rstrip("/") + f"/{m_id}_model"
                         if model_base_uri else MODELS_DIR / f"{m_id}_model")
            fitted_model.write().overwrite().save(str(save_path))
            res["model_path"] = str(save_path)
            res["status"] = "success"
        except Exception as exc:
            res["failed_phase"] = phase
            res["error"] = f"{type(exc).__name__}: {exc}"
            res["traceback"] = traceback.format_exc()
            print(f"[!] {m_name}: {res['error']}")
        finally:
            if predictions is not None:
                try:
                    predictions.unpersist()
                except Exception:
                    pass
            res["attempt_wall_clock_seconds"] = round(time.perf_counter() - started, 6)
            all_models_results[m_id] = res

    return all_models_results


# ==============================================================================
# HÀM ĐIỀU PHỐI CHÍNH TOÀN TRÌNH
# ==============================================================================
def run_end_to_end_pipeline(master_url: str, num_trees: int, max_depth: int,
                            force_etl: bool = False, skip_etl: bool = True,
                            parquet_path: Path = CLEAN_PARQUET_DIR):
    parquet_path = Path(parquet_path).expanduser().resolve()
    skip_etl = skip_etl and not force_etl
    if skip_etl and not parquet_path.exists():
        raise FileNotFoundError(f"Cleaned Parquet not found: {parquet_path}. Supply --parquet or explicitly use --run-etl.")
    start_total_wall = time.perf_counter()

    print("=" * 88)
    print(" PYSPARK: HUẤN LUYỆN 5 MÔ HÌNH TỪ PARQUET ĐÃ TIỀN XỬ LÝ")
    print("=" * 88)
    print(f" • Cụm tính toán     : {master_url}")
    print(f" • Dữ liệu sạch đầu vào: {parquet_path}")
    print(f" • Chạy lại ETL: {'không (mặc định)' if skip_etl else 'có (--run-etl)'}")
    print(f" • Cấu hình RF       : {num_trees} Trees, MaxDepth {max_depth}")
    print(" • Đo thời gian: wall-clock thực đo; không suy diễn thời gian CPU/mạng")
    print("=" * 88)

    spark = create_spark_session(master_url)
    try:
        require_training_dependencies(spark)
    except Exception:
        spark.stop()
        raise

    try:
        # GIAI ĐOẠN 1: SPARK DISTRIBUTED ETL TOÀN TRÌNH
        if skip_etl:
            print("\n" + "=" * 80)
            print(" [ĐẦU VÀO] SỬ DỤNG PARQUET ĐÃ TIỀN XỬ LÝ; KHÔNG CHẠY ETL")
            print("=" * 80)
            etl_results = {"status": "skipped", "wall_clock_time_seconds": None,
                           "reason": "Reused preprocessed Parquet; previous ETL timing is not included"}

        else:
            # LUÔN CHẠY TOÀN TRÌNH TỪ CSV THÔ (KHÔNG NHẢY CÓC)
            etl_results = execute_pure_spark_etl(
                spark=spark,
                input_csv=RAW_CSV_PATH,
                output_parquet=parquet_path,
                master_url=master_url
            )

        # GIAI ĐOẠN 2 & 3: HUẤN LUYỆN 5 MÔ HÌNH TRÊN SPARK
        ml_results = execute_pure_spark_mllib(
            spark=spark,
            parquet_path=parquet_path,
            master_url=master_url,
            num_trees=num_trees,
            max_depth=max_depth
        )

        total_pipeline_wall = time.perf_counter() - start_total_wall

        # TỔNG HỢP VÀ LƯU CHỈ SỐ METRICS
        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        summary_metrics = {
            "pipeline_name": "PySpark Pipeline (5 Models, measured results only)",
            "model_count": len(ml_results),
            "synapseml_packages": spark.sparkContext.getConf().get("spark.jars.packages", ""),
            "schema_version": 2,
            "status": "success" if all(v["status"] == "success" for v in ml_results.values()) else "failed",
            "spark_version": spark.version,
            "etl_included": not skip_etl,
            "input_parquet": str(parquet_path),
            "run_mode": "training_only" if skip_etl else "etl_and_training",
            "total_time_scope": "Spark setup, dependency checks, Parquet loading, feature pipeline, training, evaluation and model saving" + ("; ETL included" if not skip_etl else "; ETL excluded"),
            "spark_master": master_url,
            "cluster_configuration": {
                "executor_instances_requested": int(os.environ.get("SPARK_EXECUTOR_INSTANCES", "3")),
                "partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
                "xgboost_workers": int(os.environ.get("SPARK_XGB_WORKERS", "3")),
                "driver_host": os.environ.get("SPARK_DRIVER_HOST"),
                "note": "Executor requests are not a measurement of physical machines; use the Spark Master UI to verify workers",
            },
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_pipeline_wall_clock_seconds": round(total_pipeline_wall, 2),
            "total_pipeline_wall_clock_minutes": round(total_pipeline_wall / 60, 2),
            "etl_stage": etl_results,
            "mllib_stage": ml_results
        }

        metrics_filename = "spark_training_metrics.json" if skip_etl else "spark_end_to_end_metrics.json"
        metrics_file = METRICS_DIR / metrics_filename
        if metrics_file.exists():
            previous = metrics_file.read_bytes()
            archive_dir = METRICS_DIR / "archive"
            archive_dir.mkdir(exist_ok=True)
            archive_file = archive_dir / ("metrics_" + hashlib.sha256(previous).hexdigest()[:16] + ".json")
            if not archive_file.exists():
                archive_file.write_bytes(previous)
        pending_metrics = metrics_file.with_suffix(".tmp")
        pending_metrics.write_text(json.dumps(summary_metrics, indent=4, ensure_ascii=False), encoding="utf-8")
        pending_metrics.replace(metrics_file)

        # Store each invocation separately; failures must not become benchmark scores.
        run_dir = METRICS_DIR / "runs" / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        run_dir.mkdir(parents=True, exist_ok=False)
        with open(run_dir / metrics_filename, "w", encoding="utf-8") as f:
            json.dump(summary_metrics, f, indent=4, ensure_ascii=False)
        csv_rows = []
        for info in ml_results.values():
            row = {"Model": info["name"], "Status": info["status"],
                   "Error": info.get("error", "")}
            if info["status"] == "success":
                row.update({
                    "Train Time (s)": info["wall_clock_time_seconds"],
                    "Latency (ms/1k)": info["latency_ms_per_1k"],
                    "Driver Python RSS after evaluation (MB)": info["driver_python_rss_after_evaluation_mb"],
                    "Accuracy (%)": info["accuracy"] * 100,
                    "Weighted Precision (%)": info["precision_weighted"] * 100,
                    "Weighted Recall (%)": info["recall_weighted"] * 100,
                    "Weighted F1 (%)": info["f1_score_weighted"] * 100,
                    "Macro F1 (%)": info["f1_score_macro"] * 100,
                })
            csv_rows.append(row)
        pd.DataFrame(csv_rows).to_csv(run_dir / "model_metrics.csv", index=False)
        for info in ml_results.values():
            print(f"{info['name']}: {info['status']} | {info.get('error', '')}")
        print(f"[✓] Kết quả của lần chạy được lưu tại: {run_dir}")

        failed = [info["name"] for info in ml_results.values() if info["status"] != "success"]
        if failed:
            raise RuntimeError("Training incomplete; see saved errors: " + ", ".join(failed))
        return summary_metrics
    finally:
        spark.stop()



def main():
    parser = argparse.ArgumentParser(description="Train five PySpark models from cleaned Parquet (three-executor requests; ETL disabled by default)")
    parser.add_argument(
        "--master",
        type=str,
        default="spark://100.65.205.113:7077",
        help="Spark Master URL (mặc định: spark://100.65.205.113:7077 hoặc local[*])"
    )
    parser.add_argument(
        "--num-trees",
        type=int,
        default=20,
        help="Số lượng cây quyết định trong Random Forest (mặc định: 20)"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help="Độ sâu tối đa của cây (mặc định: 10)"
    )
    parser.add_argument("--parquet", type=Path, default=CLEAN_PARQUET_DIR,
                        help="Cleaned Parquet file or partitioned directory on the Driver")
    etl_mode = parser.add_mutually_exclusive_group()
    etl_mode.add_argument("--run-etl", "--force-etl", dest="run_etl", action="store_true",
                          help="Explicitly rebuild cleaned Parquet before training")
    etl_mode.add_argument("--skip-etl", action="store_true",
                          help="Skip ETL (already the default)")

    parser.add_argument("--spark-packages", default="", help="Comma-separated compatible Maven coordinates")
    parser.add_argument("--spark-repositories", default="", help="Comma-separated Maven repository URLs")
    parser.add_argument("--spark-py-files", default="", help="Python wrapper zip files for Spark")
    parser.add_argument("--xgb-workers", type=int, default=3, help="XGBoost Spark training workers")
    parser.add_argument("--executors", type=int, default=3, help="Requested executors (default: 3; does not start Spark Workers)")
    parser.add_argument("--partitions", type=int, default=12, help="Shuffle/input partitions (default: 12)")
    parser.add_argument("--driver-host", default=os.environ.get("SPARK_DRIVER_HOST"), help="Driver IP reachable from all three machines (e.g. Tailscale IP)")
    parser.add_argument("--check-dependencies", action="store_true", help="Check wrappers/JVM before ETL, then exit")

    args = parser.parse_args()
    if min(args.num_trees, args.max_depth, args.xgb_workers, args.executors, args.partitions) < 1:
        parser.error("num-trees, max-depth, xgb-workers, executors and partitions must be positive")
    for key, value in [("SPARK_EXTRA_PACKAGES", args.spark_packages),
                       ("SPARK_EXTRA_REPOSITORIES", args.spark_repositories),
                       ("SPARK_EXTRA_PY_FILES", args.spark_py_files)]:
        if value:
            os.environ[key] = value
    os.environ["SPARK_XGB_WORKERS"] = str(args.xgb_workers)
    os.environ["SPARK_EXECUTOR_INSTANCES"] = str(args.executors)
    os.environ["SPARK_PARTITIONS"] = str(args.partitions)
    if args.driver_host:
        os.environ["SPARK_DRIVER_HOST"] = args.driver_host
    if args.check_dependencies:
        session = create_spark_session(args.master)
        try:
            require_training_dependencies(session)
            print("All five model dependencies are available")
        finally:
            session.stop()
        return
    run_end_to_end_pipeline(
        master_url=args.master,
        num_trees=args.num_trees,
        max_depth=args.max_depth,
        skip_etl=not args.run_etl,
        parquet_path=args.parquet
    )


if __name__ == "__main__":
    main()
