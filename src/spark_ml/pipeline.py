"""Module: pipeline.py
Description: Pipeline học máy phân tán trên Apache Spark (PySpark MLlib)
huấn luyện Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import time
import argparse

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, floor
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def create_spark_session(app_name="FlightDelay_SparkML"):
    """Khởi tạo SparkSession tối ưu hóa bộ nhớ cho đơn máy hoặc cụm."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()

def build_feature_pipeline(categorical_cols, numeric_cols):
    """Xây dựng Pipeline trích xuất và lắp ráp đặc trưng trên Spark."""
    stages = []
    indexed_cat_cols = []
    
    for c in categorical_cols:
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="keep")
        stages.append(indexer)
        indexed_cat_cols.append(f"{c}_indexed")
        
    assembler = VectorAssembler(
        inputCols=numeric_cols + indexed_cat_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    stages.append(assembler)
    
    return Pipeline(stages=stages)

def train_and_evaluate_rf(train_df, test_df, num_trees=50, max_depth=10):
    """Huấn luyện và đánh giá RandomForestClassifier phân tán."""
    print(f"[*] Đang cấu hình Spark Random Forest (Trees={num_trees}, MaxDepth={max_depth})...")
    
    rf = RandomForestClassifier(
        labelCol="delay_cause_code",
        featuresCol="features",
        numTrees=num_trees,
        maxDepth=max_depth,
        maxBins=512,
        seed=42
    )
    
    start_time = time.perf_counter()
    rf_model = rf.fit(train_df)
    train_time = time.perf_counter() - start_time
    print(f"[✓] Huấn luyện hoàn tất trong: {train_time:.3f} giây.")
    
    predictions = rf_model.transform(test_df)
    
    eval_acc = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="accuracy")
    eval_f1 = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="f1")
    eval_prec = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="weightedPrecision")
    eval_rec = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="weightedRecall")
    
    acc = eval_acc.evaluate(predictions)
    f1 = eval_f1.evaluate(predictions)
    prec = eval_prec.evaluate(predictions)
    rec = eval_rec.evaluate(predictions)
    
    print("\n" + "="*55)
    print(" KẾT QUẢ SPARK MLLIB RANDOM FOREST (PHÂN TÁN)")
    print("="*55)
    print(f" • Accuracy           : {acc * 100:.2f}%")
    print(f" • Weighted Precision : {prec * 100:.2f}%")
    print(f" • Weighted Recall    : {rec * 100:.2f}%")
    print(f" • Weighted F1-Score  : {f1 * 100:.2f}%")
    print(f" • Training Time      : {train_time:.3f}s")
    print("="*55 + "\n")
    
    return rf_model, predictions

def main():
    parser = argparse.ArgumentParser(description="Chạy Spark MLlib Pipeline")
    parser.add_argument(
        "--data",
        type=str,
        default=r"Flight Delay Dataset — 2024/cleaned_sample.parquet",
        help="Đường dẫn dữ liệu Parquet đã làm sạch"
    )
    args = parser.parse_args()
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[*] Đang nạp dữ liệu vào Spark DataFrame từ: {args.data}")
    if args.data.endswith('.parquet'):
        df = spark.read.parquet(args.data)
    else:
        df = spark.read.csv(args.data, header=True, inferSchema=True)
        
    print(f"[*] Tổng số dòng dữ liệu trong Spark: {df.count():,}")
    
    num_cols = ['month', 'day_of_month', 'day_of_week', 'dep_hour', 'arr_hour', 'crs_elapsed_time', 'distance']
    cat_cols = ['op_unique_carrier', 'origin', 'dest', 'dep_time_of_day']
    
    # Chuẩn bị pipeline đặc trưng
    feature_pipeline = build_feature_pipeline(cat_cols, num_cols)
    pipeline_model = feature_pipeline.fit(df)
    transformed_df = pipeline_model.transform(df)
    
    train_df, test_df = transformed_df.randomSplit([0.8, 0.2], seed=42)
    print(f"[*] Train partitions: {train_df.rdd.getNumPartitions()}, Test partitions: {test_df.rdd.getNumPartitions()}")
    
    train_and_evaluate_rf(train_df, test_df)
    
    spark.stop()

if __name__ == "__main__":
    main()
