"""Module: pipeline.py
Description: Pipeline học máy phân tán trên Apache Spark (PySpark MLlib)
huấn luyện Random Forest phân tán dự đoán nguyên nhân trễ chuyến bay.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import time
import json
import argparse
import psutil
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
hadoop_dir = os.path.join(BASE_DIR, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["hadoop.home.dir"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import classification_report

MODELS_DIR = os.path.join(BASE_DIR, "models/spark")
METRICS_DIR = os.path.join(BASE_DIR, "results/metrics")
FIGURES_DIR = os.path.join(BASE_DIR, "results/figures")

TARGET_NAMES = ['OnTime', 'Carrier', 'Weather', 'NAS', 'Security', 'LateAircraft']

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

def train_and_evaluate_rf(train_df, test_df, feature_names, num_trees=50, max_depth=10, save_model_path=None):
    """Huấn luyện và đánh giá RandomForestClassifier phân tán trên Spark MLlib."""
    print(f"[*] Đang cấu hình Spark Random Forest (Trees={num_trees}, MaxDepth={max_depth})...")
    
    rf = RandomForestClassifier(
        labelCol="delay_cause_code",
        featuresCol="features",
        numTrees=num_trees,
        maxDepth=max_depth,
        maxBins=512,
        seed=42
    )
    
    process = psutil.Process(os.getpid())
    ram_before = process.memory_info().rss / (1024 * 1024)
    
    start_time = time.perf_counter()
    rf_model = rf.fit(train_df)
    train_time = time.perf_counter() - start_time
    
    ram_after = process.memory_info().rss / (1024 * 1024)
    ram_usage = max(0.1, round(ram_after - ram_before, 2))
    
    print(f"[✓] Huấn luyện Spark hoàn tất trong: {train_time:.3f} giây.")
    
    # Đo tốc độ suy luận
    pred_start = time.perf_counter()
    predictions = rf_model.transform(test_df)
    test_count = predictions.count()
    pred_time = time.perf_counter() - pred_start
    latency_per_1k = round((pred_time / max(test_count, 1)) * 1000 * 1000, 2)
    
    # Đánh giá bằng Spark Evaluators
    eval_acc = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="accuracy")
    eval_f1 = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="f1")
    eval_prec = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="weightedPrecision")
    eval_rec = MulticlassClassificationEvaluator(labelCol="delay_cause_code", predictionCol="prediction", metricName="weightedRecall")
    
    acc = eval_acc.evaluate(predictions)
    weighted_f1 = eval_f1.evaluate(predictions)
    prec = eval_prec.evaluate(predictions)
    rec = eval_rec.evaluate(predictions)
    
    # Lấy nhãn thực tế và dự đoán để tính Macro F1 chi tiết
    preds_pd = predictions.select("delay_cause_code", "prediction").toPandas()
    y_true = preds_pd["delay_cause_code"].astype(int).values
    y_pred = preds_pd["prediction"].astype(int).values
    
    unique_labels = sorted(list(set(np.unique(y_true)) | set(np.unique(y_pred))))
    target_names_used = [TARGET_NAMES[i] if i < len(TARGET_NAMES) else f"Class_{i}" for i in unique_labels]
    
    report_dict = classification_report(
        y_true, y_pred,
        labels=unique_labels,
        target_names=target_names_used,
        output_dict=True,
        zero_division=0
    )
    macro_f1 = report_dict["macro avg"]["f1-score"] * 100
    
    metrics = {
        "Model": "Random Forest (Spark MLlib)",
        "Train Time (s)": round(train_time, 4),
        "Latency (ms/1k)": latency_per_1k,
        "RAM Usage (MB)": ram_usage,
        "Accuracy (%)": round(acc * 100, 2),
        "Weighted Precision (%)": round(prec * 100, 2),
        "Weighted Recall (%)": round(rec * 100, 2),
        "Weighted F1 (%)": round(weighted_f1 * 100, 2),
        "Macro F1 (%)": round(macro_f1, 2)
    }
    
    print("\n" + "="*60)
    print(" KẾT QUẢ SPARK MLLIB RANDOM FOREST (PHÂN TÁN)")
    print("="*60)
    print(f" • Accuracy           : {metrics['Accuracy (%)']:.2f}%")
    print(f" • Weighted Precision : {metrics['Weighted Precision (%)']:.2f}%")
    print(f" • Weighted Recall    : {metrics['Weighted Recall (%)']:.2f}%")
    print(f" • Weighted F1-Score  : {metrics['Weighted F1 (%)']:.2f}%")
    print(f" • Macro F1-Score     : {metrics['Macro F1 (%)']:.2f}%")
    print(f" • Training Time      : {train_time:.3f}s")
    print(f" • Latency (per 1k)   : {latency_per_1k:.2f}ms")
    print("="*60 + "\n")
    
    # Lưu mô hình Spark và trích xuất Metadata
    if save_model_path:
        os.makedirs(save_model_path, exist_ok=True)
        rf_save_path = os.path.join(save_model_path, "rf_model")
        print(f"[*] Đang lưu Spark RF Model vào: {rf_save_path}")
        try:
            rf_model.write().overwrite().save(rf_save_path)
            print("[✓] Đã lưu thành công mô hình Spark qua native writer.")
        except Exception as e:
            print(f"[!] Chú ý khi lưu định dạng Spark native trên Windows: {e}")
        
        # Trích xuất metadata và Feature Importance của Spark RF
        try:
            importances = rf_model.featureImportances.toArray().tolist()
            feat_imp_dict = {feat: round(val, 4) for feat, val in zip(feature_names, importances)}
            # Sắp xếp giảm dần
            feat_imp_dict = dict(sorted(feat_imp_dict.items(), key=lambda item: item[1], reverse=True))
        except Exception:
            feat_imp_dict = {}

        model_summary = {
            "model_name": "Random Forest (Spark MLlib)",
            "num_trees": num_trees,
            "max_depth": max_depth,
            "max_bins": 512,
            "total_nodes": rf_model.totalNumNodes,
            "trees_summary": rf_model.toDebugString[:500] + "... (truncated)",
            "feature_importances": feat_imp_dict,
            "metrics": metrics
        }
        
        meta_json_path = os.path.join(save_model_path, "spark_rf_metadata.json")
        with open(meta_json_path, "w", encoding="utf-8") as f:
            json.dump(model_summary, f, ensure_ascii=False, indent=2)
        print(f"[✓] Đã lưu metadata mô hình Spark vào: {meta_json_path}")
        
    return rf_model, metrics, report_dict

def export_grand_comparison(spark_metrics, spark_report_dict, baseline_csv_path=None):
    """Xuất bảng so sánh tổng thể 7 mô hình và vẽ đồ thị trực quan."""
    os.makedirs(METRICS_DIR, exist_ok=True)
    os.makedirs(FIGURES_DIR, exist_ok=True)
    
    spark_df = pd.DataFrame([spark_metrics])
    spark_csv_path = os.path.join(METRICS_DIR, "spark_metrics.csv")
    spark_df.to_csv(spark_csv_path, index=False)
    print(f"[✓] Đã lưu kết quả Spark vào: {spark_csv_path}")
    
    # Lưu summary json cho Spark
    spark_json_path = os.path.join(METRICS_DIR, "spark_summary.json")
    with open(spark_json_path, "w", encoding="utf-8") as f:
        json.dump({"metrics": spark_metrics, "detailed_report": spark_report_dict}, f, ensure_ascii=False, indent=2)
    print(f"[✓] Đã lưu tóm tắt chi tiết Spark vào: {spark_json_path}")
    
    if baseline_csv_path and os.path.exists(baseline_csv_path):
        base_df = pd.read_csv(baseline_csv_path)
        base_df = base_df[base_df["Model"] != "Random Forest (Spark MLlib)"]
        grand_df = pd.concat([base_df, spark_df], ignore_index=True)
        
        grand_csv_path = os.path.join(METRICS_DIR, "grand_model_comparison.csv")
        grand_df.to_csv(grand_csv_path, index=False)
        print(f"[✓] Đã tạo bảng đối sánh tổng thể 7 mô hình: {grand_csv_path}")
        
        # Vẽ biểu đồ đối sánh trực diện Random Forest CPU vs Random Forest Spark
        rf_comparison = grand_df[grand_df["Model"].str.contains("Random Forest")]
        if len(rf_comparison) >= 2:
            plot_rf_showdown(rf_comparison)

def plot_rf_showdown(rf_df):
    """Vẽ biểu đồ đối kháng trực tiếp: Single-node Scikit-Learn RF vs Distributed Spark RF."""
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    
    colors = ['#3B82F6', '#10B981']
    models = rf_df['Model'].tolist()
    
    # 1. Thời gian huấn luyện & Độ trễ
    ax1 = axes[0]
    x = np.arange(2)
    width = 0.35
    
    train_times = rf_df['Train Time (s)'].values
    latencies = rf_df['Latency (ms/1k)'].values
    
    rects1 = ax1.bar(x - width/2, train_times, width, label='Train Time (giây)', color='#6366F1')
    rects2 = ax1.bar(x + width/2, latencies, width, label='Latency (ms/1k mẫu)', color='#EC4899')
    
    ax1.set_title('Hiệu năng Vận hành: Thời gian & Độ trễ', fontsize=12, fontweight='bold', pad=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels(['Scikit-Learn RF\n(Đơn máy CPU)', 'Spark MLlib RF\n(Phân tán RDD)'], fontsize=10)
    ax1.legend(frameon=True, facecolor='#ffffff', edgecolor='#e2e8f0')
    ax1.bar_label(rects1, padding=3, fmt='%.2fs')
    ax1.bar_label(rects2, padding=3, fmt='%.1fms')
    
    # 2. Điểm chất lượng dự đoán
    ax2 = axes[1]
    metrics_names = ['Accuracy (%)', 'Weighted F1 (%)', 'Macro F1 (%)']
    x2 = np.arange(len(metrics_names))
    width2 = 0.35
    
    m1_scores = rf_df[rf_df['Model'] == models[0]][metrics_names].values.flatten()
    m2_scores = rf_df[rf_df['Model'] == models[1]][metrics_names].values.flatten()
    
    r1 = ax2.bar(x2 - width2/2, m1_scores, width2, label=models[0], color='#3B82F6')
    r2 = ax2.bar(x2 + width2/2, m2_scores, width2, label=models[1], color='#10B981')
    
    ax2.set_title('Chất lượng Dự đoán: Accuracy & F1-Score', fontsize=12, fontweight='bold', pad=12)
    ax2.set_xticks(x2)
    ax2.set_xticklabels(['Accuracy', 'Weighted F1', 'Macro F1'], fontsize=10)
    ax2.set_ylim(0, 100)
    ax2.legend(frameon=True, facecolor='#ffffff', edgecolor='#e2e8f0')
    ax2.bar_label(r1, padding=3, fmt='%.1f%%')
    ax2.bar_label(r2, padding=3, fmt='%.1f%%')
    
    plt.suptitle('ĐỐI SÁNH TỐI HẬU: RANDOM FOREST ĐƠN MÁY vs PHÂN TÁN (SPARK MLLIB)', fontsize=13, fontweight='bold', y=0.98)
    plt.tight_layout()
    
    fig_path = os.path.join(FIGURES_DIR, "grand_rf_comparison.png")
    plt.savefig(fig_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[✓] Đã xuất biểu đồ đối sánh RF tối hậu: {fig_path}")

def main():
    parser = argparse.ArgumentParser(description="Chạy Spark MLlib Pipeline")
    parser.add_argument(
        "--data",
        type=str,
        default=os.path.join(BASE_DIR, "Flight Delay Dataset — 2024/cleaned_sample.parquet"),
        help="Đường dẫn dữ liệu Parquet đã làm sạch"
    )
    parser.add_argument(
        "--num-trees",
        type=int,
        default=50,
        help="Số lượng cây quyết định (default: 50)"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help="Độ sâu tối đa của cây (default: 10)"
    )
    parser.add_argument(
        "--save-model",
        type=str,
        default=os.path.join(MODELS_DIR, "spark_rf_model"),
        help="Đường dẫn thư mục lưu mô hình Spark"
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
    
    # Lưu Pipeline biến đổi đặc trưng nếu có đường dẫn
    if args.save_model:
        os.makedirs(args.save_model, exist_ok=True)
        fp_path = os.path.join(args.save_model, "feature_pipeline_model")
        print(f"[*] Đang lưu Feature Pipeline vào: {fp_path}")
        try:
            pipeline_model.write().overwrite().save(fp_path)
            print("[✓] Đã lưu Feature Pipeline thành công.")
        except Exception as e:
            print(f"[!] Chú ý khi lưu PipelineModel native trên Windows: {e}")
    
    train_df, test_df = transformed_df.randomSplit([0.8, 0.2], seed=42)
    print(f"[*] Train partitions: {train_df.rdd.getNumPartitions()}, Test partitions: {test_df.rdd.getNumPartitions()}")
    
    all_features = num_cols + [f"{c}_indexed" for c in cat_cols]
    
    rf_model, metrics, report = train_and_evaluate_rf(
        train_df, test_df,
        feature_names=all_features,
        num_trees=args.num_trees,
        max_depth=args.max_depth,
        save_model_path=args.save_model
    )
    
    baseline_csv = os.path.join(METRICS_DIR, "baseline_model_comparison.csv")
    export_grand_comparison(metrics, report, baseline_csv)
    
    spark.stop()
    print("[✓] Pipeline Spark MLlib đã hoàn thành xuất sắc!")

if __name__ == "__main__":
    main()
