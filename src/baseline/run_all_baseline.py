"""Module: run_all_baseline.py
Description: Điều phối thực nghiệm toàn diện 6 mô hình Hướng Thuần (Single-Node ML):
1. Logistic Regression (CPU)
2. Decision Tree (CPU)
3. Random Forest (CPU - Trọng tâm đối sánh với Spark)
4. LightGBM (CPU/GPU)
5. CatBoost (CPU/GPU)
6. XGBoost (CPU/GPU)
Xử lý mất cân bằng lớp (class_weight='balanced'), đo đạc tài nguyên (RAM, Training Time, Inference Latency),
tự động xuất Models (.joblib), Metrics (CSV/JSON) và Figures (PNG).
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import time
import json
import psutil
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị lỗi mã hóa
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix
)
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

# SOTA Boosted Trees
import lightgbm as lgb
from catboost import CatBoostClassifier
import xgboost as xgb

# Cấu hình thẩm mỹ đồ thị
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False

FEATURE_COLUMNS_NUM = [
    'month',
    'day_of_month',
    'day_of_week',
    'dep_hour',
    'arr_hour',
    'crs_elapsed_time',
    'distance'
]

FEATURE_COLUMNS_CAT = [
    'op_unique_carrier',
    'origin',
    'dest',
    'dep_time_of_day'
]

TARGET_COL = 'delay_cause_code'
LABEL_NAMES = ['OnTime', 'Carrier', 'Weather', 'NAS', 'Security', 'LateAircraft']

def get_memory_usage_mb():
    """Lấy lượng RAM hiện tại đang sử dụng bởi tiến trình (MB)."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def prepare_data(data_path):
    """Nạp và tiền xử lý ma trận đặc trưng X, vector nhãn y."""
    print(f"[*] Đang tải dữ liệu từ: {data_path}")
    if data_path.endswith('.parquet'):
        df = pd.read_parquet(data_path)
    else:
        df = pd.read_csv(data_path)
        
    print(f"[✓] Kích thước dữ liệu nạp vào: {df.shape[0]:,} dòng, {df.shape[1]} cột")
    
    X = df[FEATURE_COLUMNS_NUM + FEATURE_COLUMNS_CAT].copy()
    y = df[TARGET_COL].values
    
    # Mã hóa các biến phân loại
    label_encoders = {}
    for col in FEATURE_COLUMNS_CAT:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        label_encoders[col] = le
        
    return X, y, label_encoders

def plot_confusion_matrix(cm, labels, model_name, save_path):
    """Vẽ và lưu ma trận nhầm lẫn chuẩn khoa học."""
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=labels, yticklabels=labels)
    plt.title(f"Ma Trận Nhầm Lẫn (Confusion Matrix) - {model_name}", fontsize=13, fontweight='bold')
    plt.xlabel("Nhãn Dự Đoán (Predicted)", fontsize=11)
    plt.ylabel("Nhãn Thực Tế (Actual)", fontsize=11)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()

def plot_feature_importance(importances, feature_names, model_name, save_path):
    """Vẽ biểu đồ tầm quan trọng của các đặc trưng."""
    feat_df = pd.DataFrame({
        'Feature': feature_names,
        'Importance': importances
    }).sort_values(by='Importance', ascending=False)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Importance', y='Feature', data=feat_df, palette='viridis')
    plt.title(f"Tầm Quan Trọng Của Đặc Trưng (Feature Importance) - {model_name}", fontsize=13, fontweight='bold')
    plt.xlabel("Mức Độ Quan Trọng Tương Đối", fontsize=11)
    plt.ylabel("Đặc Trưng Dự Báo", fontsize=11)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()

def plot_comparison_dashboard(results_df, save_path):
    """Vẽ dashboard tổng hợp so sánh 4 chỉ số chính giữa 6 mô hình."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Accuracy
    sns.barplot(x='Accuracy (%)', y='Model', data=results_df, ax=axes[0, 0], palette='crest')
    axes[0, 0].set_title("Độ Chính Xác Tổng Thể (Accuracy %)", fontsize=13, fontweight='bold')
    axes[0, 0].set_xlim(0, 100)
    for p in axes[0, 0].patches:
        axes[0, 0].annotate(f"{p.get_width():.2f}%", (p.get_width() - 12, p.get_y() + p.get_height()/2.),
                            va='center', color='white', fontweight='bold')
                            
    # 2. Weighted F1
    sns.barplot(x='Weighted F1 (%)', y='Model', data=results_df, ax=axes[0, 1], palette='magma')
    axes[0, 1].set_title("Chỉ Số Weighted F1-Score (%)", fontsize=13, fontweight='bold')
    axes[0, 1].set_xlim(0, 100)
    for p in axes[0, 1].patches:
        axes[0, 1].annotate(f"{p.get_width():.2f}%", (p.get_width() - 12, p.get_y() + p.get_height()/2.),
                            va='center', color='white', fontweight='bold')

    # 3. Macro F1 (Đo hiệu năng trên lớp thiểu số)
    sns.barplot(x='Macro F1 (%)', y='Model', data=results_df, ax=axes[1, 0], palette='flare')
    axes[1, 0].set_title("Chỉ Số Macro F1-Score (%) [Khả Năng Bắt Lớp Hiếm]", fontsize=13, fontweight='bold')
    axes[1, 0].set_xlim(0, 50)
    for p in axes[1, 0].patches:
        val = p.get_width()
        axes[1, 0].annotate(f"{val:.2f}%", (val + 1, p.get_y() + p.get_height()/2.),
                            va='center', color='black', fontweight='bold')

    # 4. Thời gian huấn luyện
    sns.barplot(x='Train Time (s)', y='Model', data=results_df, ax=axes[1, 1], palette='viridis')
    axes[1, 1].set_title("Thời Gian Huấn Luyện (Training Time - Giây)", fontsize=13, fontweight='bold')
    for p in axes[1, 1].patches:
        val = p.get_width()
        axes[1, 1].annotate(f"{val:.3f}s", (val + 0.05, p.get_y() + p.get_height()/2.),
                            va='center', color='black')

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()

def main():
    parser = argparse.ArgumentParser(description="Huấn luyện và Đánh giá Toàn diện 6 Mô hình Hướng Thuần")
    parser.add_argument(
        "--data",
        type=str,
        default=r"Flight Delay Dataset — 2024/cleaned_sample.parquet",
        help="Đường dẫn file dữ liệu đã làm sạch"
    )
    parser.add_argument(
        "--models_dir",
        type=str,
        default="models/baseline",
        help="Thư mục lưu trữ trọng số mô hình"
    )
    parser.add_argument(
        "--metrics_dir",
        type=str,
        default="results/metrics",
        help="Thư mục lưu trữ báo cáo số liệu CSV/JSON"
    )
    parser.add_argument(
        "--figures_dir",
        type=str,
        default="results/figures",
        help="Thư mục lưu trữ hình ảnh biểu đồ PNG"
    )
    args = parser.parse_args()

    os.makedirs(args.models_dir, exist_ok=True)
    os.makedirs(args.metrics_dir, exist_ok=True)
    os.makedirs(args.figures_dir, exist_ok=True)

    X, y, encoders = prepare_data(args.data)
    
    # Chia tập dữ liệu 80/20 có phân tầng
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    unique_labels = sorted(list(set(y)))
    active_labels = [LABEL_NAMES[i] for i in unique_labels]
    
    print(f"[*] Tập Train: {X_train.shape[0]:,} mẫu | Tập Test: {X_test.shape[0]:,} mẫu")
    print(f"[*] Các nhãn hoạt động trong tập dữ liệu: {active_labels}\n")

    # Chuẩn hóa đặc trưng cho Logistic Regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    joblib.dump(scaler, os.path.join(args.models_dir, "scaler.joblib"))

    # Tính trọng số phạt cho các lớp thiểu số
    sample_weights_train = compute_sample_weight('balanced', y_train)

    models_config = [
        {
            "name": "Logistic Regression",
            "model": LogisticRegression(class_weight='balanced', max_iter=1000, random_state=42),
            "use_scaled": True,
            "fit_params": {}
        },
        {
            "name": "Decision Tree",
            "model": DecisionTreeClassifier(class_weight='balanced', max_depth=10, random_state=42),
            "use_scaled": False,
            "fit_params": {}
        },
        {
            "name": "Random Forest (CPU)",
            "model": RandomForestClassifier(class_weight='balanced', n_estimators=100, max_depth=12, n_jobs=-1, random_state=42),
            "use_scaled": False,
            "fit_params": {}
        },
        {
            "name": "LightGBM",
            "model": lgb.LGBMClassifier(class_weight='balanced', n_estimators=100, learning_rate=0.08, random_state=42, verbose=-1),
            "use_scaled": False,
            "fit_params": {}
        },
        {
            "name": "CatBoost",
            "model": CatBoostClassifier(auto_class_weights='Balanced', iterations=150, learning_rate=0.08, random_seed=42, verbose=0),
            "use_scaled": False,
            "fit_params": {}
        },
        {
            "name": "XGBoost",
            "model": xgb.XGBClassifier(n_estimators=100, learning_rate=0.08, max_depth=6, random_state=42, eval_metric='mlogloss'),
            "use_scaled": False,
            "fit_params": {"sample_weight": sample_weights_train}
        }
    ]

    results = []
    reports_dict = {}

    print("="*75)
    print(" BẮT ĐẦU HUẤN LUYỆN 6 MÔ HÌNH HƯỚNG THUẦN (SINGLE-NODE BENCHMARK)")
    print("="*75)

    for item in models_config:
        model_name = item["name"]
        model = item["model"]
        use_scaled = item["use_scaled"]
        fit_params = item["fit_params"]

        print(f"\n>>> Đang huấn luyện: {model_name}...")
        
        train_X = X_train_scaled if use_scaled else X_train
        test_X = X_test_scaled if use_scaled else X_test

        mem_before = get_memory_usage_mb()
        start_time = time.perf_counter()

        # Huấn luyện
        model.fit(train_X, y_train, **fit_params)
        
        train_time = time.perf_counter() - start_time
        mem_after = get_memory_usage_mb()
        ram_used_mb = max(0.1, mem_after - mem_before)

        # Đo lường độ trễ suy luận (Inference latency)
        infer_start = time.perf_counter()
        y_pred = model.predict(test_X)
        infer_time_ms = ((time.perf_counter() - infer_start) / len(test_X)) * 1000

        # Đánh giá chỉ số
        acc = accuracy_score(y_test, y_pred) * 100
        prec_weighted = precision_score(y_test, y_pred, average='weighted', zero_division=0) * 100
        rec_weighted = recall_score(y_test, y_pred, average='weighted', zero_division=0) * 100
        f1_weighted = f1_score(y_test, y_pred, average='weighted', zero_division=0) * 100
        f1_macro = f1_score(y_test, y_pred, average='macro', zero_division=0) * 100

        print(f"  [✓] Hoàn tất trong: {train_time:.3f}s | RAM tăng: {ram_used_mb:.1f} MB | Độ trễ: {infer_time_ms*1000:.2f} µs/mẫu")
        print(f"  [✓] Accuracy: {acc:.2f}% | Weighted F1: {f1_weighted:.2f}% | Macro F1: {f1_macro:.2f}%")

        # Lưu model (.joblib)
        safe_name = model_name.lower().replace(" ", "_").replace("(", "").replace(")", "")
        model_file = os.path.join(args.models_dir, f"{safe_name}.joblib")
        joblib.dump(model, model_file)
        
        # Tạo và lưu Confusion Matrix
        cm = confusion_matrix(y_test, y_pred, labels=unique_labels)
        cm_path = os.path.join(args.figures_dir, f"confusion_matrix_{safe_name}.png")
        plot_confusion_matrix(cm, active_labels, model_name, cm_path)

        # Trích xuất Feature Importance nếu mô hình hỗ trợ
        if hasattr(model, 'feature_importances_'):
            fi_path = os.path.join(args.figures_dir, f"feature_importance_{safe_name}.png")
            plot_feature_importance(model.feature_importances_, X.columns.tolist(), model_name, fi_path)

        # Lưu chi tiết phân loại
        rep = classification_report(y_test, y_pred, labels=unique_labels, target_names=active_labels, output_dict=True, zero_division=0)
        reports_dict[model_name] = rep

        results.append({
            "Model": model_name,
            "Train Time (s)": round(train_time, 4),
            "Latency (ms/1k)": round(infer_time_ms * 1000, 2),
            "RAM Usage (MB)": round(ram_used_mb, 2),
            "Accuracy (%)": round(acc, 2),
            "Weighted Precision (%)": round(prec_weighted, 2),
            "Weighted Recall (%)": round(rec_weighted, 2),
            "Weighted F1 (%)": round(f1_weighted, 2),
            "Macro F1 (%)": round(f1_macro, 2)
        })

    # Xuất bảng tổng hợp kết quả
    results_df = pd.DataFrame(results)
    csv_path = os.path.join(args.metrics_dir, "baseline_model_comparison.csv")
    results_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    json_path = os.path.join(args.metrics_dir, "baseline_summary.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({"summary": results, "detailed_reports": reports_dict}, f, indent=2, ensure_ascii=False)

    # Xuất biểu đồ so sánh Dashboard
    dashboard_path = os.path.join(args.figures_dir, "baseline_comparison_dashboard.png")
    plot_comparison_dashboard(results_df, dashboard_path)

    print("\n" + "="*75)
    print(" BẢNG TỔNG HỢP SO SÁNH HIỆU NĂNG 6 MÔ HÌNH HƯỚNG THUẦN")
    print("="*75)
    print(results_df.to_string(index=False))
    print("="*75)
    print(f"\n[✓] Đã lưu bảng số liệu tại: {csv_path}")
    print(f"[✓] Đã lưu chi tiết JSON tại: {json_path}")
    print(f"[✓] Đã lưu biểu đồ Dashboard tại: {dashboard_path}")
    print(f"[✓] Đã lưu toàn bộ mô hình tại: {args.models_dir}/\n")

if __name__ == "__main__":
    main()
