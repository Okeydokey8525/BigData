"""Module: train_baseline.py
Description: Huấn luyện các mô hình Hướng Thuần (Logistic Regression,
Decision Tree, Random Forest) trên đơn máy CPU.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import argparse
import pandas as pd

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

# Đảm bảo import được module từ thư mục gốc
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from src.utils.metrics import BenchmarkTimer, evaluate_multiclass_predictions

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

def prepare_features(df):
    """Chuẩn bị ma trận đặc trưng X và vector nhãn y."""
    X = df[FEATURE_COLUMNS_NUM + FEATURE_COLUMNS_CAT].copy()
    y = df[TARGET_COL].values
    
    # Mã hóa các biến phân loại
    for col in FEATURE_COLUMNS_CAT:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        
    return X, y

def main():
    parser = argparse.ArgumentParser(description="Huấn luyện các mô hình Baseline trên CPU")
    parser.add_argument(
        "--data",
        type=str,
        default=r"Flight Delay Dataset — 2024/cleaned_sample.parquet",
        help="Đường dẫn tệp dữ liệu đã qua tiền xử lý (Parquet hoặc CSV)"
    )
    args = parser.parse_args()
    
    print(f"[*] Đang tải dữ liệu: {args.data}")
    if args.data.endswith('.parquet'):
        df = pd.read_parquet(args.data)
    else:
        df = pd.read_csv(args.data)
        
    print(f"[*] Kích thước dữ liệu: {df.shape}")
    
    X, y = prepare_features(df)
    
    # Chia tập train/test theo tỷ lệ 80/20 có phân tầng
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"[*] Kích thước tập Train: {X_train.shape}, Tập Test: {X_test.shape}\n")
    
    # Chuẩn hóa đặc trưng cho Logistic Regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    target_names = ['OnTime', 'Carrier', 'Weather', 'NAS', 'Security', 'LateAircraft']
    unique_labels = sorted(list(set(y)))
    active_target_names = [target_names[i] for i in unique_labels]
    
    # 1. MODEL 1: LOGISTIC REGRESSION (BASELINE)
    print("="*60)
    print("1. HUẤN LUYỆN LOGISTIC REGRESSION (CPU)")
    print("="*60)
    lr_model = LogisticRegression(max_iter=500, random_state=42)
    with BenchmarkTimer("Logistic Regression Training"):
        lr_model.fit(X_train_scaled, y_train)
        
    y_pred_lr = lr_model.predict(X_test_scaled)
    evaluate_multiclass_predictions(y_test, y_pred_lr, labels=unique_labels, target_names=active_target_names)
    
    # 2. MODEL 2: DECISION TREE
    print("="*60)
    print("2. HUẤN LUYỆN DECISION TREE (CPU)")
    print("="*60)
    dt_model = DecisionTreeClassifier(max_depth=10, random_state=42)
    with BenchmarkTimer("Decision Tree Training"):
        dt_model.fit(X_train, y_train)
        
    y_pred_dt = dt_model.predict(X_test)
    evaluate_multiclass_predictions(y_test, y_pred_dt, labels=unique_labels, target_names=active_target_names)
    
    # 3. MODEL 3: RANDOM FOREST (CPU)
    print("="*60)
    print("3. HUẤN LUYỆN RANDOM FOREST (CPU - Single Node)")
    print("="*60)
    rf_model = RandomForestClassifier(n_estimators=50, max_depth=10, n_jobs=-1, random_state=42)
    with BenchmarkTimer("Random Forest CPU Training"):
        rf_model.fit(X_train, y_train)
        
    y_pred_rf = rf_model.predict(X_test)
    evaluate_multiclass_predictions(y_test, y_pred_rf, labels=unique_labels, target_names=active_target_names)

if __name__ == "__main__":
    main()
