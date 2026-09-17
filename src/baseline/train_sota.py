"""Module: train_sota.py
Description: Huấn luyện các mô hình Cải tiến Tiên tiến (State-of-the-Art Boosted Trees):
LightGBM, CatBoost và XGBoost (hỗ trợ CPU và tăng tốc GPU).
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
from sklearn.preprocessing import LabelEncoder

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
    
    # Mã hóa các biến phân loại thành dạng số
    for col in FEATURE_COLUMNS_CAT:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        
    return X, y

def main():
    parser = argparse.ArgumentParser(description="Huấn luyện các mô hình SOTA Gradient Boosting")
    parser.add_argument(
        "--data",
        type=str,
        default=r"Flight Delay Dataset — 2024/cleaned_sample.parquet",
        help="Đường dẫn tệp dữ liệu đã làm sạch"
    )
    parser.add_argument(
        "--use_gpu",
        action="store_true",
        help="Bật chế độ tăng tốc GPU nếu có phần cứng hỗ trợ (Kaggle T4 hoặc RTX)"
    )
    args = parser.parse_args()
    
    print(f"[*] Đang tải dữ liệu: {args.data}")
    if args.data.endswith('.parquet'):
        df = pd.read_parquet(args.data)
    else:
        df = pd.read_csv(args.data)
        
    X, y = prepare_features(df)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    target_names = ['OnTime', 'Carrier', 'Weather', 'NAS', 'Security', 'LateAircraft']
    unique_labels = sorted(list(set(y)))
    active_target_names = [target_names[i] for i in unique_labels]
    
    # 1. LIGHTGBM
    try:
        import lightgbm as lgb
        print("\n" + "="*60)
        print(f"1. HUẤN LUYỆN LIGHTGBM (Microsoft) [GPU: {args.use_gpu}]")
        print("="*60)
        device_type = 'gpu' if args.use_gpu else 'cpu'
        lgb_model = lgb.LGBMClassifier(
            n_estimators=100,
            learning_rate=0.1,
            device=device_type,
            random_state=42,
            verbose=-1
        )
        with BenchmarkTimer("LightGBM Training"):
            lgb_model.fit(X_train, y_train)
            
        y_pred_lgb = lgb_model.predict(X_test)
        evaluate_multiclass_predictions(y_test, y_pred_lgb, labels=unique_labels, target_names=active_target_names)
    except ImportError:
        print("[!] Thư viện lightgbm chưa được cài đặt.")
        
    # 2. CATBOOST
    try:
        from catboost import CatBoostClassifier
        print("\n" + "="*60)
        print(f"2. HUẤN LUYỆN CATBOOST (Yandex) [GPU: {args.use_gpu}]")
        print("="*60)
        task_type = 'GPU' if args.use_gpu else 'CPU'
        cb_model = CatBoostClassifier(
            iterations=100,
            learning_rate=0.1,
            task_type=task_type,
            random_seed=42,
            verbose=0
        )
        with BenchmarkTimer("CatBoost Training"):
            cb_model.fit(X_train, y_train)
            
        y_pred_cb = cb_model.predict(X_test)
        evaluate_multiclass_predictions(y_test, y_pred_cb, labels=unique_labels, target_names=active_target_names)
    except ImportError:
        print("[!] Thư viện catboost chưa được cài đặt.")
        
    # 3. XGBOOST
    try:
        import xgboost as xgb
        print("\n" + "="*60)
        print(f"3. HUẤN LUYỆN XGBOOST [GPU: {args.use_gpu}]")
        print("="*60)
        tree_method = 'hist'
        device = 'cuda' if args.use_gpu else 'cpu'
        xgb_model = xgb.XGBClassifier(
            n_estimators=100,
            learning_rate=0.1,
            tree_method=tree_method,
            device=device,
            random_state=42
        )
        with BenchmarkTimer("XGBoost Training"):
            xgb_model.fit(X_train, y_train)
            
        y_pred_xgb = xgb_model.predict(X_test)
        evaluate_multiclass_predictions(y_test, y_pred_xgb, labels=unique_labels, target_names=active_target_names)
    except ImportError:
        print("[!] Thư viện xgboost chưa được cài đặt.")

if __name__ == "__main__":
    main()
