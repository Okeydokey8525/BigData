"""Module: predictor.py
Description: Tiền xử lý chuẩn xác 13 đặc trưng và thực thi suy luận mô hình học máy.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import time
from datetime import datetime
import numpy as np
import pandas as pd
from config import (
    FEATURE_COLUMNS_NUM, FEATURE_COLUMNS_CAT,
    FEATURE_COLUMNS, CAUSE_METADATA
)
from core.model_loader import model_manager

def build_13_features(
    op_unique_carrier: str,
    origin: str,
    dest: str,
    fl_date: str,
    dep_time_str: str,
    crs_elapsed_time: float,
    distance: float
) -> np.ndarray:
    """Chuyển đổi các tham số đầu vào từ người dùng thành vector 13 đặc trưng float32.
    Đảm bảo 100% đồng nhất với pipeline huấn luyện train_all_7m.py.
    """
    # 1. Trích xuất thời gian lịch
    try:
        dt = datetime.strptime(str(fl_date).strip(), "%Y-%m-%d")
        month = float(dt.month)
        day_of_month = float(dt.day)
        day_of_week = float(dt.isoweekday())  # 1=Thứ Hai ... 7=Chủ Nhật
    except Exception:
        month, day_of_month, day_of_week = 7.0, 15.0, 1.0

    # 2. Trích xuất giờ và phút cất cánh
    try:
        parts = str(dep_time_str).strip().split(":")
        dep_hour = int(parts[0])
        dep_min = int(parts[1]) if len(parts) > 1 else 0
    except Exception:
        dep_hour, dep_min = 12, 0

    dep_min_of_day = float(dep_hour * 60 + dep_min)
    arr_min_of_day = float((dep_min_of_day + float(crs_elapsed_time)) % 1440)
    arr_hour = float((int(arr_min_of_day) // 60) % 24)

    # 3. Phân khúc thời gian trong ngày (dep_time_of_day)
    if 5 <= dep_hour < 12:
        time_of_day_str = "Morning (05:00 - 11:59)"
    elif 12 <= dep_hour < 17:
        time_of_day_str = "Afternoon (12:00 - 16:59)"
    elif 17 <= dep_hour < 21:
        time_of_day_str = "Evening (17:00 - 20:59)"
    else:
        time_of_day_str = "Night (21:00 - 04:59)"

    # 4. Mã hóa biến danh mục qua LabelEncoder đã fit
    encoders = model_manager.encoders
    cat_vals = {
        'op_unique_carrier': str(op_unique_carrier).strip().upper(),
        'origin': str(origin).strip().upper(),
        'dest': str(dest).strip().upper(),
        'dep_time_of_day': time_of_day_str
    }

    encoded_cat = {}
    for col, raw_val in cat_vals.items():
        if col in encoders:
            le = encoders[col]
            if raw_val in le.classes_:
                encoded_cat[col] = float(le.transform([raw_val])[0])
            else:
                encoded_cat[col] = 0.0  # Fallback nếu mã sân bay / hãng bay lạ
        else:
            encoded_cat[col] = 0.0

    # 5. Lắp ráp đúng thứ tự 13 đặc trưng (9 Numeric + 4 Categorical)
    row_values = [
        month,
        day_of_month,
        day_of_week,
        float(dep_hour),
        float(arr_hour),
        dep_min_of_day,
        arr_min_of_day,
        float(crs_elapsed_time),
        float(distance),
        encoded_cat['op_unique_carrier'],
        encoded_cat['origin'],
        encoded_cat['dest'],
        encoded_cat['dep_time_of_day']
    ]

    return np.array([row_values], dtype=np.float32)

def predict_single_model(model_id: str, input_features: np.ndarray) -> dict:
    """Thực thi suy luận trên 1 mô hình cụ thể và trả về kết quả chi tiết."""
    model = model_manager.get_model(model_id)
    if model is None:
        raise ValueError(f"Mô hình '{model_id}' chưa được nạp vào hệ thống.")

    # Nếu là Logistic Regression -> áp dụng StandardScaler
    if model_id == "logistic_regression" and model_manager.scaler is not None:
        X_eval = model_manager.scaler.transform(input_features)
    else:
        X_eval = input_features

    # Đo độ trễ suy luận chính xác bằng perf_counter (ms)
    t_start = time.perf_counter()
    raw_pred = np.array(model.predict(X_eval)).ravel()
    pred_label = int(raw_pred[0])
    latency_ms = (time.perf_counter() - t_start) * 1000

    # Tính toán phân bố xác suất cho 6 nhãn (0..5)
    probs = [0.0] * 6
    if hasattr(model, "predict_proba"):
        try:
            raw_probs = np.array(model.predict_proba(X_eval)).reshape(-1)
            for idx, p in enumerate(raw_probs):
                if idx < 6:
                    probs[idx] = float(p)
        except Exception:
            probs[pred_label] = 1.0
    else:
        probs[pred_label] = 1.0

    # Chuẩn hóa tổng xác suất = 100%
    sum_p = sum(probs)
    if sum_p > 0:
        probs = [p / sum_p for p in probs]

    confidence_pct = round(probs[pred_label] * 100, 2)
    meta = CAUSE_METADATA.get(pred_label, CAUSE_METADATA[0])

    probabilities_detail = []
    for idx in range(6):
        c_info = CAUSE_METADATA.get(idx, CAUSE_METADATA[0])
        probabilities_detail.append({
            "code": idx,
            "name": c_info["name"],
            "title": c_info["title"],
            "icon": c_info["icon"],
            "color": c_info["color"],
            "percent": round(probs[idx] * 100, 2)
        })

    model_display_name = model_manager.models.get(model_id, {}).get("name", model_id)

    return {
        "model_id": model_id,
        "model_name": model_display_name,
        "predicted_code": pred_label,
        "predicted_title": meta["title"],
        "predicted_name": meta["name"],
        "icon": meta["icon"],
        "color": meta["color"],
        "badge": meta["badge"],
        "description": meta["desc"],
        "advice": meta["advice"],
        "confidence_percent": confidence_pct,
        "latency_ms": round(latency_ms, 3),
        "probabilities": probabilities_detail
    }

def predict_all_models(input_features: np.ndarray) -> list:
    """Thực thi suy luận đối đầu song song trên cả 6 mô hình cho cùng 1 chuyến bay."""
    results = []
    for mid in [
        "catboost", "xgboost", "random_forest_cpu",
        "lightgbm", "decision_tree", "logistic_regression"
    ]:
        if mid in model_manager.models:
            try:
                res = predict_single_model(mid, input_features)
                results.append(res)
            except Exception as e:
                print(f"[!] Lỗi khi chạy đối đầu {mid}: {e}")
    return results
