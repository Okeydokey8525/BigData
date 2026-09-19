"""Module: app.py
Description: Ứng dụng Web API & Dashboard FastAPI phục vụ dự đoán nguyên nhân
trễ chuyến bay thương mại thời gian thực và trực quan hóa đối sánh 6 mô hình học máy.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import time
import json
from datetime import datetime
import pandas as pd
import numpy as np
import joblib

# Đảm bảo tiếng Việt không bị lỗi encoding console
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(
    title="Flight Delay AI Platform - HUIT Big Data",
    description="API dự đoán nguyên nhân trễ chuyến bay thương mại tại Hoa Kỳ năm 2024",
    version="1.0.0"
)

# Kích hoạt CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
MODELS_DIR = os.path.join(BASE_DIR, "models/baseline")
METRICS_DIR = os.path.join(BASE_DIR, "results/metrics")
FIGURES_DIR = os.path.join(BASE_DIR, "results/figures")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

# Mount Static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Biến toàn cục lưu trữ Models & Encoders nạp sẵn vào RAM
MODELS = {}
SCALER = None
ENCODERS = {}
METADATA = {}

FEATURE_COLUMNS_NUM = ['month', 'day_of_month', 'day_of_week', 'dep_hour', 'arr_hour', 'crs_elapsed_time', 'distance']
FEATURE_COLUMNS_CAT = ['op_unique_carrier', 'origin', 'dest', 'dep_time_of_day']

CAUSE_METADATA = {
    0: {
        "name": "OnTime_or_MinorDelay",
        "title": "Đúng Giờ hoặc Trễ Nhẹ (< 15 phút)",
        "icon": "✅",
        "color": "#10B981",
        "badge": "primary",
        "desc": "Chuyến bay được dự báo khởi hành và hạ cánh đúng giờ hoặc chênh lệch không đáng kể.",
        "advice": "Chuyến bay có độ tin cậy rất cao. Hành khách theo dõi cửa ra máy bay theo kế hoạch thông thường."
    },
    1: {
        "name": "Carrier Delay",
        "title": "Trễ do Lỗi Hãng Hàng Không",
        "icon": "✈️",
        "color": "#3B82F6",
        "badge": "primary",
        "desc": "Rủi ro phát sinh từ bảo trì kỹ thuật, tiếp nhiên liệu hoặc điều phối phi hành đoàn của hãng.",
        "advice": "Hãng hàng không chịu trách nhiệm hỗ trợ. Khách hàng nên bật thông báo trên app của hãng để nhận đổi cổng bay hoặc dịch vụ bồi hoàn kịp thời."
    },
    2: {
        "name": "Weather Delay",
        "title": "Trễ do Thời Tiết Cực Đoan",
        "icon": "⛈️",
        "color": "#F59E0B",
        "badge": "warning",
        "desc": "Thời tiết xấu (giông lốc, bão tuyết, tầm nhìn kém) ảnh hưởng đến đường bay an toàn.",
        "advice": "Kiểm tra dự báo thời tiết tại cả sân bay đi và sân bay đến. Chuẩn bị kế hoạch dự phòng nếu thời tiết diễn biến phức tạp."
    },
    3: {
        "name": "NAS Delay",
        "title": "Trễ do Hệ Thống Không Lưu (NAS)",
        "icon": "📡",
        "color": "#8B5CF6",
        "badge": "secondary",
        "desc": "Tắc nghẽn không phận quốc gia hoặc giới hạn tần suất cất/hạ cánh của đài kiểm soát không lưu.",
        "advice": "Thời gian lăn bánh ra đường băng (taxi-out) có thể kéo dài. Hành khách kiên nhẫn ổn định chỗ ngồi trên tàu bay."
    },
    4: {
        "name": "Security Delay",
        "title": "Trễ do An Ninh Sân Bay",
        "icon": "🛡️",
        "color": "#EC4899",
        "badge": "danger",
        "desc": "Kiểm tra an ninh bổ sung, xử lý sự cố an ninh nhà ga hoặc soi chiếu hành lý.",
        "advice": "Hành khách cần có mặt tại sân bay trước giờ bay tối thiểu 2.5 - 3 tiếng và chuẩn bị sẵn giấy tờ tùy thân hợp lệ."
    },
    5: {
        "name": "LateAircraft Delay",
        "title": "Trễ Dây Chuyền do Máy Bay Đến Muộn",
        "icon": "🔄",
        "color": "#EF4444",
        "badge": "danger",
        "desc": "Máy bay từ chặng trước bị trễ dẫn đến tàu bay chưa kịp về sân bay để phục vụ chặng kế tiếp.",
        "advice": "Tra cứu số hiệu đuôi tàu bay (Tail Number) trên Flightradar24 để biết chính xác vị trí máy bay đang đón bạn."
    }
}

def load_system_resources():
    """Nạp trước toàn bộ Mô hình, Scaler và Metadata vào bộ nhớ RAM khi server khởi động."""
    global MODELS, SCALER, ENCODERS, METADATA
    print("[*] Đang nạp tài nguyên mô hình vào RAM...")

    # Nạp Scaler
    scaler_path = os.path.join(MODELS_DIR, "scaler.joblib")
    if os.path.exists(scaler_path):
        SCALER = joblib.load(scaler_path)
        print("  [✓] Đã nạp StandardScaler")

    # Nạp Encoders
    encoders_path = os.path.join(MODELS_DIR, "encoders.joblib")
    if os.path.exists(encoders_path):
        ENCODERS = joblib.load(encoders_path)
        print("  [✓] Đã nạp Categorical Encoders")

    # Nạp Metadata
    meta_path = os.path.join(MODELS_DIR, "metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, 'r', encoding='utf-8') as f:
            METADATA = json.load(f)
        print("  [✓] Đã nạp Metadata danh mục")

    # Nạp các Models
    available_models = [
        ("random_forest_cpu", "random_forest_cpu.joblib"),
        ("lightgbm", "lightgbm.joblib"),
        ("xgboost", "xgboost.joblib"),
        ("catboost", "catboost.joblib"),
        ("decision_tree", "decision_tree.joblib"),
        ("logistic_regression", "logistic_regression.joblib")
    ]

    for model_id, filename in available_models:
        path = os.path.join(MODELS_DIR, filename)
        if os.path.exists(path):
            try:
                MODELS[model_id] = joblib.load(path)
                print(f"  [✓] Đã nạp mô hình: {model_id}")
            except Exception as e:
                print(f"  [!] Lỗi nạp {model_id}: {e}")

    print(f"[✓] Hoàn tất nạp {len(MODELS)} mô hình vào bộ nhớ RAM sẵn sàng phục vụ!\n")

# Nạp tài nguyên ngay khi khởi tạo module
load_system_resources()

@app.on_event("startup")
def startup_event():
    if not MODELS:
        load_system_resources()

# ==============================================================================
# SCHEMA ĐẦU VÀO DỰ ĐOÁN
# ==============================================================================
class FlightPredictionRequest(BaseModel):
    model_name: str = Field(default="random_forest_cpu", description="Tên mô hình muốn sử dụng")
    op_unique_carrier: str = Field(default="DL", description="Mã hãng bay (Carrier Code: AA, DL, UA, WN,...)")
    origin: str = Field(default="JFK", description="Mã IATA sân bay xuất phát")
    dest: str = Field(default="LAX", description="Mã IATA sân bay đến")
    fl_date: str = Field(default="2024-07-15", description="Ngày bay định dạng YYYY-MM-DD")
    dep_time_str: str = Field(default="18:30", description="Giờ cất cánh dự kiến HH:MM")
    crs_elapsed_time: float = Field(default=180.0, description="Thời gian bay dự kiến (phút)")
    distance: float = Field(default=1200.0, description="Khoảng cách tuyến bay (Dặm - Miles)")

# ==============================================================================
# ROUTES
# ==============================================================================
@app.get("/")
def serve_home():
    """Trả về giao diện Dashboard chính."""
    index_file = os.path.join(TEMPLATES_DIR, "index.html")
    if not os.path.exists(index_file):
        raise HTTPException(status_code=404, detail="Không tìm thấy index.html")
    return FileResponse(index_file)

@app.get("/api/metadata")
def get_metadata():
    """Trả về danh mục Hãng bay, Sân bay, Danh sách mô hình."""
    return JSONResponse(content=METADATA)

@app.get("/api/metrics")
def get_metrics_table(dataset: str = "7m"):
    """Trả về bảng đối sánh hiệu năng 7 mô hình học máy trên tập dữ liệu 7 triệu dòng."""
    candidates = [
        os.path.join(METRICS_DIR, "grand_model_comparison_7m.csv"),
        os.path.join(BASE_DIR, "results/full_7m/metrics/grand_model_comparison_7m.csv"),
        os.path.join(METRICS_DIR, "grand_model_comparison.csv")
    ]
    for target_path in candidates:
        if os.path.exists(target_path):
            df = pd.read_csv(target_path)
            return JSONResponse(content=df.to_dict(orient="records"))
    return JSONResponse(content=[])

@app.get("/api/pipeline-stages")
def get_pipeline_stages():
    """Trả về bảng số liệu chi tiết bóc tách thời gian 4 giai đoạn và toàn trình (7.07M dòng)."""
    candidates = [
        os.path.join(METRICS_DIR, "pipeline_stages_breakdown_7m.csv"),
        os.path.join(BASE_DIR, "results/full_7m/metrics/pipeline_stages_time_breakdown.csv")
    ]
    for stages_path in candidates:
        if os.path.exists(stages_path):
            df = pd.read_csv(stages_path)
            return JSONResponse(content=df.to_dict(orient="records"))
    return JSONResponse(content=[])

@app.get("/api/figures/{filename}")
def get_figure(filename: str, dataset: str = "7m"):
    """Cung cấp các biểu đồ khoa học dạng ảnh PNG hỗ trợ phân cấp folder và map alias linh hoạt."""
    # Danh sách các tên tệp khả dĩ (bao gồm tên gốc và alias chuyển đổi tương thích)
    possible_names = [filename]
    if filename.startswith("confusion_matrix_"):
        model_part = filename.replace("confusion_matrix_", "").replace(".png", "")
        possible_names.append(f"cm_{model_part}_7m.png")
        possible_names.append(f"cm_{model_part}.png")
    elif filename.startswith("feature_importance_"):
        model_part = filename.replace("feature_importance_", "").replace(".png", "")
        possible_names.append(f"feat_imp_{model_part}_7m.png")
        possible_names.append(f"feat_imp_{model_part}.png")
    elif filename == "grand_rf_comparison.png":
        possible_names.append("grand_rf_showdown_7m.png")
    elif filename == "baseline_comparison_dashboard.png":
        possible_names.append("grand_comparison_7m_dashboard.png")

    search_dirs = [
        os.path.join(FIGURES_DIR, "combined"),
        os.path.join(FIGURES_DIR, "individual"),
        FIGURES_DIR,
        os.path.join(BASE_DIR, "results/full_7m/figures/combined"),
        os.path.join(BASE_DIR, "results/full_7m/figures/individual"),
        os.path.join(BASE_DIR, "results/full_7m/figures")
    ]

    for fname in possible_names:
        for sdir in search_dirs:
            p = os.path.join(sdir, fname)
            if os.path.exists(p):
                return FileResponse(p, media_type="image/png")

    raise HTTPException(status_code=404, detail=f"Hình ảnh '{filename}' không tồn tại.")

@app.post("/api/predict")
def predict_flight_delay(req: FlightPredictionRequest):
    """Dự đoán nguyên nhân trễ chuyến bay thời gian thực."""
    if req.model_name not in MODELS:
        raise HTTPException(status_code=400, detail=f"Mô hình '{req.model_name}' chưa được nạp.")

    model = MODELS[req.model_name]

    # 1. Trích xuất đặc trưng thời gian
    try:
        dt = datetime.strptime(req.fl_date, "%Y-%m-%d")
        month = dt.month
        day_of_month = dt.day
        day_of_week = dt.isoweekday() # 1=Thứ Hai, 7=Chủ Nhật
    except Exception:
        month, day_of_month, day_of_week = 7, 15, 1

    try:
        parts = req.dep_time_str.split(":")
        dep_hour = int(parts[0])
    except Exception:
        dep_hour = 12

    arr_hour = int((dep_hour + (req.crs_elapsed_time / 60)) % 24)

    # Xác định time_of_day
    if 5 <= dep_hour < 12:
        time_of_day = "Morning (05:00 - 11:59)"
    elif 12 <= dep_hour < 17:
        time_of_day = "Afternoon (12:00 - 16:59)"
    elif 17 <= dep_hour < 21:
        time_of_day = "Evening (17:00 - 20:59)"
    else:
        time_of_day = "Night (21:00 - 04:59)"

    # 2. Mã hóa biến phân loại qua Encoders
    encoded_cat = {}
    for col, val in [
        ('op_unique_carrier', req.op_unique_carrier),
        ('origin', req.origin),
        ('dest', req.dest),
        ('dep_time_of_day', time_of_day)
    ]:
        if col in ENCODERS:
            le = ENCODERS[col]
            if val in le.classes_:
                encoded_cat[col] = int(le.transform([val])[0])
            else:
                encoded_cat[col] = 0 # Fallback giá trị đầu tiên nếu sân bay lạ
        else:
            encoded_cat[col] = 0

    # 3. Lắp ráp vector đặc trưng (11 đặc trưng)
    row_features = [
        month,
        day_of_month,
        day_of_week,
        dep_hour,
        arr_hour,
        req.crs_elapsed_time,
        req.distance,
        encoded_cat['op_unique_carrier'],
        encoded_cat['origin'],
        encoded_cat['dest'],
        encoded_cat['dep_time_of_day']
    ]

    feature_df = pd.DataFrame([row_features], columns=FEATURE_COLUMNS_NUM + FEATURE_COLUMNS_CAT)

    # Nếu là Logistic Regression -> cần chuẩn hóa
    if req.model_name == "logistic_regression" and SCALER is not None:
        feature_input = SCALER.transform(feature_df)
    else:
        feature_input = feature_df

    # 4. Thực thi suy luận & đo độ trễ
    start_time = time.perf_counter()
    pred_label = int(model.predict(feature_input)[0])
    latency_ms = (time.perf_counter() - start_time) * 1000

    # 5. Tính toán phân bố xác suất
    probs = [0.0] * 6
    if hasattr(model, "predict_proba"):
        try:
            raw_probs = model.predict_proba(feature_input)[0]
            for idx, p in enumerate(raw_probs):
                if idx < 6:
                    probs[idx] = float(p)
        except Exception:
            probs[pred_label] = 1.0
    else:
        probs[pred_label] = 1.0

    # Đảm bảo tổng xác suất bằng 100%
    sum_p = sum(probs)
    if sum_p > 0:
        probs = [p / sum_p for p in probs]

    confidence_pct = probs[pred_label] * 100

    cause_info = CAUSE_METADATA.get(pred_label, CAUSE_METADATA[0])

    probabilities_output = []
    for idx in range(6):
        meta = CAUSE_METADATA.get(idx, CAUSE_METADATA[0])
        probabilities_output.append({
            "code": idx,
            "title": meta["title"],
            "percent": round(probs[idx] * 100, 2),
            "color": meta["color"]
        })

    model_display_names = {
        "random_forest_cpu": "Random Forest (Scikit-Learn)",
        "lightgbm": "LightGBM (Microsoft)",
        "xgboost": "XGBoost (DMLC)",
        "catboost": "CatBoost (Yandex)",
        "decision_tree": "Decision Tree (Cây đơn)",
        "logistic_regression": "Logistic Regression"
    }

    return {
        "model_used": model_display_names.get(req.model_name, req.model_name),
        "predicted_cause_code": pred_label,
        "predicted_cause_name": cause_info["name"],
        "predicted_cause_title": cause_info["title"],
        "predicted_cause_desc": cause_info["desc"],
        "icon": cause_info["icon"],
        "badge_type": cause_info["badge"],
        "confidence_pct": round(confidence_pct, 2),
        "latency_ms": round(latency_ms, 3),
        "probabilities": probabilities_output,
        "advice": cause_info["advice"]
    }

if __name__ == "__main__":
    import uvicorn
    print("[*] Đang khởi động FastAPI Server trên cổng 8000...")
    uvicorn.run("src.serving.app:app", host="127.0.0.1", port=8000, reload=False)
