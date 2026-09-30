"""Module: app.py
Description: FastAPI Application phục vụ Web_Air Dashboard 3 Tab tương tác.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import json
import psutil
import pandas as pd
from typing import Optional

# Hỗ trợ UTF-8 console Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config import (
    PROJECT_ROOT, STATIC_DIR, TEMPLATES_DIR, METRICS_DIR,
    FIGURES_DIR, FIGURES_COMBINED_DIR, FIGURES_INDIVIDUAL_DIR,
    PRESETS, CAUSE_METADATA
)
from core.model_loader import model_manager
from core.predictor import (
    build_13_features, predict_single_model, predict_all_models
)

app = FastAPI(
    title="Web_Air - Hệ Thống Dự Báo & Đối Sánh Mô Hình Trễ Chuyến Bay 7M",
    description="Ứng dụng Web Python phục vụ kiểm thử, suy luận và trực quan hóa thực nghiệm đồ án Big Data",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount thư mục static và cấu hình Jinja2 templates
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Khởi tạo nạp mô hình khi server start
@app.on_event("startup")
def startup_event():
    print("[*] Khởi động Web_Air Application...")
    model_manager.load_all()

# ==============================================================================
# SCHEMA KIỂM TRA ĐẦU VÀO
# ==============================================================================
class FlightPredictionInput(BaseModel):
    model_id: str = Field(default="catboost", description="ID mô hình muốn chạy")
    op_unique_carrier: str = Field(default="DL", description="Mã hãng bay")
    origin: str = Field(default="JFK", description="Mã IATA sân bay đi")
    dest: str = Field(default="LAX", description="Mã IATA sân bay đến")
    fl_date: str = Field(default="2024-07-15", description="Ngày bay YYYY-MM-DD")
    dep_time_str: str = Field(default="08:30", description="Giờ cất cánh dự kiến HH:MM")
    crs_elapsed_time: float = Field(default=360.0, description="Thời gian bay dự kiến (phút)")
    distance: float = Field(default=2475.0, description="Cự ly chuyến bay (dặm)")

# ==============================================================================
# ROUTES TRANG GIAO DIỆN
# ==============================================================================
@app.get("/", response_class=HTMLResponse)
def home_page(request: Request):
    """Render giao diện chính 3 Tab của Web_Air."""
    if not model_manager.is_loaded:
        model_manager.load_all()

    models_list = model_manager.get_models_list()
    carriers = model_manager.metadata.get("carriers", [])
    airports = model_manager.metadata.get("airports", [])

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "models": models_list,
            "carriers": carriers,
            "airports": airports,
            "presets": PRESETS,
            "causes": CAUSE_METADATA
        }
    )

# ==============================================================================
# REST API ENDPOINTS
# ==============================================================================
@app.get("/api/metadata")
def get_metadata():
    """Trả về siêu dữ liệu Hãng bay, Sân bay, Mô hình và Presets mẫu."""
    if not model_manager.is_loaded:
        model_manager.load_all()

    return JSONResponse(content={
        "models": model_manager.get_models_list(),
        "carriers": model_manager.metadata.get("carriers", []),
        "airports": model_manager.metadata.get("airports", []),
        "presets": PRESETS,
        "causes": CAUSE_METADATA
    })

@app.post("/api/predict")
def predict_flight(req: FlightPredictionInput):
    """API dự đoán chuyến bay bằng 1 mô hình cụ thể."""
    try:
        X_vec = build_13_features(
            op_unique_carrier=req.op_unique_carrier,
            origin=req.origin,
            dest=req.dest,
            fl_date=req.fl_date,
            dep_time_str=req.dep_time_str,
            crs_elapsed_time=req.crs_elapsed_time,
            distance=req.distance
        )
        res = predict_single_model(req.model_id, X_vec)
        return JSONResponse(content={"status": "success", "data": res})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/predict-all")
def predict_flight_head_to_head(req: FlightPredictionInput):
    """API chạy đối đầu cùng lúc cả 6 mô hình cho cùng 1 chuyến bay."""
    try:
        X_vec = build_13_features(
            op_unique_carrier=req.op_unique_carrier,
            origin=req.origin,
            dest=req.dest,
            fl_date=req.fl_date,
            dep_time_str=req.dep_time_str,
            crs_elapsed_time=req.crs_elapsed_time,
            distance=req.distance
        )
        results = predict_all_models(X_vec)

        # Tính toán sự đồng thuận (Consensus)
        votes = {}
        fastest_model = None
        min_latency = float("inf")

        for r in results:
            code = r["predicted_code"]
            votes[code] = votes.get(code, 0) + 1
            if r["latency_ms"] < min_latency:
                min_latency = r["latency_ms"]
                fastest_model = r["model_name"]

        consensus_code = max(votes, key=votes.get) if votes else 0
        consensus_info = CAUSE_METADATA.get(consensus_code, CAUSE_METADATA[0])

        return JSONResponse(content={
            "status": "success",
            "consensus": {
                "predicted_code": consensus_code,
                "title": consensus_info["title"],
                "icon": consensus_info["icon"],
                "agree_count": votes.get(consensus_code, 0),
                "total_models": len(results),
                "fastest_model": fastest_model,
                "min_latency_ms": min_latency
            },
            "models_results": results
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/metrics")
def get_metrics_table():
    """Lấy dữ liệu bảng đối sánh 7 triệu dòng (Grand Comparison 7M)."""
    candidates = [
        os.path.join(METRICS_DIR, "grand_model_comparison_7m.csv"),
        os.path.join(PROJECT_ROOT, "results", "baseline", "metrics", "grand_model_comparison_7m.csv"),
        os.path.join(PROJECT_ROOT, "results", "full_7m", "metrics", "grand_model_comparison_7m.csv"),
        os.path.join(METRICS_DIR, "grand_model_comparison.csv")
    ]
    for target in candidates:
        if os.path.exists(target):
            df = pd.read_csv(target)
            return JSONResponse(content=df.to_dict(orient="records"))
    return JSONResponse(content=[])

@app.get("/api/pipeline-stages")
def get_pipeline_stages_table():
    """Lấy số liệu phân rã 4 giai đoạn pipeline."""
    candidates = [
        os.path.join(METRICS_DIR, "pipeline_stages_breakdown_7m.csv"),
        os.path.join(PROJECT_ROOT, "results", "baseline", "metrics", "pipeline_stages_breakdown_7m.csv"),
        os.path.join(PROJECT_ROOT, "results", "full_7m", "metrics", "pipeline_stages_time_breakdown.csv"),
        os.path.join(PROJECT_ROOT, "results", "full_7m", "metrics", "pipeline_stages_breakdown_7m.csv")
    ]
    for target in candidates:
        if os.path.exists(target):
            df = pd.read_csv(target)
            return JSONResponse(content=df.to_dict(orient="records"))
    return JSONResponse(content=[])

@app.get("/api/figures-list")
def get_figures_catalog():
    """Trả về danh mục phân loại đầy đủ 24 biểu đồ khoa học đã xuất bản."""
    catalog = [
        # Nhóm 1: Tổng quan & Đối sánh (11 ảnh)
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "grand_comparison_7m_dashboard.png",
            "title": "Bảng Vàng Đối Sánh Toàn Diện 7 Mô Hình",
            "desc": "Tập hợp 6 biểu đồ cốt lõi đánh giá Accuracy, F1, Thời gian, Latency và RAM trên 7 triệu dòng."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "grand_rf_showdown_7m.png",
            "title": "Trực Diện: Random Forest CPU vs Spark RF (Cùng 7M Dòng)",
            "desc": "Minh chứng Spark RF (49.89s) vượt trội nhanh hơn 2.8 lần so với RF Scikit-Learn CPU (106.79s)."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "multi_metric_radar_7m.png",
            "title": "Biểu Đồ Radar Đa Giác 5 Chỉ Số Chất Lượng",
            "desc": "So sánh 5 chiều: Accuracy, Precision, Recall, F1-Score, và Hiệu quả bộ nhớ của các mô hình."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "accuracy_vs_speed_bubble_7m.png",
            "title": "Biểu Đồ Bong Bóng (Bubble Trade-off Plot)",
            "desc": "Đánh đổi giữa Tốc độ huấn luyện, Chất lượng F1-Score và Dung lượng RAM chiếm dụng."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "delay_distribution_7m_pie.png",
            "title": "Biểu Đồ Donut Phân Bố 6 Nhãn Trễ Trên 7M Dòng",
            "desc": "Tỷ trọng: Đúng giờ (79.2%), Máy bay đến muộn (8.4%), Hãng bay (6.2%), Không lưu NAS (5.7%)."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "models_training_time_horizontal_bar_7m.png",
            "title": "Xếp Hạng Thời Gian Huấn Luyện (Training Time)",
            "desc": "GPU RTX 5050 tăng tốc XGBoost (19.27s) và CatBoost (19.60s) nhanh hơn 5.5 lần RF CPU."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "models_grouped_bar_7m.png",
            "title": "So Sánh Cột Kép: Accuracy & Weighted F1-Score",
            "desc": "CatBoost dẫn đầu toàn diện với Weighted F1 đạt 70.83% và Macro F1 đạt 16.54%."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "models_latency_bar_7m.png",
            "title": "Độ Trễ Suy Luận 1,000 Mẫu (Inference Latency)",
            "desc": "Decision Tree nhanh nhất (0.67ms/1k mẫu), tiếp đến LightGBM (11.71ms) và CatBoost (12.03ms)."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "models_peak_ram_bar_7m.png",
            "title": "Bộ Nhớ Tiêu Thụ Đỉnh (Peak RAM Usage)",
            "desc": "Spark RF tiết kiệm RAM nhất (1,012 MB) nhờ cơ chế phân vùng và tính toán luồng lười (Lazy Evaluation)."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "pipeline_stages_breakdown_7m.png",
            "title": "Bóc Tách 4 Giai Đoạn Pipeline (ETL, FE, Train, Eval)",
            "desc": "So sánh chi tiết tổng thời gian pipeline giữa Hướng Thuần (154.97s) và Hướng Spark (65.24s)."
        },
        {
            "category": "overview",
            "category_name": "Tổng Quan & Đối Sánh Toàn Trình",
            "filename": "pipeline_time_ratio_spark_pie.png",
            "title": "Tỷ Trọng Thời Gian Pipeline Spark",
            "desc": "Biểu đồ tròn phân bổ thời gian thực thi trong hệ sinh thái phân tán Apache Spark."
        },

        # Nhóm 2: Ma trận nhầm lẫn (Confusion Matrix - 7 ảnh)
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_catboost_7m.png",
            "title": "Confusion Matrix: CatBoost (SOTA GPU/CPU)",
            "desc": "Khả năng phân loại chính xác trên 1.04 triệu chuyến bay thuộc tập kiểm thử Test Set."
        },
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_xgboost_7m.png",
            "title": "Confusion Matrix: XGBoost (GPU Accelerated)",
            "desc": "Đo lường độ chính xác phân loại đa lớp trên 1.04 triệu chuyến bay tập Test."
        },
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_lightgbm_7m.png",
            "title": "Confusion Matrix: LightGBM",
            "desc": "Ma trận nhiệt thể hiện phân loại 6 nhãn trễ của mô hình LightGBM."
        },
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_random_forest_cpu_7m.png",
            "title": "Confusion Matrix: Random Forest (Scikit-Learn CPU)",
            "desc": "Kết quả phân loại của Random Forest đơn máy 8 luồng CPU trên 7 triệu dòng."
        },
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_spark_rf_7m.png",
            "title": "Confusion Matrix: Apache Spark MLlib Random Forest",
            "desc": "Ma trận nhầm lẫn của mô hình phân tán cốt lõi đồ án trên cụm Spark."
        },
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_decision_tree_7m.png",
            "title": "Confusion Matrix: Decision Tree",
            "desc": "Phân loại của Cây quyết định đơn lẻ làm mốc so sánh độ sâu và quá khớp."
        },
        {
            "category": "confusion_matrix",
            "category_name": "Ma Trận Nhầm Lẫn (Confusion Matrix)",
            "filename": "cm_logistic_regression_7m.png",
            "title": "Confusion Matrix: Logistic Regression",
            "desc": "Mô hình tuyến tính tối thiểu làm Baseline đối chứng."
        },

        # Nhóm 3: Tầm quan trọng đặc trưng (Feature Importance - 6 ảnh)
        {
            "category": "feature_importance",
            "category_name": "Tầm Quan Trọng Đặc Trưng (Feature Importance)",
            "filename": "feat_imp_catboost_7m.png",
            "title": "Feature Importance: CatBoost",
            "desc": "Xếp hạng mức độ đóng góp của 13 đặc trưng trong mô hình CatBoost."
        },
        {
            "category": "feature_importance",
            "category_name": "Tầm Quan Trọng Đặc Trưng (Feature Importance)",
            "filename": "feat_imp_xgboost_7m.png",
            "title": "Feature Importance: XGBoost",
            "desc": "Tầm quan trọng của các biến thời gian và danh mục trong XGBoost."
        },
        {
            "category": "feature_importance",
            "category_name": "Tầm Quan Trọng Đặc Trưng (Feature Importance)",
            "filename": "feat_imp_lightgbm_7m.png",
            "title": "Feature Importance: LightGBM",
            "desc": "Số lần phân nhánh và mức giảm Gain của các đặc trưng trong LightGBM."
        },
        {
            "category": "feature_importance",
            "category_name": "Tầm Quan Trọng Đặc Trưng (Feature Importance)",
            "filename": "feat_imp_random_forest_cpu_7m.png",
            "title": "Feature Importance: Random Forest (CPU)",
            "desc": "Mức độ giảm độ vẩn đục Gini (MDI) của 13 đặc trưng trong Scikit-Learn RF."
        },
        {
            "category": "feature_importance",
            "category_name": "Tầm Quan Trọng Đặc Trưng (Feature Importance)",
            "filename": "feat_imp_spark_rf_7m.png",
            "title": "Feature Importance: Spark MLlib Random Forest",
            "desc": "Tầm quan trọng đặc trưng trích xuất từ 20 cây phân tán của Spark RF Model."
        },
        {
            "category": "feature_importance",
            "category_name": "Tầm Quan Trọng Đặc Trưng (Feature Importance)",
            "filename": "feat_imp_decision_tree_7m.png",
            "title": "Feature Importance: Decision Tree",
            "desc": "Phân bổ trọng số phân nhánh của Cây quyết định đơn."
        }
    ]
    return JSONResponse(content=catalog)

@app.get("/api/figures/{filename}")
def serve_figure(filename: str):
    """Phục vụ file ảnh biểu đồ khoa học chất lượng cao."""
    search_dirs = [
        FIGURES_COMBINED_DIR,
        FIGURES_INDIVIDUAL_DIR,
        FIGURES_DIR,
        os.path.join(PROJECT_ROOT, "results", "baseline", "figures", "combined"),
        os.path.join(PROJECT_ROOT, "results", "baseline", "figures", "individual"),
        os.path.join(PROJECT_ROOT, "results", "baseline", "figures"),
        os.path.join(PROJECT_ROOT, "results", "full_7m", "figures", "combined"),
        os.path.join(PROJECT_ROOT, "results", "full_7m", "figures", "individual"),
        os.path.join(PROJECT_ROOT, "results", "full_7m", "figures")
    ]
    for sdir in search_dirs:
        p = os.path.join(sdir, filename)
        if os.path.exists(p):
            return FileResponse(p, media_type="image/png")

    raise HTTPException(status_code=404, detail=f"Không tìm thấy biểu đồ: {filename}")

@app.get("/api/system-status")
def get_system_status():
    """Thông số cấu hình phần cứng và tiến trình hệ thống."""
    mem = psutil.virtual_memory()
    proc = psutil.Process(os.getpid())
    return JSONResponse(content={
        "cpu": "AMD Ryzen 7 250 (8 Cores, 16 Threads)",
        "gpu": "NVIDIA GeForce RTX 5050 Laptop GPU (CUDA Cores)",
        "ram_total_gb": round(mem.total / (1024**3), 2),
        "ram_available_gb": round(mem.available / (1024**3), 2),
        "ram_used_percent": mem.percent,
        "process_ram_mb": round(proc.memory_info().rss / (1024*1024), 2),
        "dataset_rows_raw": "7,079,081 dòng CSV (1.22 GB)",
        "dataset_rows_clean": "6,965,267 dòng Parquet (228.4 MB, nén 81.7%)",
        "models_count": len(model_manager.models)
    })
