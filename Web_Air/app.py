"""Module: app.py
Description: FastAPI Application phục vụ Web_Air Dashboard 3 Tab tương tác.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import json
import psutil
import platform
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
from core.results_store import metrics_rows, pipeline_rows, result_status
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
    arr_time_str: Optional[str] = Field(default=None, description="Giờ đến địa phương HH:MM")
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
            distance=req.distance,
            arr_time_str=req.arr_time_str
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
            distance=req.distance,
            arr_time_str=req.arr_time_str
        )
        results = predict_all_models(X_vec)

        if not results:
            raise ValueError("Không có mô hình nào dự đoán thành công")

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
    return JSONResponse(content=metrics_rows())

@app.get("/api/results-status")
def get_results_status():
    return JSONResponse(content=result_status())

@app.get("/api/pipeline-stages")
def get_pipeline_stages_table():
    return JSONResponse(content=pipeline_rows())

@app.get("/api/figures-list")
def get_figures_catalog():
    """Trả về danh mục phân loại đầy đủ 24 biểu đồ khoa học đã xuất bản."""
    targets = [
        ('cpu_quality_20261010.png', 'Chất lượng sáu mô hình CPU', 'Accuracy, Weighted F1 và Macro F1 của kết quả 10/10/2026.'),
        ('cpu_spark_speed_20261010.png', 'Thời gian CPU và Spark ngày 10/10', 'Thời gian train và suy luận của sáu mô hình CPU và ba mô hình Spark MLlib.'),
        ('cpu_feature_importance_20261010.png', 'Đặc trưng của Random Forest và CatBoost CPU', 'Hai mô hình sử dụng thang đo tầm quan trọng khác nhau.'),
        ('delay_distribution_7m_pie.png', 'Phân bố sáu lớp', 'OnTime chiếm khoảng 79,2% dữ liệu sạch.'),
        ('cm_random_forest_cpu_7m.png', 'Ma trận nhầm lẫn Random Forest CPU', 'Tỷ lệ dự đoán theo từng lớp.'),
        ('cm_catboost_7m.png', 'Ma trận nhầm lẫn CatBoost CPU', 'Tỷ lệ dự đoán theo từng lớp.'),
    ]
    return [{'category': 'confusion_matrix' if name.startswith('cm_') else ('feature_importance' if 'feature_importance' in name else 'overview'), 'category_name':'Kết quả thực nghiệm','filename':name,'title':title,'desc':desc}
        for name,title,desc in targets if any(os.path.isfile(os.path.join(folder,name)) for folder in
        [os.path.join(PROJECT_ROOT,'Bao_Cao','figures_20261010'),FIGURES_COMBINED_DIR,FIGURES_INDIVIDUAL_DIR])]


@app.get("/api/figures/{filename}")
def serve_figure(filename: str):
    """Phục vụ file ảnh biểu đồ khoa học chất lượng cao."""
    if filename != os.path.basename(filename):
        raise HTTPException(status_code=404, detail="Tên hình không hợp lệ")
    search_dirs = [
        os.path.join(PROJECT_ROOT, "Bao_Cao", "figures_20261010"),
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
        "cpu": platform.processor() or platform.machine(),
        "gpu": "Chưa đo",
        "ram_total_gb": round(mem.total / (1024**3), 2),
        "ram_available_gb": round(mem.available / (1024**3), 2),
        "ram_used_percent": mem.percent,
        "process_ram_mb": round(proc.memory_info().rss / (1024*1024), 2),
        "dataset_rows_raw": "7,079,081 dòng CSV (1.22 GB)",
        "dataset_rows_clean": "6,965,267 dòng sạch; Parquet 140.57 MB theo kết quả 10/10",
        "models_count": len(model_manager.models)
    })
