"""Module: config.py
Description: Cấu hình hệ thống, ánh xạ đường dẫn tài nguyên và đặc tả 13 đặc trưng.
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os

# Đường dẫn gốc dự án (cha của Web_Air)
WEB_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(WEB_DIR, ".."))

MODELS_DIR = os.path.join(PROJECT_ROOT, "models", "baseline")
SPARK_MODELS_DIR = os.path.join(PROJECT_ROOT, "models", "spark")
METRICS_DIR = os.path.join(PROJECT_ROOT, "results", "metrics")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "results", "figures")
FIGURES_COMBINED_DIR = os.path.join(FIGURES_DIR, "combined")
FIGURES_INDIVIDUAL_DIR = os.path.join(FIGURES_DIR, "individual")

STATIC_DIR = os.path.join(WEB_DIR, "static")
TEMPLATES_DIR = os.path.join(WEB_DIR, "templates")

# 13 ĐẶC TRƯNG CHUẨN HOÀN TOÀN TRÙNG KHỚP VỚI KỊCH BẢN HUẤN LUYỆN 7 TRIỆU DÒNG
FEATURE_COLUMNS_NUM = [
    'month', 'day_of_month', 'day_of_week',
    'dep_hour', 'arr_hour', 'dep_min_of_day', 'arr_min_of_day',
    'crs_elapsed_time', 'distance'
]

FEATURE_COLUMNS_CAT = [
    'op_unique_carrier', 'origin', 'dest', 'dep_time_of_day'
]

FEATURE_COLUMNS = FEATURE_COLUMNS_NUM + FEATURE_COLUMNS_CAT

# DANH SÁCH 6 LỚP NGUYÊN NHÂN TRỄ CHUYẾN BAY (0..5)
CAUSE_METADATA = {
    0: {
        "name": "OnTime_or_MinorDelay",
        "title": "Đúng Giờ hoặc Trễ Nhẹ (< 15 phút)",
        "icon": "✅",
        "color": "#10B981",
        "badge": "success",
        "desc": "Chuyến bay được dự báo khởi hành và hạ cánh đúng giờ hoặc chênh lệch không đáng kể.",
        "advice": "Chuyến bay có độ tin cậy rất cao. Hành khách theo dõi cửa ra máy bay theo kế hoạch thông thường."
    },
    1: {
        "name": "Carrier Delay",
        "title": "Trễ do Hãng Hàng Không",
        "icon": "✈️",
        "color": "#3B82F6",
        "badge": "primary",
        "desc": "Rủi ro phát sinh từ bảo trì kỹ thuật, tiếp nhiên liệu, dọn vệ sinh hoặc điều phối phi hành đoàn của hãng.",
        "advice": "Hãng hàng không chịu trách nhiệm hỗ trợ. Khách hàng nên bật thông báo trên app của hãng để nhận đổi cổng bay hoặc dịch vụ bồi hoàn kịp thời."
    },
    2: {
        "name": "Weather Delay",
        "title": "Trễ do Thời Tiết Cực Đoan",
        "icon": "⛈️",
        "color": "#F59E0B",
        "badge": "warning",
        "desc": "Thời tiết xấu (giông bão, tuyết phủ, sương mù, tầm nhìn kém) ảnh hưởng đến an toàn hành lang bay.",
        "advice": "Kiểm tra dự báo thời tiết tại cả sân bay đi và sân bay đến. Chuẩn bị kế hoạch dự phòng nếu thời tiết diễn biến phức tạp."
    },
    3: {
        "name": "NAS Delay",
        "title": "Trễ do Không Lưu Quốc Gia (NAS)",
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
        "desc": "Kiểm tra an ninh bổ sung, xử lý sự cố an ninh nhà ga, soi chiếu lại hành lý hoặc hành khách.",
        "advice": "Hành khách cần có mặt tại sân bay trước giờ bay tối thiểu 2.5 - 3 tiếng và chuẩn bị sẵn giấy tờ tùy thân hợp lệ."
    },
    5: {
        "name": "LateAircraft Delay",
        "title": "Trễ Dây Chuyền do Tàu Bay Đến Muộn",
        "icon": "🔄",
        "color": "#EF4444",
        "badge": "danger",
        "desc": "Máy bay từ chặng trước bị trễ dẫn đến tàu bay chưa kịp về sân bay để phục vụ chặng kế tiếp.",
        "advice": "Tra cứu số hiệu đuôi tàu bay (Tail Number) trên Flightradar24 để biết chính xác vị trí máy bay đang đón bạn."
    }
}

# CÁC CHUYẾN BAY MẪU ĐỂ NGƯỜI DÙNG THỬ NGHIỆM 1-CLICK (PRESETS)
PRESETS = [
    {
        "id": "transcon",
        "name": "JFK ➔ LAX (Xuyên Lục Địa, Delta Air Lines)",
        "op_unique_carrier": "DL",
        "origin": "JFK",
        "dest": "LAX",
        "fl_date": "2024-07-15",
        "dep_time_str": "08:30",
        "crs_elapsed_time": 360,
        "distance": 2475,
        "desc": "Tuyến bay dài bờ Đông sang bờ Tây vào sáng sớm mùa hè."
    },
    {
        "id": "hub_rush",
        "name": "ORD ➔ ATL (Giờ Cao Điểm Chiều, American Airlines)",
        "op_unique_carrier": "AA",
        "origin": "ORD",
        "dest": "ATL",
        "fl_date": "2024-08-20",
        "dep_time_str": "17:45",
        "crs_elapsed_time": 130,
        "distance": 606,
        "desc": "Tuyến giữa 2 siêu hub hàng không vào giờ cao điểm chiều dễ tắc nghẽn NAS."
    },
    {
        "id": "storm_season",
        "name": "DFW ➔ MIA (Mùa Bão Mùa Thu, Southwest Airlines)",
        "op_unique_carrier": "WN",
        "origin": "DFW",
        "dest": "MIA",
        "fl_date": "2024-09-18",
        "dep_time_str": "19:15",
        "crs_elapsed_time": 170,
        "distance": 1121,
        "desc": "Chuyến bay tối tới Florida trong mùa mưa bão bờ Vịnh Mexico."
    },
    {
        "id": "short_hop",
        "name": "SFO ➔ SEA (Chặng Ngắn Tây Bắc, United Airlines)",
        "op_unique_carrier": "UA",
        "origin": "SFO",
        "dest": "SEA",
        "fl_date": "2024-05-10",
        "dep_time_str": "12:00",
        "crs_elapsed_time": 135,
        "distance": 679,
        "desc": "Chặng bay thương mại tầm trung ven biển Tây Bắc."
    }
]
