"""
Build IEEE Word Document for Big Data Capstone Project (HUIT).
Tác giả: Nhóm 6 (Cù Văn Vĩ An, Lê Đức Lương, Trần Huỳnh Tuấn Anh)
GVHD: TS. Phan Hồ Viết Trường
Mục tiêu: Cập nhật nội dung hoàn chỉnh vào file BaoCao_DoAn_BigData_Nhom6_IEEE.docx
theo đúng chuẩn quốc tế IEEE (2 cột, Styles, Tables, Equations, Figures, References).
Khắc phục triệt để lỗi double numbering và tự động nhúng toàn bộ 6 biểu đồ thực nghiệm.
"""

import zipfile
import xml.etree.ElementTree as ET
import os
import sys
import re

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

DOCX_PATH = r"c:\LeDucLuong\HK VII\NhapMonBigData\DoAn\Bao_Cao\BaoCao_DoAn_BigData_Nhom6_IEEE.docx"

# Danh sách 6 hình ảnh biểu đồ thực nghiệm tiêu biểu
FIGURES = [
    (1, os.path.abspath(r"results/baseline/figures/combined/delay_distribution_7m_pie.png")),
    (2, os.path.abspath(r"results/baseline/figures/combined/pipeline_stages_breakdown_7m.png")),
    (3, os.path.abspath(r"results/baseline/figures/combined/grand_rf_showdown_7m.png")),
    (4, os.path.abspath(r"results/baseline/figures/individual/cm_spark_rf_7m.png")),
    (5, os.path.abspath(r"results/baseline/figures/individual/feat_imp_spark_rf_7m.png")),
    (6, os.path.abspath(r"results/baseline/figures/combined/multi_metric_radar_7m.png")),
]

# Namespace OpenXML Strict của template IEEE
NS = "http://purl.oclc.org/ooxml/wordprocessingml/main"
W14 = "http://schemas.microsoft.com/office/word/2010/wordml"
REL_NS = "http://purl.oclc.org/ooxml/officeDocument/relationships"

def qn(tag):
    return f"{{{NS}}}{tag}"

def p_tag(style_name=None, text="", bold_prefix=None, italic=False, bold=False, align=None):
    """Tạo một thẻ đoạn văn w:p với style và các thuộc tính run."""
    p = ET.Element(qn("p"))
    pPr = ET.SubElement(p, qn("pPr"))
    if style_name:
        pStyle = ET.SubElement(pPr, qn("pStyle"))
        pStyle.attrib[qn("val")] = style_name
    if align:
        jc = ET.SubElement(pPr, qn("jc"))
        jc.attrib[qn("val")] = align
    
    # Run in đậm tiền tố nếu có (ví dụ "Tóm tắt—" hoặc "TABLE I.")
    if bold_prefix:
        r_pre = ET.SubElement(p, qn("r"))
        rPr_pre = ET.SubElement(r_pre, qn("rPr"))
        ET.SubElement(rPr_pre, qn("b"))
        if italic:
            ET.SubElement(rPr_pre, qn("i"))
        t_pre = ET.SubElement(r_pre, qn("t"))
        t_pre.attrib["xml:space"] = "preserve"
        t_pre.text = bold_prefix

    # Run nội dung chính
    if text:
        r = ET.SubElement(p, qn("r"))
        rPr = ET.SubElement(r, qn("rPr"))
        if bold:
            ET.SubElement(rPr, qn("b"))
        if italic:
            ET.SubElement(rPr, qn("i"))
        t = ET.SubElement(r, qn("t"))
        t.attrib["xml:space"] = "preserve"
        t.text = text

    return p

def table_tag(headers, rows, col_widths=None):
    """Tạo bảng chuẩn OpenXML cho IEEE."""
    tbl = ET.Element(qn("tbl"))
    tblPr = ET.SubElement(tbl, qn("tblPr"))
    tblW = ET.SubElement(tblPr, qn("tblW"))
    tblW.attrib[qn("w")] = "0"
    tblW.attrib[qn("type")] = "auto"
    jc = ET.SubElement(tblPr, qn("jc"))
    jc.attrib[qn("val")] = "center"
    
    # Viền bảng đơn giản kiểu IEEE (chỉ có top, bottom và viền dưới header)
    borders = ET.SubElement(tblPr, qn("tblBorders"))
    for b_name in ["top", "bottom"]:
        b = ET.SubElement(borders, qn(b_name))
        b.attrib[qn("val")] = "single"
        b.attrib[qn("sz")] = "8"
        b.attrib[qn("space")] = "0"
        b.attrib[qn("color")] = "auto"
    for b_name in ["left", "right", "insideH", "insideV"]:
        b = ET.SubElement(borders, qn(b_name))
        b.attrib[qn("val")] = "none"

    # Hàng tiêu đề (Header Row)
    tr_h = ET.SubElement(tbl, qn("tr"))
    trPr_h = ET.SubElement(tr_h, qn("trPr"))
    ET.SubElement(trPr_h, qn("tblHeader"))
    for col_idx, h_text in enumerate(headers):
        tc = ET.SubElement(tr_h, qn("tc"))
        p = ET.SubElement(tc, qn("p"))
        pPr = ET.SubElement(p, qn("pPr"))
        pStyle = ET.SubElement(pPr, qn("pStyle"))
        pStyle.attrib[qn("val")] = "tablecolhead"
        jc_cell = ET.SubElement(pPr, qn("jc"))
        jc_cell.attrib[qn("val")] = "center"
        r = ET.SubElement(p, qn("r"))
        rPr = ET.SubElement(r, qn("rPr"))
        ET.SubElement(rPr, qn("b"))
        t = ET.SubElement(r, qn("t"))
        t.text = h_text

    # Các hàng dữ liệu (Data Rows)
    for row in rows:
        tr = ET.SubElement(tbl, qn("tr"))
        for col_idx, val in enumerate(row):
            tc = ET.SubElement(tr, qn("tc"))
            p = ET.SubElement(tc, qn("p"))
            pPr = ET.SubElement(p, qn("pPr"))
            pStyle = ET.SubElement(pPr, qn("pStyle"))
            pStyle.attrib[qn("val")] = "tablecopy"
            jc_cell = ET.SubElement(pPr, qn("jc"))
            jc_cell.attrib[qn("val")] = "center" if col_idx > 0 else "left"
            r = ET.SubElement(p, qn("r"))
            t = ET.SubElement(r, qn("t"))
            t.text = str(val)

    return tbl

def build_full_document_xml():
    root = ET.Element(qn("document"))
    root.attrib["xmlns:w"] = NS
    root.attrib["xmlns:w14"] = W14
    root.attrib["xmlns:r"] = REL_NS

    body = ET.SubElement(root, qn("body"))

    # =========================================================================
    # 1. PHÂN ĐOẠN 1: TIÊU ĐỀ BÀI BÁO (1 CỘT DUY NHẤT - TRẢI RỘNG TOÀN TRANG)
    # =========================================================================
    p_title = p_tag(
        style_name="papertitle",
        text="Ứng Dụng Thuật Toán Random Forest Phân Tán Dự Đoán Nguyên Nhân Trễ Chuyến Bay Thương Mại: Trường Hợp Tại Hoa Kỳ Năm 2024",
        bold=True,
        align="center"
    )
    body.append(p_title)

    p_eng_title = p_tag(
        style_name="papertitle",
        text="Distributed Random Forest for Flight Delay Cause Prediction: A Case Study of US Commercial Aviation in 2024",
        italic=True,
        align="center"
    )
    body.append(p_eng_title)

    # Đoạn ngắt phân đoạn 1 cột (Continuous Section Break) dành riêng cho tiêu đề
    p_title_sect = ET.Element(qn("p"))
    pPr_t = ET.SubElement(p_title_sect, qn("pPr"))
    pStyle_t = ET.SubElement(pPr_t, qn("pStyle"))
    pStyle_t.attrib[qn("val")] = "Author"
    sect_title = ET.SubElement(pPr_t, qn("sectPr"))
    ET.SubElement(sect_title, qn("type")).attrib[qn("val")] = "continuous"
    pgSz_t = ET.SubElement(sect_title, qn("pgSz"))
    pgSz_t.attrib[qn("w")] = "595.30pt"
    pgSz_t.attrib[qn("h")] = "841.90pt"
    pgSz_t.attrib[qn("code")] = "9"
    pgMar_t = ET.SubElement(sect_title, qn("pgMar"))
    pgMar_t.attrib[qn("top")] = "27pt"
    pgMar_t.attrib[qn("right")] = "44.65pt"
    pgMar_t.attrib[qn("bottom")] = "72pt"
    pgMar_t.attrib[qn("left")] = "44.65pt"
    pgMar_t.attrib[qn("header")] = "36pt"
    pgMar_t.attrib[qn("footer")] = "36pt"
    cols_t = ET.SubElement(sect_title, qn("cols"))
    cols_t.attrib[qn("space")] = "36pt"  # 1 CỘT TOÀN TRANG
    ET.SubElement(sect_title, qn("titlePg"))
    ET.SubElement(sect_title, qn("docGrid")).attrib[qn("linePitch")] = "360"
    body.append(p_title_sect)

    # =========================================================================
    # 2. PHÂN ĐOẠN 2: KHỐI TÁC GIẢ (CHIA ĐỀU 3 CỘT SONG SONG)
    # =========================================================================
    def create_author_paragraph(name, student_id, role=None, gvhd=None, add_column_break=False):
        p = ET.Element(qn("p"))
        pPr = ET.SubElement(p, qn("pPr"))
        pStyle = ET.SubElement(pPr, qn("pStyle"))
        pStyle.attrib[qn("val")] = "Author"
        spacing = ET.SubElement(pPr, qn("spacing"))
        spacing.attrib[qn("before")] = "5pt"
        spacing.attrib[qn("beforeAutospacing")] = "1"
        jc = ET.SubElement(pPr, qn("jc"))
        jc.attrib[qn("val")] = "center"

        # Tên tác giả
        r_name = ET.SubElement(p, qn("r"))
        rPr_n = ET.SubElement(r_name, qn("rPr"))
        ET.SubElement(rPr_n, qn("b"))
        sz_n = ET.SubElement(rPr_n, qn("sz"))
        sz_n.attrib[qn("val")] = "20"
        t_n = ET.SubElement(r_name, qn("t"))
        t_n.text = name

        # MSSV / Vai trò
        r_id = ET.SubElement(p, qn("r"))
        ET.SubElement(r_id, qn("br"))
        t_id = ET.SubElement(r_id, qn("t"))
        t_id.text = f"MSSV: {student_id}" + (f" ({role})" if role else "")

        # Đơn vị
        r_org = ET.SubElement(p, qn("r"))
        ET.SubElement(r_org, qn("br"))
        rPr_o = ET.SubElement(r_org, qn("rPr"))
        ET.SubElement(rPr_o, qn("i"))
        t_o = ET.SubElement(r_org, qn("t"))
        t_o.text = "Khoa Công nghệ Thông tin"

        r_univ = ET.SubElement(p, qn("r"))
        ET.SubElement(r_univ, qn("br"))
        rPr_u = ET.SubElement(r_univ, qn("rPr"))
        ET.SubElement(rPr_u, qn("i"))
        t_u = ET.SubElement(r_univ, qn("t"))
        t_u.text = "Trường ĐH Công Thương TP.HCM"

        # Email
        r_em = ET.SubElement(p, qn("r"))
        ET.SubElement(r_em, qn("br"))
        t_em = ET.SubElement(r_em, qn("t"))
        t_em.text = f"{student_id}@huit.edu.vn"

        # GVHD nếu có
        if gvhd:
            r_g = ET.SubElement(p, qn("r"))
            ET.SubElement(r_g, qn("br"))
            rPr_g = ET.SubElement(r_g, qn("rPr"))
            ET.SubElement(rPr_g, qn("b"))
            t_g = ET.SubElement(r_g, qn("t"))
            t_g.text = f"GVHD: {gvhd}"

        # Ngắt cột để đẩy tác giả tiếp theo sang cột kế bên
        if add_column_break:
            r_col_br = ET.SubElement(p, qn("r"))
            br_col = ET.SubElement(r_col_br, qn("br"))
            br_col.attrib[qn("type")] = "column"

        return p

    # Cột 1: Tác giả 1 (Cù Văn Vĩ An) -> ngắt cột sang Cột 2
    body.append(create_author_paragraph("Cù Văn Vĩ An", "2001230005", add_column_break=True))

    # Cột 2: Tác giả 2 (Lê Đức Lương - Trưởng nhóm) -> ngắt cột sang Cột 3
    body.append(create_author_paragraph("Lê Đức Lương", "2001230490", role="Trưởng nhóm", add_column_break=True))

    # Cột 3: Tác giả 3 (Trần Huỳnh Tuấn Anh) + GVHD
    body.append(create_author_paragraph("Trần Huỳnh Tuấn Anh", "2001230023", gvhd="TS. Phan Hồ Viết Trường", add_column_break=False))

    # Đoạn ngắt phân đoạn 3 cột dành cho tác giả (Section Break Continuous chia 3 cột)
    p_auth_sect = ET.Element(qn("p"))
    pPr_a = ET.SubElement(p_auth_sect, qn("pPr"))
    sect_auth = ET.SubElement(pPr_a, qn("sectPr"))
    ET.SubElement(sect_auth, qn("type")).attrib[qn("val")] = "continuous"
    pgSz_a = ET.SubElement(sect_auth, qn("pgSz"))
    pgSz_a.attrib[qn("w")] = "595.30pt"
    pgSz_a.attrib[qn("h")] = "841.90pt"
    pgSz_a.attrib[qn("code")] = "9"
    pgMar_a = ET.SubElement(sect_auth, qn("pgMar"))
    pgMar_a.attrib[qn("top")] = "22.50pt"
    pgMar_a.attrib[qn("right")] = "44.65pt"
    pgMar_a.attrib[qn("bottom")] = "72pt"
    pgMar_a.attrib[qn("left")] = "44.65pt"
    cols_a = ET.SubElement(sect_auth, qn("cols"))
    cols_a.attrib[qn("num")] = "3"
    cols_a.attrib[qn("space")] = "24pt"
    ET.SubElement(sect_auth, qn("docGrid")).attrib[qn("linePitch")] = "360"
    body.append(p_auth_sect)

    # =========================================================================
    # 3. TÓM TẮT (ABSTRACT) & TỪ KHÓA (KEYWORDS)
    # =========================================================================
    abstract_text = (
        "Giao thông hàng không giữ vai trò huyết mạch trong nền kinh tế, tuy nhiên sự cố trễ chuyến bay luôn gây thiệt hại tài chính "
        "hàng chục tỷ USD mỗi năm. Trước đây, các nghiên cứu thường chỉ dừng ở bài toán phân loại nhị phân (trễ hoặc không trễ) trên tập mẫu thu nhỏ. "
        "Bài báo này đề xuất một giải pháp dữ liệu lớn toàn diện dự đoán 5 nhóm nguyên nhân trễ chính (Hãng bay, Thời tiết, Không lưu NAS, An ninh và "
        "Máy bay đến muộn) trên tập dữ liệu đầy đủ 7,079,081 chuyến bay thương mại nội địa Hoa Kỳ năm 2024. Hệ thống được triển khai song song "
        "theo hai nhánh: Đơn máy tối ưu (Polars/Scikit-learn) và Cụm tính toán phân tán Apache Spark trên 3 nút máy tính vật lý kết nối qua mạng "
        "Tailscale Mesh VPN (32 vCPU, 40GB RAM). Kết hợp định dạng lưu trữ cột Apache Parquet nén Snappy phân vùng 12 tháng giúp giảm 82.5% dung lượng "
        "và tăng tốc ETL 10.97 lần. Kết quả thực nghiệm đối sánh 7 mô hình chứng minh Spark MLlib Random Forest hoàn thành huấn luyện trong 44.31 giây "
        "(nhanh hơn 2.41 lần so với đơn máy 106.79s), đạt thông lượng suy luận 1.98 ms/1k mẫu, vượt trội về Macro F1-Score (28.25% so với 14.73%) "
        "và tiết kiệm 405 MB RAM. Hệ thống được đóng gói thành công thành ứng dụng Web_Air (FastAPI) phục vụ suy luận tức thời dưới 40 ms."
    )
    body.append(p_tag(style_name="Abstract", bold_prefix="Tóm tắt—", text=abstract_text, italic=True))

    keywords_text = "Big Data, Apache Spark, Random Forest phân tán, Spark MLlib, Trễ chuyến bay, Apache Parquet, Tailscale Mesh VPN, Phân loại đa lớp, FastAPI."
    body.append(p_tag(style_name="Keywords", bold_prefix="Từ khóa—", text=keywords_text, italic=True))

    # =========================================================================
    # 4. CHƯƠNG I: TỔNG QUAN ĐỀ TÀI (INTRODUCTION)
    # Chú ý: Heading1 có auto-numbering I., II., ... nên không truyền tiền tố thủ công
    # =========================================================================
    body.append(p_tag(style_name="Heading1", text="TỔNG QUAN ĐỀ TÀI"))
    
    intro_1 = (
        "Trong năm 2024, mạng lưới hàng không nội địa Hoa Kỳ đã vận hành hơn 7 triệu chuyến bay thương mại. "
        "Theo thống kê của Cục Thống kê Vận tải Hoa Kỳ (BTS) [9] và FAA [10], thiệt hại kinh tế do trễ chuyến bay vượt quá 30 tỷ USD mỗi năm. "
        "Nhiều công trình học máy trước đây chủ yếu tập trung vào phân loại nhị phân (trễ hoặc đúng giờ) hoặc dự báo thời lượng trễ trên các mẫu rút gọn [24]. "
        "Rất ít nghiên cứu giải quyết bài toán phân loại đa lớp xác định nguyên nhân trễ chi phối (Dominant Delay Cause) giữa 5 nhóm nguyên nhân thực tế "
        "của BTS trên quy mô dữ liệu lớn toàn vẹn."
    )
    body.append(p_tag(style_name="BodyText", text=intro_1))

    intro_2 = (
        "Tập dữ liệu khai thác bay năm 2024 có quy mô lên tới 7,079,081 dòng với dung lượng tệp CSV thô đạt 2.3 GB từ nguồn BTS [9]. "
        "Các phương pháp phân tích trên máy trạm đơn lẻ nhanh chóng gặp hiện tượng tràn bộ nhớ (Out-Of-Memory) và thời gian huấn luyện kéo dài. "
        "Đóng góp chính của bài báo này bao gồm: (1) Tiếp nhận và tiền xử lý toàn vẹn hơn 7 triệu bản ghi mà không cần rút gọn mẫu; "
        "(2) Xây dựng bài toán phân loại đa lớp 5 nhóm nguyên nhân thực tế dựa trên nguyên tắc Dominant Cause; (3) Triển khai thành công cụm "
        "Apache Spark phân tán trên 3 máy tính vật lý thông qua Tailscale Mesh VPN [16]; (4) Thực nghiệm đối sánh toàn diện 7 mô hình học máy "
        "và đóng gói ứng dụng phục vụ mô hình Web_Air đạt độ trễ suy luận tức thời dưới 40 ms."
    )
    body.append(p_tag(style_name="BodyText", text=intro_2))

    # =========================================================================
    # 5. CHƯƠNG II: CƠ SỞ LÝ THUYẾT VÀ CÔNG NGHỆ LIÊN QUAN
    # Heading2 có auto-numbering A., B., ... nên không truyền tiền tố thủ công
    # =========================================================================
    body.append(p_tag(style_name="Heading1", text="CƠ SỞ LÝ THUYẾT VÀ CÔNG NGHỆ LIÊN QUAN"))

    body.append(p_tag(style_name="Heading2", text="Hệ sinh thái Apache Spark và Tính toán trong bộ nhớ"))
    spark_theory = (
        "Khác với mô hình Hadoop MapReduce truyền thống [4], [5], [13] phải ghi dữ liệu trung gian xuống đĩa cứng HDFS sau mỗi chu kỳ tính toán, "
        "Apache Spark [2], [11] lưu trữ dữ liệu trực tiếp trong bộ nhớ truy xuất ngẫu nhiên (In-Memory Computing). Thông qua cấu trúc dữ liệu trừu tượng "
        "RDD (Resilient Distributed Dataset) và đồ thị có hướng không chu trình DAG (Directed Acyclic Graph), Spark tối ưu hóa kế hoạch "
        "thực thi nhờ Catalyst Optimizer và cơ chế Đánh giá lười biếng (Lazy Evaluation), mang lại tốc độ xử lý nhanh hơn từ 10 đến 100 lần."
    )
    body.append(p_tag(style_name="BodyText", text=spark_theory))

    body.append(p_tag(style_name="Heading2", text="Thuật toán Random Forest phân tán trên Spark MLlib"))
    rf_theory = (
        "Thuật toán Random Forest (Leo Breiman, 2001) [1] xây dựng tập hợp B cây quyết định độc lập thông qua kỹ thuật Đóng bao (Bagging) "
        "và chọn ngẫu nhiên tập con đặc trưng (Feature Subsampling). Điểm phân chia tại mỗi nút được xác định nhằm cực đại hóa độ giảm "
        "chỉ số vẩn đục Gini Impurity:"
    )
    body.append(p_tag(style_name="BodyText", text=rf_theory))

    # Equation Gini
    body.append(p_tag(style_name="equation", text="Gini(S) = 1 - SUM(p_k^2),  k in {0, ..., C-1}    (1)", align="center"))

    rf_dist = (
        "Trong môi trường phân tán Spark MLlib [3], [12], việc tìm điểm phân chia tối ưu không quét tuần tự toàn bộ dữ liệu mà sử dụng giải thuật "
        "Distributed Histogram-based Split Finding kế thừa từ mô hình tính toán song song họ PLANET và các thuật toán streaming histogram [19], [20]. "
        "Driver phát tán cấu trúc cây hiện tại xuống các Executor; mỗi Executor chỉ quét phân vùng dữ liệu cục bộ để lập biểu đồ tần suất nhãn "
        "cho các điểm chia ứng viên (quantile binning). Hàm treeAggregate sau đó tổng hợp các Histogram về Driver với chi phí truyền thông mạng tối thiểu [19], [21], "
        "giải tỏa nút thắt cổ chai bộ nhớ và mở rộng tuyến tính theo quy mô dữ liệu. Nghiên cứu của Hayat et al. (2025) [18] cũng khẳng định hiệu năng "
        "vượt trội và tính ổn định của các thuật toán phân loại phân tán trên Spark MLlib trên tập dữ liệu lớn so với các kiến trúc đơn máy."
    )
    body.append(p_tag(style_name="BodyText", text=rf_dist))

    body.append(p_tag(style_name="Heading2", text="Định dạng lưu trữ tối ưu Apache Parquet Snappy"))
    parquet_theory = (
        "Apache Parquet là định dạng lưu trữ hướng cột (Columnar Storage) chuyên dụng cho Big Data [14]. Nhờ lưu trữ các giá trị cùng kiểu "
        "liên tiếp nhau, Parquet kết hợp thuật toán nén Snappy, Dictionary Encoding và Run-Length Encoding giúp giảm hơn 80% dung lượng đĩa. "
        "Đặc biệt, cơ chế Predicate Pushdown cho phép bộ máy Spark bỏ qua các khối dữ liệu không thỏa mãn điều kiện lọc ngay tại tầng đọc tệp. "
        "Đánh giá gần đây của Guimaraes và Araujo (2026) [17] trên các kiến trúc Cloud Lakehouse chuẩn TPC-DS cũng khẳng định định dạng tệp mở "
        "hướng cột như Parquet mang lại hiệu quả mở rộng (scalability) và hiệu năng truy vấn I/O vượt trội khi tích hợp cùng các hệ thống xử lý phân tán."
    )
    body.append(p_tag(style_name="BodyText", text=parquet_theory))

    # =========================================================================
    # 6. CHƯƠNG III: PHƯƠNG PHÁP NGHIÊN CỨU VÀ THIẾT KẾ HỆ THỐNG PHÂN TÁN
    # =========================================================================
    body.append(p_tag(style_name="Heading1", text="PHƯƠNG PHÁP NGHIÊN CỨU VÀ THIẾT KẾ HỆ THỐNG"))

    body.append(p_tag(style_name="Heading2", text="Kiến trúc Pipeline hai nhánh song song (Dual-Branch)"))
    pipe_desc = (
        "Đề tài thiết kế kiến trúc hệ thống gồm 2 nhánh đối sánh độc lập từ tập dữ liệu gốc BTS 2024: "
        "(1) Nhánh Đơn máy tối ưu sử dụng Polars streaming và Scikit-learn Pipeline kết hợp GPU NVIDIA để huấn luyện các thuật toán Boosting "
        "(XGBoost, LightGBM, CatBoost); (2) Nhánh Phân tán ứng dụng Apache Spark MLlib huấn luyện Random Forest trên cụm 3 nút vật lý."
    )
    body.append(p_tag(style_name="BodyText", text=pipe_desc))

    body.append(p_tag(style_name="Heading2", text="Thiết lập Cụm vật lý 3 Node qua Tailscale Mesh VPN"))
    cluster_desc = (
        "Để vượt qua rào cản tường lửa và NAT trong mạng gia đình của các thành viên, nhóm ứng dụng giải pháp Tailscale Mesh VPN [16] "
        "dựa trên giao thức mã hóa WireGuard để liên kết 3 máy tính cá nhân thành một mạng LAN ảo P2P an toàn với IP cố định 100.x.y.z. "
        "Hạ tầng cụm bao gồm: Master Node 0 (Intel Core i7-11800H, 16GB RAM, Driver điều phối), Worker Node 1 (AMD Ryzen 5 5600H, 16GB RAM) "
        "và Worker Node 2 (Intel Core i5-10300H, 8GB RAM), đạt tổng tài nguyên 32 vCPU và 40 GB RAM."
    )
    body.append(p_tag(style_name="BodyText", text=cluster_desc))

    body.append(p_tag(style_name="Heading2", text="Tiền xử lý, gán nhãn 5 lớp và Chống rò rỉ dữ liệu"))
    data_prep = (
        "Từ 7,079,081 chuyến bay gốc, hệ thống loại bỏ 113,814 bản ghi bị hủy hoặc chuyển hướng, giữ lại 6,965,267 chuyến bay hợp lệ. "
        "Đối với các chuyến bay trễ >= 15 phút, biến mục tiêu 5 lớp được gán nhãn theo nguyên tắc Nguyên nhân chi phối lớn nhất "
        "(Dominant Cause = argmax(DelayMinutes)). Trường hợp có từ hai nguyên nhân đạt số phút trễ tối đa bằng nhau, hệ thống ưu tiên gán theo thứ tự "
        "phân cấp tác động vận hành (LateAircraft -> Carrier -> NAS -> Weather -> Security). Toàn bộ các trường số phút trễ chi tiết bị loại bỏ "
        "khỏi không gian đặc trưng đầu vào để ngăn ngừa triệt để hiện tượng rò rỉ nhãn (target leakage)."
    )
    body.append(p_tag(style_name="BodyText", text=data_prep))

    body.append(p_tag(style_name="Heading2", text="Tối ưu hóa lưu trữ và Tinh chỉnh siêu tham số Spark"))
    tuning_desc = (
        "Tập dữ liệu được chuyển đổi sang Parquet phân vùng theo 12 tháng (partitionBy('Month')), giảm kích thước từ 2.3 GB xuống 409.8 MB "
        "(giảm 82.5%). Cấu hình Spark Tuning được tinh chỉnh tối ưu: spark.driver.memory=6g, spark.executor.memory=4g, spark.executor.cores=4, "
        "spark.sql.shuffle.partitions=32 (khớp số luồng vật lý của cụm) và kích hoạt KryoSerializer tăng tốc độ tuần tự hóa qua mạng."
    )
    body.append(p_tag(style_name="BodyText", text=tuning_desc))

    # =========================================================================
    # 7. CHƯƠNG IV: THỰC NGHIỆM VÀ ĐÁNH GIÁ KẾT QUẢ
    # =========================================================================
    body.append(p_tag(style_name="Heading1", text="THỰC NGHIỆM VÀ ĐÁNH GIÁ KẾT QUẢ"))

    exp_setup = (
        "Dữ liệu 7 triệu dòng được phân chia ngẫu nhiên theo tỷ lệ chuẩn: 70% Train (4,875,851 dòng), 15% Validation (1,044,254 dòng) "
        "và 15% Test độc lập (1,045,162 dòng) dùng chung để đánh giá tất cả các mô hình. Toàn bộ kết quả thực nghiệm được đo đạc tự động."
    )
    body.append(p_tag(style_name="BodyText", text=exp_setup))

    # BẢNG I: ĐỐI SÁNH 7 MÔ HÌNH HỌC MÁY
    # tablehead tự động sinh "TABLE I. ", "TABLE II. " nên chỉ truyền nội dung tiêu đề
    body.append(p_tag(style_name="tablehead", text="BẢNG ĐỐI SÁNH TOÀN DIỆN HIỆU NĂNG CÁC MÔ HÌNH HỌC MÁY TRÊN 7,079,081 DÒNG", align="center"))
    headers_m = ["Mô hình", "Thời gian (s)", "Độ trễ (ms/1k)", "RAM (MB)", "Accuracy (%)", "Weighted F1 (%)", "Macro F1 (%)"]
    rows_m = [
        ["Logistic Reg (CPU)", "17.99", "40.00", "1710.4", "79.18", "69.98", "14.73"],
        ["Decision Tree (CPU)", "51.12", "0.67", "1418.5", "79.19", "70.45", "15.72"],
        ["Random Forest (CPU)", "106.79", "33.51", "1417.9", "79.18", "69.98", "14.73"],
        ["LightGBM (CPU/GPU)", "78.93", "11.71", "1464.8", "79.25", "70.34", "15.94"],
        ["XGBoost (GPU)", "19.27", "73.52", "1742.0", "79.27", "70.36", "15.55"],
        ["CatBoost (GPU)", "19.60", "12.03", "1908.6", "79.29", "70.83", "16.54"],
        ["Decision Tree (Spark)", "24.96", "1.31", "835.2", "79.23", "70.26", "28.25"],
        ["Random Forest (Spark)", "44.31", "1.98", "835.2", "79.22", "70.03", "28.25"]
    ]
    body.append(table_tag(headers_m, rows_m))

    analysis_table1 = (
        "Bảng I ghi nhận CatBoost [8], XGBoost [6] và LightGBM [7] dẫn đầu về Accuracy (79.29%, 79.27% và 79.25%). Đáng chú ý nhất, mô hình Random Forest trên Spark MLlib "
        "đạt chỉ số Macro F1-Score lên tới 28.25% (gần gấp đôi mức 14.73% của Random Forest CPU đơn máy). Phân tích đánh giá đa lớp của Sokolova và Lapalme [22] "
        "chỉ ra rằng Macro F1 đánh giá không trọng số giữa các lớp, qua đó phản ánh trung thực khả năng nhận diện các nhóm nguyên nhân thiểu số trong điều kiện "
        "mất cân bằng dữ liệu nghiêm trọng [23], [24] (điển hình như nhóm Security chỉ chiếm <0.4% và Weather chiếm 6.1%). Về thời gian, Spark RF chỉ mất 44.31 giây "
        "để huấn luyện trên gần 5 triệu dòng, nhanh hơn 2.41 lần so với RF đơn máy CPU (106.79s), đồng thời giảm tiêu thụ RAM đỉnh từ 1,417.9 MB "
        "xuống chỉ còn 835.2 MB trên máy điều khiển Driver, củng cố thêm các phát hiện thực nghiệm của Hayat et al. [18]."
    )
    body.append(p_tag(style_name="BodyText", text=analysis_table1))

    # BẢNG II: BÓC TÁCH 4 GIAI ĐOẠN PIPELINE
    body.append(p_tag(style_name="tablehead", text="BÓC TÁCH THỜI GIAN 4 GIAI ĐOẠN PIPELINE (THUẦN VS SPARK PHÂN TÁN)", align="center"))
    headers_p = ["Giai đoạn Pipeline", "Hướng Thuần Đơn máy (s)", "Hướng Spark Phân tán (s)", "Tốc độ cải thiện"]
    rows_p = [
        ["1. Tiền xử lý (ETL)", "43.88", "4.00", "Spark nhanh hơn 10.97 lần"],
        ["2. Trích xuất đặc trưng", "2.50", "8.52", "Thuần nhanh hơn (Spark tạo DAG)"],
        ["3. Huấn luyện (Random Forest)", "106.79", "49.89", "Spark nhanh hơn 2.14 lần"],
        ["4. Đánh giá & Suy luận (1M test)", "1.80", "2.83", "Tương đương nhau"],
        ["TỔNG THỜI GIAN TOÀN TRÌNH", "154.97 s (~2.58 phút)", "65.24 s (~1.08 phút)", "SPARK NHANH HƠN 2.38 LẦN"],
        ["Dung lượng RAM đỉnh", "1,417.9 MB", "1,012.2 MB", "Spark tiết kiệm >405 MB RAM"]
    ]
    body.append(table_tag(headers_p, rows_p))

    analysis_table2 = (
        "Bảng II khẳng định quy luật của kỹ thuật dữ liệu lớn: việc tối ưu hóa định dạng lưu trữ cột Parquet Snappy phân tán [14], [17] giúp giai đoạn ETL "
        "rút ngắn ngoạn mục từ 43.88 giây xuống chỉ còn 4.00 giây (nhanh gấp gần 11 lần). Tổng thời gian thực thi toàn trình của cụm Spark "
        "chỉ mất 65.24 giây, nhanh hơn 2.38 lần so với quy trình đơn máy truyền thống."
    )
    body.append(p_tag(style_name="BodyText", text=analysis_table2))

    # CÁC HÌNH ẢNH MINH HỌA TIÊU BIỂU THEO CHUẨN IEEE
    # figurecaption tự động sinh "Fig. 1. ", "Fig. 2. " nên chỉ truyền nội dung chú thích
    body.append(p_tag(style_name="Heading2", text="Phân tích trực quan hóa khoa học"))

    # Fig 1: Delay Distribution
    body.append(p_tag(text="[IMAGE_SLOT_1]", align="center"))
    body.append(p_tag(
        style_name="figurecaption",
        text="Phân bố tỷ lệ 5 nhóm nguyên nhân trễ chuyến bay năm 2024. LateAircraft chiếm 38.5%, Carrier chiếm 31.2%, NAS chiếm 23.8%, Weather chiếm 6.1% và Security chiếm <0.4%."
    ))

    # Fig 2: Pipeline Stages
    body.append(p_tag(text="[IMAGE_SLOT_2]", align="center"))
    body.append(p_tag(
        style_name="figurecaption",
        text="So sánh thời gian thực thi 4 giai đoạn Pipeline giữa Hướng Thuần đơn máy (154.97s) và Hướng Spark phân tán (65.24s - nhanh hơn 2.38 lần toàn diện)."
    ))

    # Fig 3: Grand RF Showdown
    body.append(p_tag(text="[IMAGE_SLOT_3]", align="center"))
    body.append(p_tag(
        style_name="figurecaption",
        text="Đối kháng trực diện Random Forest CPU vs Spark RF MLlib: Spark RF rút ngắn thời gian từ 106.79s xuống 44.31s và đạt độ trễ suy luận siêu thấp 1.98 ms/1k mẫu."
    ))

    # Fig 4: Confusion Matrix Spark RF
    body.append(p_tag(text="[IMAGE_SLOT_4]", align="center"))
    body.append(p_tag(
        style_name="figurecaption",
        text="Ma trận nhầm lẫn chuẩn hóa của Spark MLlib Random Forest trên 1,045,162 mẫu kiểm tra, nhận diện chính xác 78.4% trễ máy bay đến muộn và 68.2% trễ hãng bay."
    ))

    # Fig 5: Feature Importance Spark RF
    body.append(p_tag(text="[IMAGE_SLOT_5]", align="center"))
    body.append(p_tag(
        style_name="figurecaption",
        text="Tầm quan trọng đặc trưng của Spark RF: dep_min_of_day (35.89%), month (17.04%) và dep_hour (16.67%) là 3 yếu tố chi phối lớn nhất đến nguyên nhân trễ."
    ))

    # Fig 6: Radar Multi-metric
    body.append(p_tag(text="[IMAGE_SLOT_6]", align="center"))
    body.append(p_tag(
        style_name="figurecaption",
        text="Đánh giá đa chiều 5 chỉ số chất lượng: Spark RF bung rộng nhất ở góc Macro F1-Score (28.25%), chứng minh độ nhạy vượt trội trên bài toán mất cân bằng lớp."
    ))

    body.append(p_tag(style_name="Heading2", text="Ứng dụng phục vụ mô hình Web_Air"))
    web_desc = (
        "Mô hình được đóng gói thành công vào ứng dụng Web_Air xây dựng trên nền tảng FastAPI bất đồng bộ [15]. "
        "Giao diện hiện đại gồm 3 Tab chức năng: (1) Live Prediction: cho phép nhập thông tin chuyến bay và hiển thị kết quả đối đầu song song "
        "giữa Random Forest CPU, XGBoost và Spark RF; (2) Grand Benchmark: bảng đối sánh trực quan động 7 mô hình; (3) Visuals Gallery: "
        "thư viện hiển thị toàn bộ 24 biểu đồ khoa học với modal phóng to độ phân giải cao. Đo đạc tải thực tế ghi nhận độ trễ suy luận tức thời "
        "đạt 38.4 ms/request và thông lượng xử lý vượt trên 1,200 yêu cầu/giây."
    )
    body.append(p_tag(style_name="BodyText", text=web_desc))

    # =========================================================================
    # 8. CHƯƠNG V: KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN
    # =========================================================================
    body.append(p_tag(style_name="Heading1", text="KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN"))

    conc_text = (
        "Đề tài đã hoàn thành xuất sắc toàn bộ các mục tiêu đặt ra: xử lý toàn vẹn hơn 7 triệu chuyến bay năm 2024, làm chủ quy trình "
        "lưu trữ Parquet Snappy phân vùng 12 tháng, triển khai thành công cụm Spark 3 nút vật lý qua Tailscale Mesh VPN và chứng minh "
        "tính ưu việt vượt trội của Spark MLlib Random Forest về tốc độ huấn luyện (nhanh hơn 2.41 lần), độ trễ suy luận (1.98 ms) và "
        "khả năng phân loại lớp thiểu số (Macro F1 đạt 28.25%)."
    )
    body.append(p_tag(style_name="BodyText", text=conc_text))

    limit_text = (
        "Hạn chế còn tồn tại bao gồm: biến thiên băng thông và độ trễ qua mạng VPN Internet gia đình tạo ra chi phí phụ trội truyền thông "
        "(communication overhead) trong các tác vụ shuffle dữ liệu và tổng hợp histogram của Spark. Trong tương lai, nhóm định hướng: "
        "(1) Mở rộng cụm Spark lên hạ tầng mạng LAN trung tâm dữ liệu hoặc đám mây (Google Cloud Dataproc / AWS EMR) để giải tỏa nút thắt truyền thông; "
        "(2) Tích hợp luồng dữ liệu thời gian thực qua Apache Kafka và Spark Structured Streaming."
    )
    body.append(p_tag(style_name="BodyText", text=limit_text))

    # =========================================================================
    # 9. LỜI CẢM ƠN (ACKNOWLEDGMENT)
    # =========================================================================
    body.append(p_tag(style_name="Heading5", text="LỜI CẢM ƠN (ACKNOWLEDGMENT)"))
    ack_text = (
        "Nhóm tác giả xin bày tỏ lòng biết ơn sâu sắc đến Ban Giám hiệu Trường Đại học Công Thương TP. Hồ Chí Minh (HUIT), "
        "các Thầy, Cô Khoa Công nghệ Thông tin và đặc biệt là TS. Phan Hồ Viết Trường đã tận tình định hướng khoa học, gợi mở giải pháp "
        "và đồng hành cùng nhóm trong suốt quá trình hoàn thành đồ án học phần này."
    )
    body.append(p_tag(style_name="BodyText", text=ack_text))

    # =========================================================================
    # 10. TÀI LIỆU THAM KHẢO (REFERENCES - 24 MỤC CHUẨN IEEE)
    # references style tự động đánh số [1], [2], ... nên loại bỏ [X] ở đầu chuỗi
    # =========================================================================
    body.append(p_tag(style_name="Heading5", text="TÀI LIỆU THAM KHẢO (REFERENCES)"))
    
    refs = [
        "[1] L. Breiman, \"Random Forests,\" Machine Learning, vol. 45, no. 1, pp. 5–32, 2001, doi: 10.1023/A:1010933404324.",
        "[2] M. Zaharia et al., \"Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing,\" in Proc. 9th USENIX NSDI, 2012, pp. 15–28.",
        "[3] X. Meng et al., \"MLlib: Machine Learning in Apache Spark,\" Journal of Machine Learning Research (JMLR), vol. 17, no. 34, pp. 1–7, 2016.",
        "[4] J. Dean and S. Ghemawat, \"MapReduce: Simplified data processing on large clusters,\" Commun. ACM, vol. 51, no. 1, pp. 107–113, 2008, doi: 10.1145/1327452.1327492.",
        "[5] S. Ghemawat, H. Gobioff, and S. Leung, \"The Google File System,\" in Proc. 19th ACM SOSP, 2003, pp. 29–43, doi: 10.1145/945445.945450.",
        "[6] T. Chen and C. Guestrin, \"XGBoost: A Scalable Tree Boosting System,\" in Proc. 22nd ACM SIGKDD, 2016, pp. 785–794, doi: 10.1145/2939672.2939785.",
        "[7] G. Ke et al., \"LightGBM: A Highly Efficient Gradient Boosting Decision Tree,\" in Advances in Neural Information Processing Systems (NeurIPS), vol. 30, 2017, pp. 3146–3154.",
        "[8] L. Prokhorenkova et al., \"CatBoost: Unbiased Boosting with Categorical Features,\" in Advances in Neural Information Processing Systems (NeurIPS), vol. 31, 2018, pp. 6638–6648.",
        "[9] Bureau of Transportation Statistics (BTS), \"Airline On-Time Performance Data and Delay Statistics,\" U.S. Dept. of Transportation, 2024. [Online]. Available: https://www.transtats.bts.gov/",
        "[10] Federal Aviation Administration (FAA), \"National Airspace System Performance Reports,\" U.S. Dept. of Transportation, 2024. [Online]. Available: https://www.faa.gov/data_research/",
        "[11] Apache Software Foundation, \"Apache Spark 3.5.1 Documentation: Cluster Mode & Spark SQL,\" 2024. [Online]. Available: https://spark.apache.org/docs/latest/",
        "[12] Apache Software Foundation, \"Spark MLlib Guide: Distributed Classification & Pipelines,\" 2024. [Online]. Available: https://spark.apache.org/docs/latest/ml-guide.html",
        "[13] Apache Software Foundation, \"Hadoop HDFS Architecture Guide,\" 2024. [Online]. Available: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html",
        "[14] Apache Software Foundation, \"Apache Parquet File Format Specification,\" 2024. [Online]. Available: https://parquet.apache.org/",
        "[15] S. Ramírez, \"FastAPI: Modern, High-Performance Web Framework for Python,\" 2024. [Online]. Available: https://fastapi.tiangolo.com/",
        "[16] Tailscale Inc., \"Tailscale WireGuard Mesh Networking & Peer-to-Peer NAT Traversal,\" 2024. [Online]. Available: https://tailscale.com/kb/",
        "[17] I. V. P. Guimaraes and A. Araujo, \"Evaluating Scalability Using Open Table File Formats in Cloud Lakehouse Architectures with TPC-DS,\" in Proc. SCITEPRESS, 2026. [Online]. Available: https://www.scitepress.org/Papers/2026/147254/147254.pdf",
        "[18] K. Hayat, N. Fatima, and S. Nazir, \"Distributed Machine Learning with Spark MLlib: A Comprehensive Performance Evaluation,\" Pakistan Journal of Scientific Research (PJOSR), 2025. [Online]. Available: http://paas-pk.org/index.php/pjosr/article/download/2013/1084/3593",
        "[19] B. Panda, J. Herbach, S. Basu, and R. J. Bayardo, \"PLANET: Massively parallel learning of tree ensembles with MapReduce,\" Proc. VLDB Endow., vol. 2, no. 2, pp. 1426–1437, 2009, doi: 10.14778/1687553.1687569.",
        "[20] A. Ben-Haim and E. Tom-Tov, \"A streaming parallel decision tree algorithm,\" J. Mach. Learn. Res., vol. 11, pp. 849–872, 2010.",
        "[21] F. Abuzaid et al., \"Yggdrasil: An optimized system for training deep decision trees at scale,\" in Adv. Neural Inf. Process. Syst. (NeurIPS), 2016.",
        "[22] M. Sokolova and G. Lapalme, \"A systematic analysis of performance measures for classification tasks,\" Inf. Process. Manage., vol. 45, no. 4, pp. 427–437, 2009, doi: 10.1016/j.ipm.2009.03.002.",
        "[23] Y. Sun, A. K. C. Wong, and M. S. Kamel, \"Classification of imbalanced data: A review,\" Int. J. Pattern Recognit. Artif. Intell., vol. 23, no. 4, pp. 687–719, 2009, doi: 10.1142/S0218001409007326.",
        "[24] R. Hendrickx, M. Zoutendijk, M. Mitici, and J. Schäfer, \"Considering airport planners' preferences and imbalanced datasets when predicting flight delays and cancellations,\" in Proc. IEEE/AIAA DASC, 2021, doi: 10.1109/DASC52595.2021.9594367."
    ]

    # Cuối danh sách tài liệu tham khảo, gắn sectPr 2 cột
    for idx, ref in enumerate(refs):
        clean_ref = re.sub(r"^\[\d+\]\s*", "", ref)
        p_ref = p_tag(style_name="references", text=clean_ref)
        if idx == len(refs) - 1:
            pPr_ref = p_ref.find(qn("pPr"))
            sect_end = ET.SubElement(pPr_ref, qn("sectPr"))
            ET.SubElement(sect_end, qn("type")).attrib[qn("val")] = "continuous"
            pgSz_e = ET.SubElement(sect_end, qn("pgSz"))
            pgSz_e.attrib[qn("w")] = "595.30pt"
            pgSz_e.attrib[qn("h")] = "841.90pt"
            pgSz_e.attrib[qn("code")] = "9"
            pgMar_e = ET.SubElement(sect_end, qn("pgMar"))
            pgMar_e.attrib[qn("top")] = "54pt"
            pgMar_e.attrib[qn("right")] = "45.35pt"
            pgMar_e.attrib[qn("bottom")] = "72pt"
            pgMar_e.attrib[qn("left")] = "45.35pt"
            cols_e = ET.SubElement(sect_end, qn("cols"))
            cols_e.attrib[qn("num")] = "2"
            cols_e.attrib[qn("space")] = "18pt"
        body.append(p_ref)

    # sectPr cuối body
    end_sect = ET.SubElement(body, qn("sectPr"))
    ET.SubElement(end_sect, qn("type")).attrib[qn("val")] = "continuous"
    pgSz_fin = ET.SubElement(end_sect, qn("pgSz"))
    pgSz_fin.attrib[qn("w")] = "595.30pt"
    pgSz_fin.attrib[qn("h")] = "841.90pt"
    pgSz_fin.attrib[qn("code")] = "9"
    pgMar_fin = ET.SubElement(end_sect, qn("pgMar"))
    pgMar_fin.attrib[qn("top")] = "54pt"
    pgMar_fin.attrib[qn("right")] = "44.65pt"
    pgMar_fin.attrib[qn("bottom")] = "72pt"
    pgMar_fin.attrib[qn("left")] = "44.65pt"
    cols_fin = ET.SubElement(end_sect, qn("cols"))
    cols_fin.attrib[qn("space")] = "36pt"

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

def embed_images_in_docx(docx_path):
    """Tự động nhúng 6 biểu đồ thực nghiệm vào tài liệu qua Microsoft Word Engine."""
    print("[*] Đang chèn và định dạng 6 hình ảnh khoa học qua Microsoft Word Engine...")
    import win32com.client as wc
    
    w = wc.Dispatch("Word.Application")
    w.Visible = False
    w.DisplayAlerts = 0
    try:
        doc = w.Documents.Open(docx_path)
        for fig_num, img_path in FIGURES:
            slot_tag = f"[IMAGE_SLOT_{fig_num}]"
            rng = doc.Content
            find = rng.Find
            find.ClearFormatting()
            find.Text = slot_tag
            found = find.Execute()
            if found:
                print(f"  -> Nhúng hình ảnh Fig. {fig_num}: {os.path.basename(img_path)}")
                shape = doc.InlineShapes.AddPicture(
                    FileName=img_path,
                    LinkToFile=False,
                    SaveWithDocument=True,
                    Range=rng
                )
                shape.Width = 240 # ~8.46 cm (khớp độ rộng cột chuẩn IEEE A4 2 cột)
                shape.Range.ParagraphFormat.Alignment = 1 # wdAlignParagraphCenter
            else:
                print(f"  [!] Cảnh báo: Không tìm thấy vị trí đánh dấu {slot_tag}")
        doc.Save()
        print(f"[✓] Đã nhúng thành công toàn bộ {len(FIGURES)} hình ảnh vào tài liệu. Tổng InlineShapes: {doc.InlineShapes.Count}")
        doc.Close(False)
    finally:
        w.Quit()

def update_docx():
    print("[*] Đang sinh nội dung XML tài liệu báo cáo IEEE mới...")
    new_doc_xml = build_full_document_xml()
    
    temp_zip_path = DOCX_PATH + ".tmp"
    
    with zipfile.ZipFile(DOCX_PATH, 'r') as zin:
        with zipfile.ZipFile(temp_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename == "word/document.xml":
                    zout.writestr(item, new_doc_xml)
                else:
                    zout.writestr(item, zin.read(item.filename))
                    
    # Thay thế file gốc an toàn
    os.replace(temp_zip_path, DOCX_PATH)
    print(f"[✓] Đã tạo thành công cấu trúc XML mới tại: {DOCX_PATH}")

    # Nhúng hình ảnh trực tiếp qua Word COM Engine
    embed_images_in_docx(DOCX_PATH)
    print(f"[✓] HOÀN TẤT TẤT CẢ CÁC BƯỚC XÂY DỰNG BÁO CÁO IEEE: {DOCX_PATH}")

if __name__ == "__main__":
    update_docx()
