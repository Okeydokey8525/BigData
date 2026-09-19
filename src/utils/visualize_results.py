"""Module: visualize_results.py
Description: Hệ thống trực quan hóa khoa học toàn diện phục vụ Đồ án Big Data (HUIT).
Hỗ trợ vẽ:
  1. Biểu đồ tròn/donut phân bố nguyên nhân trễ & tỷ trọng thời gian pipeline.
  2. Biểu đồ cột phân đoạn (Stacked Bar) bóc tách thời gian 4 giai đoạn & End-to-End.
  3. Biểu đồ đối kháng trực diện Random Forest CPU vs Spark RF.
  4. Biểu đồ riêng từng mô hình: Confusion Matrix Heatmap & Feature Importance Bar.
  5. Biểu đồ ghép tổng hợp đa chỉ số (Grand Comparison Dashboard).
Tác giả: Nhóm 6 - Nhập môn Big Data (HUIT)
"""

import os
import sys
import io
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Đảm bảo hiển thị tiếng Việt trên Windows console không bị UnicodeEncodeError
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thiết lập phong cách đồ thị chuẩn khoa học
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Segoe UI', 'Arial', 'sans-serif']
plt.rcParams['axes.edgecolor'] = '#CBD5E1'
plt.rcParams['axes.linewidth'] = 0.8

# Bảng màu chuẩn mực đồng bộ toàn hệ thống
CAUSE_COLORS = {
    'OnTime_or_MinorDelay': '#10B981', # Xanh ngọc lục bảo
    'Carrier': '#3B82F6',              # Xanh dương
    'Weather': '#F59E0B',              # Vàng hổ phách
    'NAS': '#8B5CF6',                  # Tím thạch anh
    'Security': '#EC4899',             # Hồng
    'LateAircraft': '#EF4444'          # Đỏ cam
}

STAGE_COLORS = {
    'ETL': '#0284C7',                  # Xanh lam đậm
    'Feature': '#0D9488',              # Xanh lam ngọc
    'Training': '#F97316',             # Cam rực
    'Evaluation': '#8B5CF6'            # Tím
}

# ==============================================================================
# 1. BIỂU ĐỒ TRÒN & DONUT (PIE / DONUT CHARTS)
# ==============================================================================

def plot_delay_distribution_pie(data_counts, output_path, title="Phân Bố 6 Nguyên Nhân Trễ Chuyến Bay Thương Mại (2024)"):
    """Vẽ biểu đồ Donut hiện đại phân bố các nhãn trễ."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 7), subplot_kw=dict(aspect="equal"))

    labels = list(data_counts.keys())
    counts = list(data_counts.values())
    total = sum(counts)
    colors = [CAUSE_COLORS.get(l, '#94A3B8') for l in labels]

    wedges, texts, autotexts = ax.pie(
        counts,
        autopct='%1.1f%%',
        pctdistance=0.75,
        colors=colors,
        startangle=140,
        wedgeprops=dict(width=0.45, edgecolor='white', linewidth=2)
    )

    plt.setp(autotexts, size=9.5, weight="bold", color="white")
    
    # Legend chi tiết với số lượng mẫu
    legend_labels = [f"{l}: {c:,} ({c/total*100:.1f}%)" for l, c in zip(labels, counts)]
    ax.legend(
        wedges,
        legend_labels,
        title="Nhãn Mục Tiêu (Tổng: {:,} chuyến)".format(total),
        loc="center left",
        bbox_to_anchor=(0.95, 0, 0.5, 1),
        fontsize=9.5,
        title_fontsize=10.5,
        frameon=True,
        facecolor='#F8FAFC'
    )

    ax.set_title(title, fontsize=13, weight='bold', pad=20, color='#0F172A')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ tròn phân bố nhãn: {output_path}")

def plot_pipeline_time_ratio_pie(stages_dict, output_path, title="Tỷ Trọng Thời Gian Các Khâu Trong Pipeline"):
    """Vẽ biểu đồ Donut tỷ trọng thời gian các khâu trong toàn trình Pipeline."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig, ax = plt.subplots(figsize=(7.5, 6.5), subplot_kw=dict(aspect="equal"))

    labels = list(stages_dict.keys())
    times = list(stages_dict.values())
    total_time = sum(times)
    colors = [STAGE_COLORS.get(l, '#64748B') for l in labels]

    wedges, texts, autotexts = ax.pie(
        times,
        autopct='%1.1f%%',
        pctdistance=0.75,
        colors=colors,
        startangle=90,
        wedgeprops=dict(width=0.45, edgecolor='white', linewidth=2)
    )

    plt.setp(autotexts, size=10, weight="bold", color="white")
    legend_labels = [f"{l}: {t:.2f}s ({t/total_time*100:.1f}%)" for l, t in zip(labels, times)]
    ax.legend(
        wedges,
        legend_labels,
        title=f"Các Giai Đoạn (Tổng: {total_time:.2f}s)",
        loc="center left",
        bbox_to_anchor=(0.95, 0, 0.5, 1),
        fontsize=9.5
    )

    ax.set_title(title, fontsize=12.5, weight='bold', pad=18, color='#0F172A')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ tròn tỷ trọng thời gian: {output_path}")

# ==============================================================================
# 2. BIỂU ĐỒ CỘT (BAR CHARTS - ĐỐI SÁNH & PHÂN ĐOẠN)
# ==============================================================================

def plot_pipeline_stages_stacked_bar(stages_csv_path, output_path, title="So Sánh Thời Gian 4 Giai Đoạn & Toàn Trình (Thuần vs Spark)"):
    """Vẽ biểu đồ cột phân đoạn (Stacked Bar Chart) so sánh thời gian 4 giai đoạn giữa Thuần và Spark."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.read_csv(stages_csv_path)

    # Đảm bảo các cột giai đoạn có mặt
    # Dòng 0: Pandas, Dòng 1: Spark
    approaches = df['Approach'].tolist()
    stages = ['ETL', 'Feature_Engineering', 'Training', 'Evaluation']
    stage_labels = ['1. Tiền xử lý (ETL)', '2. Trích xuất đặc trưng', '3. Huấn luyện (RF)', '4. Đánh giá & Suy luận']
    stage_colors = ['#0284C7', '#0D9488', '#F97316', '#8B5CF6']

    fig, ax = plt.subplots(figsize=(10, 6.5))
    x = np.arange(len(approaches))
    width = 0.45

    bottom = np.zeros(len(approaches))
    for s_col, s_lbl, s_clr in zip(stages, stage_labels, stage_colors):
        values = df[s_col].values
        p = ax.bar(x, values, width, bottom=bottom, label=s_lbl, color=s_clr, edgecolor='white', linewidth=1.5)
        
        # Thêm nhãn số giây bên trong mỗi khối phân đoạn nếu giá trị đủ lớn
        for i, val in enumerate(values):
            if val > (df['Total_Time'].max() * 0.05):
                ax.text(x[i], bottom[i] + val / 2, f"{val:.1f}s", ha='center', va='center', color='white', fontweight='bold', fontsize=9.5)
        bottom += values

    # Thêm tổng thời gian trên đỉnh mỗi cột
    for i, tot in enumerate(df['Total_Time']):
        ax.text(x[i], tot + (df['Total_Time'].max() * 0.02), f"Tổng: {tot:.2f}s\n(~{tot/60:.2f} phút)", ha='center', va='bottom', fontweight='bold', fontsize=11, color='#0F172A')

    ax.set_xticks(x)
    ax.set_xticklabels(approaches, fontsize=12, fontweight='bold')
    ax.set_ylabel("Thời gian thực thi (Giây)", fontsize=11.5, labelpad=10)
    ax.set_title(title, fontsize=13.5, weight='bold', pad=25, color='#0F172A')
    ax.legend(title="Giai đoạn thực thi", loc='upper left', frameon=True, facecolor='#F8FAFC', fontsize=10)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.set_ylim(0, df['Total_Time'].max() * 1.22)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ cột phân đoạn thời gian: {output_path}")

def plot_rf_showdown_bar(rf_df, output_path, title="Đối Kháng Trực Diện: Random Forest CPU vs Spark RF MLlib"):
    """Vẽ biểu đồ đối kháng trực diện 2 mô hình Random Forest (Đơn máy vs Phân tán)."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    models = rf_df["Model"].tolist()
    colors = ['#3B82F6', '#EF4444']

    # Xác định linh hoạt tên cột cho Train Time, Latency, RAM
    col_time = next((c for c in ["Train Time (s)", "Training Time (s)", "Time (s)"] if c in rf_df.columns), "Train Time (s)")
    col_lat = next((c for c in ["Latency (ms/1k)", "Latency (per 1k ms)", "Latency (ms)"] if c in rf_df.columns), "Latency (ms/1k)")

    # Subplot 1: Chỉ số chất lượng mô hình (Accuracy vs F1)
    x = np.arange(len(models))
    width = 0.35
    axes[0].bar(x - width/2, rf_df["Accuracy (%)"], width, label="Accuracy (%)", color="#3B82F6", edgecolor="black", linewidth=0.6)
    axes[0].bar(x + width/2, rf_df["Weighted F1 (%)"], width, label="Weighted F1 (%)", color="#10B981", edgecolor="black", linewidth=0.6)
    axes[0].set_ylabel("Tỷ lệ (%)", fontsize=11)
    axes[0].set_title("Chỉ Số Phân Loại (Accuracy & F1-Score)", fontsize=12, weight="bold")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(models, rotation=10, fontsize=10.5)
    axes[0].set_ylim([max(0, rf_df["Accuracy (%)"].min() - 15), 105])
    axes[0].legend(loc="lower right")
    axes[0].grid(axis="y", linestyle="--", alpha=0.5)

    for i in x:
        axes[0].text(i - width/2, rf_df["Accuracy (%)"].iloc[i] + 1, f"{rf_df['Accuracy (%)'].iloc[i]:.1f}%", ha='center', fontsize=9.5, weight="bold")
        axes[0].text(i + width/2, rf_df["Weighted F1 (%)"].iloc[i] + 1, f"{rf_df['Weighted F1 (%)'].iloc[i]:.1f}%", ha='center', fontsize=9.5, weight="bold")

    # Subplot 2: Thời gian huấn luyện & Độ trễ
    axes[1].bar(x - width/2, rf_df[col_time], width, label="Training Time (s)", color="#F59E0B", edgecolor="black", linewidth=0.6)
    axes[1].set_ylabel("Thời gian huấn luyện (Giây)", fontsize=11, color="#B45309")
    axes[1].set_title("Thời Gian Huấn Luyện & Độ Trễ Suy Luận", fontsize=12, weight="bold")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(models, rotation=10, fontsize=10.5)
    axes[1].grid(axis="y", linestyle="--", alpha=0.5)

    ax2 = axes[1].twinx()
    ax2.plot(x, rf_df[col_lat], color="#8B5CF6", marker="o", linewidth=2.5, markersize=8, label="Latency (ms/1k)")
    ax2.set_ylabel("Độ trễ suy luận (ms / 1,000 mẫu)", fontsize=11, color="#6D28D9")
    ax2.grid(False)

    for i in x:
        axes[1].text(i - width/2, rf_df[col_time].iloc[i] + (rf_df[col_time].max()*0.02), f"{rf_df[col_time].iloc[i]:.2f}s", ha='center', fontsize=9.5, weight="bold", color="#B45309")
        ax2.text(i, rf_df[col_lat].iloc[i] + (rf_df[col_lat].max()*0.04), f"{rf_df[col_lat].iloc[i]:.1f}ms", ha='center', fontsize=9.5, weight="bold", color="#6D28D9")

    fig.suptitle(title, fontsize=14, weight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ đối kháng Random Forest: {output_path}")

def plot_grand_comparison_dashboard(df_metrics, output_path, title="Bảng Đối Sánh Toàn Diện Hiệu Năng Các Mô Hình Học Máy (7 Mô Hình)"):
    """Vẽ Dashboard 4 subplot tổng hợp toàn bộ các mô hình học máy."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))

    palette = sns.color_palette("viridis", len(df_metrics))
    col_time = next((c for c in ["Train Time (s)", "Training Time (s)", "Time (s)"] if c in df_metrics.columns), "Train Time (s)")
    col_lat = next((c for c in ["Latency (ms/1k)", "Latency (per 1k ms)", "Latency (ms)"] if c in df_metrics.columns), "Latency (ms/1k)")

    # 1. Accuracy
    sns.barplot(data=df_metrics, x="Accuracy (%)", y="Model", hue="Model", ax=axes[0, 0], palette=palette, legend=False)
    axes[0, 0].set_title("1. Độ Chính Xác (Accuracy %)", fontsize=12, weight="bold")
    axes[0, 0].set_xlim([max(0, df_metrics["Accuracy (%)"].min() - 10), 100])
    for i, v in enumerate(df_metrics["Accuracy (%)"]):
        axes[0, 0].text(v + 0.5, i, f"{v:.1f}%", va="center", weight="bold", fontsize=9.5)

    # 2. Weighted F1
    sns.barplot(data=df_metrics, x="Weighted F1 (%)", y="Model", hue="Model", ax=axes[0, 1], palette=palette, legend=False)
    axes[0, 1].set_title("2. Điểm Số F1 Cân Bằng (Weighted F1 %)", fontsize=12, weight="bold")
    axes[0, 1].set_xlim([max(0, df_metrics["Weighted F1 (%)"].min() - 10), 100])
    for i, v in enumerate(df_metrics["Weighted F1 (%)"]):
        axes[0, 1].text(v + 0.5, i, f"{v:.1f}%", va="center", weight="bold", fontsize=9.5)

    # 3. Training Time
    sns.barplot(data=df_metrics, x=col_time, y="Model", hue="Model", ax=axes[1, 0], palette="magma", legend=False)
    axes[1, 0].set_title("3. Thời Gian Huấn Luyện (Training Time - Giây)", fontsize=12, weight="bold")
    axes[1, 0].set_xlabel("Thời gian (Giây)")
    for i, v in enumerate(df_metrics[col_time]):
        axes[1, 0].text(v + (df_metrics[col_time].max()*0.01), i, f"{v:.2f}s", va="center", weight="bold", fontsize=9.5)

    # 4. Latency
    sns.barplot(data=df_metrics, x=col_lat, y="Model", hue="Model", ax=axes[1, 1], palette="mako", legend=False)
    axes[1, 1].set_title("4. Độ Trễ Suy Luận (Latency - ms / 1,000 mẫu)", fontsize=12, weight="bold")
    axes[1, 1].set_xlabel("Độ trễ (ms)")
    for i, v in enumerate(df_metrics[col_lat]):
        axes[1, 1].text(v + (df_metrics[col_lat].max()*0.01), i, f"{v:.1f}ms", va="center", weight="bold", fontsize=9.5)

    for ax in axes.flat:
        ax.grid(axis="x", linestyle="--", alpha=0.5)
        ax.set_ylabel("")

    fig.suptitle(title, fontsize=15, weight="bold", y=0.99)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo Grand Comparison Dashboard: {output_path}")

# ==============================================================================
# 3. BIỂU ĐỒ RIÊNG TỪNG MÔ HÌNH (CONFUSION MATRIX & FEATURE IMPORTANCE)
# ==============================================================================

def plot_confusion_matrix_individual(cm, labels, output_path, model_name):
    """Vẽ ma trận nhầm lẫn (Heatmap) chuẩn hóa phần trăm riêng cho từng mô hình."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # Chuẩn hóa theo hàng (Recall per class)
    cm_norm = cm.astype('float') / (cm.sum(axis=1)[:, np.newaxis] + 1e-9) * 100

    fig, ax = plt.subplots(figsize=(8.5, 7))
    sns.heatmap(
        cm_norm,
        annot=True,
        fmt=".1f",
        cmap="Blues",
        xticklabels=labels,
        yticklabels=labels,
        cbar_kws={'label': 'Tỷ lệ nhận diện đúng (%)'},
        ax=ax
    )

    ax.set_title(f"Ma Trận Nhầm Lẫn (Confusion Matrix) - {model_name}", fontsize=12.5, weight="bold", pad=15)
    ax.set_xlabel("Nhãn Dự Đoán (Predicted Label)", fontsize=11, labelpad=8)
    ax.set_ylabel("Nhãn Thực Tế (True Label)", fontsize=11, labelpad=8)
    plt.xticks(rotation=25, ha='right', fontsize=9.5)
    plt.yticks(rotation=0, fontsize=9.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ riêng Confusion Matrix: {output_path}")

def plot_feature_importance_individual(feat_imp_dict, output_path, model_name):
    """Vẽ biểu đồ thanh ngang tầm quan trọng đặc trưng riêng cho từng mô hình cây."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    sorted_items = sorted(feat_imp_dict.items(), key=lambda x: x[1], reverse=True)[:15]
    features = [k for k, v in sorted_items][::-1]
    scores = [v for k, v in sorted_items][::-1]

    fig, ax = plt.subplots(figsize=(9, 6))
    colors = sns.color_palette("Blues_r", len(features))
    bars = ax.barh(features, scores, color=colors, edgecolor='#CBD5E1')

    for bar, score in zip(bars, scores):
        ax.text(score + (max(scores)*0.01), bar.get_y() + bar.get_height()/2, f"{score:.4f}", va='center', fontsize=9, weight='bold', color='#1E293B')

    ax.set_title(f"Tầm Quan Trọng Đặc Trưng (Feature Importance) - {model_name}", fontsize=12.5, weight="bold", pad=15)
    ax.set_xlabel("Trọng số đóng góp (Gini / Split Importance)", fontsize=11, labelpad=8)
    ax.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ riêng Feature Importance: {output_path}")

# ==============================================================================
# 4. CÁC BIỂU ĐỒ NÂNG CAO MỚI (RADAR, BUBBLE, GROUPED BAR, HORIZONTAL BAR, RAM)
# ==============================================================================

def plot_multi_metric_radar(df_metrics, output_path, title="Đánh Giá Đa Chiều 5 Chỉ Số Chất Lượng (Radar Chart - 7 Mô Hình)"):
    """Vẽ biểu đồ Mạng Nhện / Đa Giác (Radar / Spider Chart) so sánh 5 góc chất lượng của các mô hình."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Danh sách các chỉ số cần vẽ radar
    metrics = ['Accuracy (%)', 'Precision (%)', 'Recall (%)', 'Weighted F1 (%)', 'Macro F1 (%)']
    available_metrics = [m for m in metrics if m in df_metrics.columns]
    
    if len(available_metrics) < 3:
        print("  [!] Không đủ chỉ số để vẽ Radar Chart (cần ít nhất 3 chỉ số).")
        return

    num_vars = len(available_metrics)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    angles += angles[:1] # Khép kín vòng tròn

    fig, ax = plt.subplots(figsize=(9, 8), subplot_kw=dict(polar=True))
    
    # Bảng màu cho từng mô hình
    colors = ['#10B981', '#3B82F6', '#8B5CF6', '#F59E0B', '#EC4899', '#06B6D4', '#EF4444']
    
    # Nhãn hiển thị thân thiện trên 5 đỉnh
    display_labels = [
        'Accuracy\n(Tổng thể)',
        'Precision\n(Độ chuẩn xác)',
        'Recall\n(Độ phủ)',
        'Weighted F1\n(Cân bằng)',
        'Macro F1\n(Lớp thiểu số)'
    ][:num_vars]

    for idx, row in df_metrics.iterrows():
        model_name = row['Model']
        values = [row[m] for m in available_metrics]
        values += values[:1] # Khép kín đường vẽ
        color = colors[idx % len(colors)]
        
        # Chọn độ dày nét: Spark RF và Random Forest CPU nét đậm hơn
        is_highlight = 'Random Forest' in model_name or 'Spark' in model_name
        lw = 2.5 if is_highlight else 1.5
        alpha_fill = 0.12 if is_highlight else 0.05
        
        ax.plot(angles, values, label=model_name, color=color, linewidth=lw, marker='o', markersize=4)
        ax.fill(angles, values, color=color, alpha=alpha_fill)

    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_thetagrids(np.degrees(angles[:-1]), display_labels, fontsize=10.5, fontweight='bold')
    
    # Thang đo từ 0 đến 100%
    ax.set_ylim(0, 100)
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels(["20%", "40%", "60%", "80%", "100%"], fontsize=8.5, color="#64748B")
    ax.grid(True, linestyle='--', color='#CBD5E1', alpha=0.8)

    ax.set_title(title, fontsize=13, weight='bold', pad=25, color='#0F172A')
    ax.legend(loc='upper right', bbox_to_anchor=(1.35, 1.1), fontsize=9.5, frameon=True, facecolor='#F8FAFC')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ mạng nhện Radar Chart: {output_path}")

def plot_accuracy_vs_speed_bubble(df_metrics, output_path, title="Cân Bằng Hiệu Năng & Tốc Độ Huấn Luyện (Bubble Trade-off Plot)"):
    """Vẽ biểu đồ bong bóng phân tán (Bubble Chart): F1 vs Training Time vs RAM Usage."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    col_time = next((c for c in ["Train Time (s)", "Training Time (s)", "Time (s)"] if c in df_metrics.columns), "Train Time (s)")
    col_ram = next((c for c in ["RAM Usage (MB)", "Peak RAM (MB)", "RAM (MB)"] if c in df_metrics.columns), "RAM Usage (MB)")

    fig, ax = plt.subplots(figsize=(10.5, 6.8))

    x = df_metrics[col_time].values
    y = df_metrics["Weighted F1 (%)"].values
    
    # Kích thước bong bóng tỷ lệ thuận với lượng RAM tiêu thụ
    if col_ram in df_metrics.columns:
        rams = df_metrics[col_ram].values
        sizes = [max(120, r * 0.8) for r in rams]
    else:
        sizes = [300] * len(x)
        rams = [0] * len(x)

    colors = ['#10B981', '#3B82F6', '#8B5CF6', '#F59E0B', '#EC4899', '#06B6D4', '#EF4444']

    for i, row in df_metrics.iterrows():
        clr = colors[i % len(colors)]
        sc = ax.scatter(x[i], y[i], s=sizes[i], color=clr, alpha=0.7, edgecolors='black', linewidth=1.5, zorder=5)
        
        # Chú thích tên mô hình ngay cạnh bong bóng
        label_text = f"{row['Model']}\n({x[i]:.1f}s | F1: {y[i]:.1f}%)"
        ax.annotate(
            label_text,
            (x[i], y[i]),
            xytext=(10, 8),
            textcoords='offset points',
            fontsize=9.5,
            fontweight='bold',
            color='#1E293B',
            bbox=dict(boxstyle="round,pad=0.3", fc="#FFFFFF", ec=clr, alpha=0.9)
        )

    ax.set_xscale('log') # Thang log vì thời gian có thể chênh lệch từ 0.5s đến 100s
    ax.set_xlabel("Thời Gian Huấn Luyện - log scale (Giây)", fontsize=11.5, labelpad=10)
    ax.set_ylabel("Chỉ Số Weighted F1-Score (%)", fontsize=11.5, labelpad=10)
    ax.set_ylim([max(0, min(y) - 15), 100])
    ax.grid(True, which="both", ls="--", alpha=0.4)

    # Chú thích kích thước bong bóng
    ax.text(0.02, 0.05, "● Kích thước bong bóng thể hiện dung lượng RAM tiêu thụ đỉnh (MB)",
            transform=ax.transAxes, fontsize=9.5, fontstyle='italic', color='#64748B')

    ax.set_title(title, fontsize=13, weight='bold', pad=18, color='#0F172A')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ bong bóng Bubble Plot: {output_path}")

def plot_models_grouped_bar(df_metrics, output_path, title="So Sánh Đối Đầu Chỉ Số Accuracy & F1-Score (7 Mô Hình)"):
    """Vẽ biểu đồ cột kép (Grouped Bar) so sánh Accuracy và Weighted F1."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 6))

    models = df_metrics["Model"].tolist()
    x = np.arange(len(models))
    width = 0.35

    rects1 = ax.bar(x - width/2, df_metrics["Accuracy (%)"], width, label="Accuracy (%)", color="#3B82F6", edgecolor="white", linewidth=1)
    rects2 = ax.bar(x + width/2, df_metrics["Weighted F1 (%)"], width, label="Weighted F1 (%)", color="#10B981", edgecolor="white", linewidth=1)

    ax.set_ylabel("Tỷ lệ (%)", fontsize=11.5)
    ax.set_title(title, fontsize=13, weight="bold", pad=20, color='#0F172A')
    ax.set_xticks(x)
    ax.set_xticklabels(models, rotation=15, ha="right", fontsize=10.5, weight="bold")
    ax.legend(loc="upper right", frameon=True, facecolor="#F8FAFC")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim([0, 105])

    for rect in rects1:
        h = rect.get_height()
        ax.annotate(f"{h:.1f}%", xy=(rect.get_x() + rect.get_width() / 2, h), xytext=(0, 3),
                    textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold", color="#1D4ED8")

    for rect in rects2:
        h = rect.get_height()
        ax.annotate(f"{h:.1f}%", xy=(rect.get_x() + rect.get_width() / 2, h), xytext=(0, 3),
                    textcoords="offset points", ha="center", va="bottom", fontsize=9, fontweight="bold", color="#047857")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ cột kép Grouped Bar: {output_path}")

def plot_models_training_time_horizontal_bar(df_metrics, output_path, title="Xếp Hạng Thời Gian Huấn Luyện Các Mô Hình (Training Time)"):
    """Vẽ biểu đồ thanh ngang xếp hạng thời gian huấn luyện từ nhanh nhất đến lâu nhất."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    col_time = next((c for c in ["Train Time (s)", "Training Time (s)", "Time (s)"] if c in df_metrics.columns), "Train Time (s)")
    
    df_sorted = df_metrics.sort_values(by=col_time, ascending=True)
    
    fig, ax = plt.subplots(figsize=(10, 5.5))
    colors = sns.color_palette("rocket_r", len(df_sorted))
    
    bars = ax.barh(df_sorted["Model"], df_sorted[col_time], color=colors, edgecolor="#CBD5E1")
    
    for bar in bars:
        w = bar.get_width()
        ax.text(w + (df_sorted[col_time].max() * 0.015), bar.get_y() + bar.get_height()/2,
                f"{w:.2f}s", va="center", fontsize=9.5, fontweight="bold", color="#0F172A")

    ax.set_xlabel("Thời gian huấn luyện (Giây) - Càng ngắn càng tốt", fontsize=11, labelpad=8)
    ax.set_title(title, fontsize=12.5, weight="bold", pad=15, color='#0F172A')
    ax.grid(axis="x", linestyle="--", alpha=0.5)
    ax.set_xlim(0, df_sorted[col_time].max() * 1.15)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ thanh ngang Training Time: {output_path}")

def plot_models_latency_bar(df_metrics, output_path, title="So Sánh Độ Trễ Suy Luận (Inference Latency - ms / 1,000 mẫu)"):
    """Vẽ biểu đồ cột so sánh độ trễ suy luận (ms / 1.000 mẫu)."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    col_lat = next((c for c in ["Latency (ms/1k)", "Latency (per 1k ms)", "Latency (ms)"] if c in df_metrics.columns), "Latency (ms/1k)")
    
    fig, ax = plt.subplots(figsize=(11, 5.5))
    colors = sns.color_palette("mako", len(df_metrics))
    
    bars = ax.bar(df_metrics["Model"], df_metrics[col_lat], color=colors, edgecolor="white", width=0.55)
    
    for bar in bars:
        h = bar.get_height()
        ax.annotate(f"{h:.1f}ms", xy=(bar.get_x() + bar.get_width() / 2, h), xytext=(0, 3),
                    textcoords="offset points", ha="center", va="bottom", fontsize=9.5, fontweight="bold", color="#1E293B")

    ax.set_ylabel("Độ trễ suy luận (ms / 1,000 mẫu) - Càng thấp càng tốt", fontsize=11)
    ax.set_title(title, fontsize=12.5, weight="bold", pad=18, color='#0F172A')
    ax.set_xticklabels(df_metrics["Model"], rotation=15, ha="right", fontsize=10, weight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim(0, df_metrics[col_lat].max() * 1.18)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ cột Latency: {output_path}")

def plot_peak_ram_comparison(df_metrics, output_path, title="So Sánh Mức Tiêu Thụ Bộ Nhớ Đỉnh (Peak RAM Usage - MB)"):
    """Vẽ biểu đồ cột so sánh lượng RAM tiêu thụ đỉnh giữa các mô hình."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    col_ram = next((c for c in ["RAM Usage (MB)", "Peak RAM (MB)", "RAM (MB)"] if c in df_metrics.columns), None)
    
    if not col_ram or col_ram not in df_metrics.columns:
        print("  [!] Không tìm thấy cột RAM để vẽ biểu đồ Peak RAM.")
        return

    fig, ax = plt.subplots(figsize=(11, 5.5))
    colors = sns.color_palette("flare", len(df_metrics))
    
    bars = ax.bar(df_metrics["Model"], df_metrics[col_ram], color=colors, edgecolor="white", width=0.55)
    
    for bar in bars:
        h = bar.get_height()
        ax.annotate(f"{h:.1f} MB", xy=(bar.get_x() + bar.get_width() / 2, h), xytext=(0, 3),
                    textcoords="offset points", ha="center", va="bottom", fontsize=9.5, fontweight="bold", color="#831843")

    ax.set_ylabel("Bộ nhớ RAM đỉnh (MB)", fontsize=11)
    ax.set_title(title, fontsize=12.5, weight="bold", pad=18, color='#0F172A')
    ax.set_xticklabels(df_metrics["Model"], rotation=15, ha="right", fontsize=10, weight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_ylim(0, df_metrics[col_ram].max() * 1.18)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  [✓] Đã tạo biểu đồ cột Peak RAM: {output_path}")
