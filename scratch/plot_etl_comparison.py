import os
import sys
import matplotlib.pyplot as plt
import numpy as np

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Thiết lập font và style chuẩn khoa học
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['axes.edgecolor'] = '#CBD5E1'
plt.rcParams['axes.linewidth'] = 1.2

def create_etl_comparison_figure():
    out_dir_1 = "results/figures/combined"
    out_dir_2 = "results/full_7m/figures/combined"
    os.makedirs(out_dir_1, exist_ok=True)
    os.makedirs(out_dir_2, exist_ok=True)

    out_file_1 = os.path.join(out_dir_1, "etl_spark_vs_thuan_benchmark_7m.png")
    out_file_2 = os.path.join(out_dir_2, "etl_spark_vs_thuan_benchmark_7m.png")

    fig = plt.figure(figsize=(15, 9), facecolor='white')
    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.28)

    # -------------------------------------------------------------
    # 1. SUBPLOT 1: THỜI GIAN THỰC THI TIỀN XỬ LÝ (ETL RUNTIME)
    # -------------------------------------------------------------
    ax1 = fig.add_subplot(gs[0, 0])
    methods = [
        'Đơn máy Thuần\n(Pandas / Polars)',
        'Spark 3 Laptop\n(Thực tế có Wi-Fi)',
        'Spark Phân Tán\n(Thuần Tính Toán)'
    ]
    times = [65.42, 66.94, 28.12]
    colors1 = ['#3B82F6', '#F59E0B', '#10B981']

    bars1 = ax1.bar(methods, times, color=colors1, width=0.55, edgecolor='#1E293B', linewidth=1.2)
    ax1.set_ylabel('Thời gian thực thi (Giây)', fontsize=11, fontweight='bold', color='#1E293B')
    ax1.set_title('(A) Thời Gian Tiền Xử Lý 7,079,081 Dòng Dữ Liệu', fontsize=12.5, fontweight='bold', color='#0F172A', pad=12)
    ax1.set_ylim(0, 85)
    ax1.grid(axis='y', linestyle='--', alpha=0.5)

    # Ghi chú giá trị trên cột
    for bar, val in zip(bars1, times):
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, yval + 1.8, f"{val:.2f} s", ha='center', va='bottom', fontsize=10.5, fontweight='bold', color='#0F172A')

    # Mũi tên và nhãn tăng tốc (Speedup)
    ax1.annotate('Nhanh gấp 2.33 lần\n(Tiết kiệm 57% thời gian)',
                 xy=(2, 28.12), xytext=(1.4, 48),
                 arrowprops=dict(facecolor='#059669', shrink=0.08, width=2, headwidth=7),
                 fontsize=10, fontweight='bold', color='#047857',
                 bbox=dict(boxstyle="round,pad=0.4", fc="#ECFDF5", ec="#10B981", lw=1))

    # -------------------------------------------------------------
    # 2. SUBPLOT 2: THÔNG LƯỢNG XỬ LÝ (THROUGHPUT - ROWS/SEC)
    # -------------------------------------------------------------
    ax2 = fig.add_subplot(gs[0, 1])
    # 7,079,081 dòng / thời gian
    throughputs = [7079081 / 65.42, 7079081 / 66.94, 7079081 / 28.12]
    colors2 = ['#60A5FA', '#FBBF24', '#34D399']

    bars2 = ax2.bar(methods, [t / 1000 for t in throughputs], color=colors2, width=0.55, edgecolor='#1E293B', linewidth=1.2)
    ax2.set_ylabel('Thông lượng (Nghìn dòng / Giây)', fontsize=11, fontweight='bold', color='#1E293B')
    ax2.set_title('(B) Tốc Độ Xử Lý Dữ Liệu (Throughput)', fontsize=12.5, fontweight='bold', color='#0F172A', pad=12)
    ax2.set_ylim(0, 320)
    ax2.grid(axis='y', linestyle='--', alpha=0.5)

    for bar, val in zip(bars2, throughputs):
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, yval + 6, f"{val:,.0f} rows/s\n({val/1000:.1f}k)", ha='center', va='bottom', fontsize=9.5, fontweight='bold', color='#0F172A')

    # -------------------------------------------------------------
    # 3. SUBPLOT 3: TIÊU THỤ BỘ NHỚ RAM ĐỈNH (PEAK RAM USAGE)
    # -------------------------------------------------------------
    ax3 = fig.add_subplot(gs[1, 0])
    ram_labels = ['Đơn máy Thuần\n(Toàn bộ RAM máy)', 'Spark Cluster\n(Driver Node)', 'Spark Cluster\n(Mỗi Worker Node)']
    rams = [1756.5, 610.0, 392.9]
    colors3 = ['#EF4444', '#8B5CF6', '#06B6D4']

    bars3 = ax3.bar(ram_labels, rams, color=colors3, width=0.55, edgecolor='#1E293B', linewidth=1.2)
    ax3.set_ylabel('RAM tiêu thụ đỉnh (MB)', fontsize=11, fontweight='bold', color='#1E293B')
    ax3.set_title('(C) Tối Ưu Hóa Bộ Nhớ RAM Trong Tiền Xử Lý', fontsize=12.5, fontweight='bold', color='#0F172A', pad=12)
    ax3.set_ylim(0, 2200)
    ax3.grid(axis='y', linestyle='--', alpha=0.5)

    for bar, val in zip(bars3, rams):
        yval = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2, yval + 40, f"{val:.1f} MB", ha='center', va='bottom', fontsize=10, fontweight='bold', color='#0F172A')

    ax3.annotate('Tiết kiệm 77.6% RAM\nTránh tràn bộ nhớ OOM',
                 xy=(2, 392.9), xytext=(1.45, 1100),
                 arrowprops=dict(facecolor='#0891B2', shrink=0.08, width=2, headwidth=7),
                 fontsize=10, fontweight='bold', color='#0E7490',
                 bbox=dict(boxstyle="round,pad=0.4", fc="#CFFAFE", ec="#06B6D4", lw=1))

    # -------------------------------------------------------------
    # 4. SUBPLOT 4: HIỆU QUẢ NÉN & ĐỊNH DẠNG LƯU TRỮ (STORAGE)
    # -------------------------------------------------------------
    ax4 = fig.add_subplot(gs[1, 1])
    storage_labels = ['CSV Thô Ban Đầu\n(Raw Dataset 2024)', 'Parquet Snappy\n(Phân vùng 12 Tháng)']
    sizes = [1248.37, 140.57]
    colors4 = ['#94A3B8', '#10B981']

    bars4 = ax4.bar(storage_labels, sizes, color=colors4, width=0.48, edgecolor='#1E293B', linewidth=1.2)
    ax4.set_ylabel('Dung lượng lưu trữ (MB)', fontsize=11, fontweight='bold', color='#1E293B')
    ax4.set_title('(D) Hiệu Quả Nén & Tối Ưu Hóa Lưu Trữ Cột', fontsize=12.5, fontweight='bold', color='#0F172A', pad=12)
    ax4.set_ylim(0, 1500)
    ax4.grid(axis='y', linestyle='--', alpha=0.5)

    for bar, val in zip(bars4, sizes):
        yval = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2, yval + 30, f"{val:.1f} MB", ha='center', va='bottom', fontsize=10.5, fontweight='bold', color='#0F172A')

    ax4.annotate('Giảm 88.7% Dung lượng\n(Tỷ lệ nén 8.88x)',
                 xy=(1, 140.57), xytext=(0.65, 650),
                 arrowprops=dict(facecolor='#059669', shrink=0.08, width=2, headwidth=7),
                 fontsize=10.5, fontweight='bold', color='#047857',
                 bbox=dict(boxstyle="round,pad=0.4", fc="#ECFDF5", ec="#10B981", lw=1))

    # Tiêu đề tổng quát (Super Title)
    fig.suptitle('ĐỐI SÁNH HIỆU NĂNG TIỀN XỬ LÝ DỮ LIỆU LỚN (ETL BENCHMARK 7,079,081 CHUYẾN BAY)\nĐƠN MÁY THUẦN (PANDAS/POLARS) VS CỤM PHÂN TÁN APACHE SPARK (3 LAPTOP)',
                 fontsize=14, fontweight='bold', color='#0F172A', y=0.98)

    plt.savefig(out_file_1, dpi=300, bbox_inches='tight')
    plt.savefig(out_file_2, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[✓] Đã tạo thành công hình biểu đồ ETL Benchmark:")
    print(f"    - {out_file_1}")
    print(f"    - {out_file_2}")

if __name__ == "__main__":
    create_etl_comparison_figure()
