import os
import sys
import matplotlib.pyplot as plt
import numpy as np

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Thiết lập font và style chuẩn khoa học, thanh lịch
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['axes.edgecolor'] = '#94A3B8'
plt.rcParams['axes.linewidth'] = 1.0

def create_simple_etl_plot():
    # Dữ liệu đối sánh
    methods = ['Thuần CPU (Đơn máy)', 'Apache Spark (Cụm 3 Laptop)']
    times = [65.42, 28.12]
    colors = ['#3B82F6', '#10B981']  # Xanh dương & Xanh lá chuẩn hiện đại

    fig, ax = plt.subplots(figsize=(7.5, 5.5), facecolor='white')
    x = np.arange(len(methods))
    width = 0.45

    # Vẽ cột
    bars = ax.bar(x, times, width=width, color=colors, edgecolor='#1E293B', linewidth=1.2)

    # Hiển thị số liệu thời gian trên đỉnh mỗi cột (chỉ số giây, không ghi chú nhanh/giảm)
    for bar, val in zip(bars, times):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + 1.5,
            f"{val:.2f} s",
            ha='center',
            va='bottom',
            fontsize=12,
            fontweight='bold',
            color='#0F172A'
        )

    # Định dạng trục
    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11.5, fontweight='bold', color='#1E293B')
    ax.set_ylabel('Thời gian thực thi (Giây)', fontsize=11.5, fontweight='bold', color='#1E293B', labelpad=10)
    ax.set_title('So Sánh Thời Gian Tiền Xử Lý (ETL - 7,079,081 Dòng)\nThuần CPU vs Apache Spark', 
                 fontsize=12.5, fontweight='bold', color='#0F172A', pad=18)
    
    ax.set_ylim(0, 80)
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    # Đảm bảo viền gọn gàng
    plt.tight_layout()

    # Đường dẫn lưu file
    out_paths = [
        "results/figures/combined/etl_spark_vs_thuan_benchmark_7m.png",
        "results/full_7m/figures/combined/etl_spark_vs_thuan_benchmark_7m.png",
        "results/figures/combined/so_sanh_thoi_gian_tien_xu_ly_cpu_vs_spark.png"
    ]

    for p in out_paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        plt.savefig(p, dpi=300, bbox_inches='tight')
        print(f"[✓] Đã tạo thành công: {p}")

    plt.close()

if __name__ == "__main__":
    create_simple_etl_plot()
