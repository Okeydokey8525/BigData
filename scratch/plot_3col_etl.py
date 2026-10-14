import os
import sys
import matplotlib.pyplot as plt
import numpy as np

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['axes.edgecolor'] = '#94A3B8'
plt.rcParams['axes.linewidth'] = 1.0

def create_3col_etl_plot():
    methods = [
        'Thuần CPU\n(Đơn máy)',
        'Apache Spark\n(Thực tế Wi-Fi)',
        'Apache Spark\n(Thuần tính toán)'
    ]
    times = [65.42, 66.94, 28.12]
    colors = ['#3B82F6', '#F59E0B', '#10B981']

    fig, ax = plt.subplots(figsize=(8.2, 5.5), facecolor='white')
    x = np.arange(len(methods))
    width = 0.48

    bars = ax.bar(x, times, width=width, color=colors, edgecolor='#1E293B', linewidth=1.2)

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

    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11, fontweight='bold', color='#1E293B')
    ax.set_ylabel('Thời gian thực thi (Giây)', fontsize=11.5, fontweight='bold', color='#1E293B', labelpad=10)
    ax.set_title('So Sánh Thời Gian Tiền Xử Lý (ETL - 7,079,081 Dòng)\nThuần CPU vs Apache Spark', 
                 fontsize=12.5, fontweight='bold', color='#0F172A', pad=18)
    
    ax.set_ylim(0, 85)
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    out_path = "results/figures/combined/so_sanh_thoi_gian_tien_xu_ly_cpu_vs_spark_3cot.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[✓] Đã tạo thành công bản 3 cột: {out_path}")

if __name__ == "__main__":
    create_3col_etl_plot()
