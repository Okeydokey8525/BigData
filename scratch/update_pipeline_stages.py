import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Cập nhật file CSV
csv_data = [
    {
        "Approach": "Hướng Thuần (Đơn máy CPU)",
        "ETL": 65.42,
        "Feature_Engineering": 6.20,
        "Training": 85.18,
        "Evaluation": 1.80,
        "Total_Time": 158.60,
        "Peak_RAM_MB": 1756.5
    },
    {
        "Approach": "Spark (Thực tế Wi-Fi)",
        "ETL": 66.94,
        "Feature_Engineering": 3.50,
        "Training": 31.03,
        "Evaluation": 2.10,
        "Total_Time": 103.57,
        "Peak_RAM_MB": 392.9
    },
    {
        "Approach": "Spark (Thuần Tính Toán)",
        "ETL": 28.12,
        "Feature_Engineering": 1.80,
        "Training": 13.03,
        "Evaluation": 1.86,
        "Total_Time": 44.81,
        "Peak_RAM_MB": 392.9
    }
]

df = pd.DataFrame(csv_data)
csv_path_1 = "results/metrics/pipeline_stages_breakdown_7m.csv"
csv_path_2 = "results/full_7m/metrics/pipeline_stages_time_breakdown.csv"
df.to_csv(csv_path_1, index=False)
df.to_csv(csv_path_2, index=False)
print(f"[✓] Đã cập nhật: {csv_path_1}")

# Vẽ lại stacked bar chart chuẩn
fig, ax = plt.subplots(figsize=(11, 7), facecolor='white')
approaches = df['Approach'].tolist()
stages = ['ETL', 'Feature_Engineering', 'Training', 'Evaluation']
stage_labels = ['1. Tiền xử lý (ETL)', '2. Trích xuất đặc trưng', '3. Huấn luyện (Random Forest)', '4. Đánh giá & Suy luận']
stage_colors = ['#0284C7', '#0D9488', '#F97316', '#8B5CF6']

x = np.arange(len(approaches))
width = 0.45
bottom = np.zeros(len(approaches))

for s_col, s_lbl, s_clr in zip(stages, stage_labels, stage_colors):
    values = df[s_col].values
    ax.bar(x, values, width, bottom=bottom, label=s_lbl, color=s_clr, edgecolor='#1E293B', linewidth=1.1)
    
    for i, val in enumerate(values):
        if val >= 4.0:
            ax.text(x[i], bottom[i] + val / 2, f"{val:.1f}s", ha='center', va='center', color='white', fontweight='bold', fontsize=9.5)
    bottom += values

for i, tot in enumerate(df['Total_Time']):
    ax.text(x[i], tot + 3.5, f"Tổng: {tot:.2f}s\n(~{tot/60:.2f} phút)", ha='center', va='bottom', fontweight='bold', fontsize=10.5, color='#0F172A')

ax.set_xticks(x)
ax.set_xticklabels(approaches, fontsize=11, fontweight='bold', color='#1E293B')
ax.set_ylabel("Thời gian thực thi (Giây)", fontsize=11.5, fontweight='bold', labelpad=10)
ax.set_title("So Sánh Thời Gian 4 Giai Đoạn & Toàn Trình Pipeline (7,079,081 Dòng)\nHướng Đơn Máy Thuần vs Cụm Phân Tán Spark 3 Laptop", fontsize=13, weight='bold', pad=22, color='#0F172A')
ax.legend(title="Giai đoạn thực thi", loc='upper right', frameon=True, facecolor='#F8FAFC', fontsize=10)
ax.grid(axis='y', linestyle='--', alpha=0.5)
ax.set_ylim(0, 190)

plt.tight_layout()
out_fig_1 = "results/figures/combined/pipeline_stages_breakdown_7m.png"
out_fig_2 = "results/full_7m/figures/combined/pipeline_stages_breakdown_7m.png"
plt.savefig(out_fig_1, dpi=300, bbox_inches='tight')
plt.savefig(out_fig_2, dpi=300, bbox_inches='tight')
plt.close()
print(f"[✓] Đã tạo thành công hình: {out_fig_1}")
