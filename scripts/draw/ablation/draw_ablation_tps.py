##### run by cmd #####
HELP = 'python draw_ablation_tps.py -f file_path'
##### run by cmd #####

X = "threads"
Y = "tps"
XLABEL = "Threads"
YLABEL = "Troughput(Txn/s)"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas_for_ablation as schemas

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
args = parser.parse_args()
file: str = args.file

# savepath = f'../../pics/ablation/bench_ablation_tps.pdf'
savepath = f'./bench_ablation_tps.pdf'

schemas_dict = {
    'Loom_20': 'Loom$_\mathit{Medium}$',
    'LoomFR_20': 'LoomFR$_\mathit{Medium}$',
    'LoomRaw_20': 'LoomRaw$_\mathit{Medium}$',
    'Loom_1': 'Loom$_\mathit{High}$',
    'LoomFR_1': 'LoomFR$_\mathit{High}$',
    'LoomRaw_1': 'LoomRaw$_\mathit{High}$',
}

#################### 数据准备 ####################
recs = pd.read_csv(file)
# inner_schemas = recs['protocol'].unique()
# print(inner_schemas)

recs['warehouse'] = recs['warehouse'].astype(str)
recs['protocol_warehouse'] = recs['protocol'] + '_' + recs['warehouse']
unique_protocols = recs['protocol_warehouse'].unique()
print(unique_protocols)


#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

# for idx, (schema, color) in enumerate(schemas):
marker_list = ['<', 's', '>', 'v', 'o', '^', 'D', 'h'] 
for idx, (schema, color) in enumerate(schemas):
    # records = recs[recs['protocol'] == schema]
    records = recs[recs['protocol_warehouse'] == schema]
    # print(records[Y])
    p.plot(
        ax,
        xdata=records[X],
        ydata=records[Y],
        color=color, 
        legend_label=schemas_dict[schema] if schemas_dict.get(schema) else schema,
        marker=marker_list[idx]
    )

# 设置X轴标签
ax.set_xticks([int(t) for t in recs['threads'].unique()])
# ax.set_xticks([int(t) for i, t in enumerate(recs['block_size'].unique()) if i % 2 == 0])
# ax.set_xticklabels([str(int(t) // 100) if (t % 100 == 0) else str(float(t) / 100) for t in recs['block_size'].unique()])

# 自适应Y轴变化
step = None
p.format_yticks(ax, suffix='K', step=9000, step_num=4)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)
# ax.set_ylabel(YLABEL, labelpad=-10)
# box1: plt.Bbox = ax.get_window_extent()
# box2: plt.Bbox = ax.get_tightbbox()

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.2), columnspacing=0.5)
# if contention == 'pres':
#     p.legend(
#         ax, 
#         loc="upper center", 
#         ncol=4, 
#         anchor=(0.5, 1.18) if contention == 'compare' else (0.5, 1.2), 
#         kwargs={ 'size': 10 } if contention == 'compare' else None,
#         columnspacing=2
#     )
# else:
#     p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))

# 保存
p.save(savepath)