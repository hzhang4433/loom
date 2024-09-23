##### run by cmd #####
HELP = 'python draw_latency_tps.py -f file_path -w warehouse -b blocksize'
##### run by cmd #####

X = "threads"
Y = "tx_latency"
XLABEL = "Threads"
YLABEL = "Latency(ms)"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
parser.add_argument("-w", "--warehouse", type=str, required=True, help="warehouse: warehouse number")
parser.add_argument("-b", "--blocksize", type=str, required=True, help="blocksize: size of block")
args = parser.parse_args()
file: str = args.file
warehouse = args.warehouse
blocksize = args.blocksize

savepath = f'../pics/thread/bench_thread_{warehouse}:{blocksize}_latency.pdf'


#################### 数据准备 ####################
if (file.endswith('csv')):
    recs = pd.read_csv(file)
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

# for idx, (schema, color) in enumerate(schemas):
marker_list = ['v', 's', 'o', '^', '<', '>', 'D', 'h'] 
for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    # print(records[Y])
    p.plot(
        ax,
        xdata=records[X],
        ydata=records[Y],
        color=color, 
        legend_label=schema,
    )

# 设置X轴标签
ax.set_xticks([int(t) for t in recs['threads'].unique()])
# ax.set_xticks([int(t) for i, t in enumerate(recs['block_size'].unique()) if i % 2 == 0])
# ax.set_xticklabels([str(int(t) // 100) if (t % 100 == 0) else str(float(t) / 100) for t in recs['block_size'].unique()])

# 自适应Y轴变化
step = None
# if workload == 'smallbank' and contention == 'skewed':
#     step = 140000
# elif workload == 'tpcc' and contention == '10orderlines':
#     step = 13000
# p.format_yticks(ax, suffix='K', step_num=4)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)
# ax.set_ylabel(YLABEL, labelpad=-10)
# box1: plt.Bbox = ax.get_window_extent()
# box2: plt.Bbox = ax.get_tightbbox()

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))
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