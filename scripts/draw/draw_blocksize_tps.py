##### run by cmd #####
HELP = 'python draw_blocksize_tps.py -f file_path -w warehouse -t thread'
##### run by cmd #####

X = "block_size"
Y = "tps"
XLABEL = "Block Size"
YLABEL = "Troughput(Txn/s)"

import pandas as pd
import argparse
import sys
import numpy as np

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
parser.add_argument("-w", "--warehouse", type=str, required=True, help="warehouse: warehouse number")
parser.add_argument("-t", "--thread", type=str, required=True, help="thread: thread number")
args = parser.parse_args()
file: str = args.file
warehouse = args.warehouse
thread_num = args.thread

# savepath = f'../pics/blocksize/bench_blocksize_{warehouse}:{thread_num}_tps.pdf'
savepath = f'./bench_blocksize_{warehouse}:{thread_num}_tps.pdf'


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

blocksizes = recs['block_size'].unique()
print(blocksizes)
uniform_ticks = np.arange(len(blocksizes))

# for idx, (schema, color) in enumerate(schemas):
marker_list = ['v', 's', 'o', '^', '<', '>', 'D', 'h'] 
for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    # print(records[Y])
    p.plot(
        ax,
        xdata=uniform_ticks,
        # xdata=records[X],
        ydata=records[Y],
        color=color, 
        legend_label=schema,
    )

# 设置X轴标签
ax.set_xticks(uniform_ticks, blocksizes)
# ax.set_xticks([int(t) for t in recs['block_size'].unique()])
# ax.set_xticks([int(t) for i, t in enumerate(recs['block_size'].unique()) if i % 2 == 0])
# ax.set_xticklabels([str(int(t) // 100) if (t % 100 == 0) else str(float(t) / 100) for t in recs['block_size'].unique()])

# 自适应Y轴变化
step = None
# if workload == 'smallbank' and contention == 'skewed':
#     step = 140000
# elif workload == 'tpcc' and contention == '10orderlines':
#     step = 13000
p.format_yticks(ax, suffix='K', step=10000, step_num=4)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)
# ax.set_ylabel(YLABEL, labelpad=-10)
# box1: plt.Bbox = ax.get_window_extent()
# box2: plt.Bbox = ax.get_tightbbox()

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.22), columnspacing=1.5)
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