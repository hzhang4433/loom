##### run by cmd #####
HELP = 'python draw_blocksize_latency.py -f file_path -w warehouse -t thread'
##### run by cmd #####

X = "block_size"
Y = "tx_latency"
XLABEL = "Block Size"
YLABEL = "Latency(ms)"
# XLABEL = "区块大小"
# YLABEL = "延迟(毫秒)"

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

# savepath = f'../pics/blocksize/bench_blocksize_{warehouse}:{thread_num}_latency.pdf'
savepath = f'./bench_blocksize_{warehouse}:{thread_num}_latency.pdf'


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
        # legend_label="Loom++" if schema == "Loom" else schema,
        legend_label=schema,
    )

# 设置X轴标签
ax.set_xticks(uniform_ticks, blocksizes)
# ax.set_xticks([int(t) for t in recs['block_size'].unique()])
# ax.set_xticklabels([str(int(t) // 100) for t in recs['block_size'].unique()])

# 自适应Y轴变化
step = None
# p.format_yticks(ax, suffix='K', step_num=4)
p.format_yticks(ax, step=10, step_num=5) # 75 15 10
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
# p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.22), columnspacing=1.5)
# p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.22), columnspacing=2.1)
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.33), columnspacing=0.6, handletextpad=0.3, labelspacing=0.2, handlelength=1.1)

# 保存
p.save(savepath)