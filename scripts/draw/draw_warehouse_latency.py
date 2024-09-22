##### run by cmd #####
HELP = 'python draw_warehouses_tps.py -f file_path -b blocksize -t thread'
##### run by cmd #####

X = "warehouse"
Y = "tx_latency"
XLABEL = "Warehouse"
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
parser.add_argument("-b", "--blocksize", type=str, required=True, help="blocksize: size of block")
parser.add_argument("-t", "--thread", type=str, required=True, help="thread: thread number")
args = parser.parse_args()
file: str = args.file
block_size = args.blocksize
thread_num = args.thread

savepath = f'../pics/warehouse/bench_warehouse_{block_size}:{thread_num}_latency.pdf'


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

print(recs['warehouse'].unique())

# 设置X轴标签
ax.set_xticks([int(t) for t in recs['warehouse'].unique()])

# 自适应Y轴变化
step = None
# if workload == 'smallbank' and contention == 'skewed':
#     step = 140000
# elif workload == 'tpcc' and contention == '10orderlines':
#     step = 13000
# p.format_yticks(ax, step=step, step_num=4)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)
# ax.set_ylabel(YLABEL, labelpad=-10)
# box1: plt.Bbox = ax.get_window_extent()
# box2: plt.Bbox = ax.get_tightbbox()

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))

# 保存
p.save(savepath)