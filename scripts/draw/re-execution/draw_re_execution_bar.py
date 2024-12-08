##### run by cmd #####
HELP = 'python draw_re_execution.py -f file_path -w warehouse -t thread'
##### run by cmd #####

# X = "warehouse"
X = "block_size"
Y = "concurrency_ratio"
XLABEL = "Degree of Concurrency"
YLABEL = "Block Size"

import pandas as pd
import argparse
import sys
import numpy as np

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas_for_reExecution
schemas = schemas_for_reExecution

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
parser.add_argument("-w", "--warehouse", type=str, required=True, help="warehouse: warehouse number")
parser.add_argument("-t", "--thread", type=str, required=True, help="thread: thread number")
args = parser.parse_args()
file: str = args.file
warehouse = args.warehouse
thread_num = args.thread

# savepath = f'../../pics/re-execution/bench_re-execution_{block_size}:{thread_num}.pdf'
savepath = f'./bench_re-execution_{warehouse}:{thread_num}_bar.pdf'


#################### 数据准备 ####################
if (file.endswith('csv')):
    recs = pd.read_csv(file)
inner_schemas = ['Loom', 'Harmony', 'Aria']
print(inner_schemas)
# blocksizes = [1600, 400, 100]
blocksizes = [100, 400, 1600]
data = {schema: [] for schema in inner_schemas}

for block_size in blocksizes:
    for schema in inner_schemas:
        # 假设在DataFrame中，你有类似于 'block_size' 和 'schema' 的列
        filtered_records = recs[(recs[X] == block_size) & (recs['protocol'] == schema)]
        # 假设你有一个'count'列记录被中止的事务数
        concurrency = filtered_records[Y].mean()
        data[schema].append(concurrency)

print(data)

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)


print(blocksizes)
uniform_ticks = np.arange(len(blocksizes))

# 设置柱状图的宽度和标签
width = 0.25  # 每个柱子的宽度
bar_positions = np.arange(len(blocksizes))

# for idx, (schema, color) in enumerate(schemas):
for idx, (schema, color) in enumerate(schemas):
    ax.barh(
        bar_positions + idx * width, 
        data[schema], 
        width, 
        label=schema,
        color=color,
        hatch=['||', '\\\\', 'xx', '--', '++', '//'][idx % 6],
        ec='black', ls='-', lw=1
    )


# 设置 Y 轴的刻度标签
ax.set_yticks(bar_positions + width)
ax.set_yticklabels(blocksizes)

# 设置X轴标签
# ax.set_xticks(uniform_ticks, blocksizes)
# ax.set_xticks(range(0, 25, 8), [str(x) for x in range(0, 25, 8)])
ax.set_xticks(range(0, 31, 10), [str(x) for x in range(0, 31, 10)])
# 设置label
p.set_labels(ax, XLABEL, YLABEL)
# ax.set_ylabel(YLABEL, labelpad=-10)
# box1: plt.Bbox = ax.get_window_extent()
# box2: plt.Bbox = ax.get_tightbbox()

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.15))

# 保存
p.save(savepath)