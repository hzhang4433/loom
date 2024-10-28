##### run by cmd #####
HELP = 'python draw_rollback.py -f file_path -b blocksize -t thread'
##### run by cmd #####

X = "warehouse"
Y = "rollback"
XLABEL = "Warehouse"
YLABEL = "# of Aborted Transactions"

import pandas as pd
import argparse
import sys
import numpy as np

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas_for_rollback
schemas = schemas_for_rollback

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
parser.add_argument("-b", "--blocksize", type=int, required=True, help="blocksize: 100 or 400 or 1600")
parser.add_argument("-t", "--thread", type=int, required=True, help="thread: 48")
args = parser.parse_args()
file: str = args.file
block_size = args.blocksize
thread_num = args.thread

savepath = f'../../pics/rollback/bench_rollback_{block_size}:{thread_num}.pdf'
# savepath = f'./bench_rollback_{block_size}:{thread_num}.pdf'


#################### 数据准备 ####################
if (file.endswith('csv')):
    recs = pd.read_csv(file)
inner_schemas = ['Loom', 'Harmony', 'Aria']
print(inner_schemas)
warehouse_num = [1, 20, 40, 60]
data = {schema: [] for schema in inner_schemas}

for warehouse in warehouse_num:
    for schema in inner_schemas:
        # 假设在DataFrame中，你有类似于 'warehouse' 和 'schema' 的列
        filtered_records = recs[(recs['warehouse'] == warehouse) & (recs['protocol'] == schema)]
        # print(filtered_records)
        # 假设你有一个'count'列记录被中止的事务数
        rollback_overhead = filtered_records[Y].sum()
        data[schema].append(rollback_overhead)

print(data)

# #################### 画图 ####################
p = MyPlot(1, 1)
# p.fig.clear()
# gs = p.fig.add_gridspec(4, 4, hspace=0.4)
# ax_bottom: plt.Axes = p.fig.add_subplot(gs[1:, :])
# ax_top: plt.Axes = p.fig.add_subplot(gs[:1, :])
# p.init(ax_bottom)
# p.init(ax_top)
# ax_bottom.grid(axis=p.grid, linewidth=p.border_width)
# ax_bottom.set_axisbelow(True)
# ax_top.grid(axis=p.grid, linewidth=p.border_width)
# ax_top.set_axisbelow(True)
# ax_bottom.spines.top.set_visible(False)
# ax_top.spines.bottom.set_visible(False)
# d = 0.5  # proportion of vertical to horizontal extent of the slanted line
# kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
#             linestyle="none", color='k', mec='k', mew=p.tick_width, clip_on=False)
# ax_top.plot([0, 1], [0, 0], transform=ax_top.transAxes, **kwargs)
# ax_bottom.plot([0, 1], [1, 1], transform=ax_bottom.transAxes, **kwargs)

ax_bottom: plt.Axes = p.axes
ax_bottom.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax_bottom)

bar_width = 0.28
index = np.arange(len(warehouse_num))

for idx, (schema, color) in enumerate(schemas):
    p.bar(
        ax=ax_bottom,
        xdata=index + idx * bar_width,
        ydata=data[schema],
        legend_label=schema,
        color=color,
        width=bar_width,
        hatch=['xx', '\\\\', '--', '||', '++', '//'][idx % 6],
    )

# 设置X轴标签
# ax.set_xlim(-0.6, 1.6)
# print(recs['warehouse'].unique())
# ax_bottom.set_xticks([int(t) for t in warehouse_num])
ax_bottom.set_xticks(index + bar_width)
ax_bottom.set_xticklabels(warehouse_num)


# 自适应Y轴变化
# ax_bottom.set_ylim(0, (recs[recs['protocol'] == 'Loom'][Y].max()) * 1.25)
# ax_top.set_ylim(recs[Y].max() * 0.4, recs[Y].max() * 1.4)
# p.format_yticks(ax_bottom, max_y_data=int((recs[recs['protocol'] == 'Loom'][Y].max()) * 1.2), step_num=4)
# p.format_yticks(ax_bottom, suffix='K', step_num=4)
# ax_top.set_yticks([int(recs[Y].max() * 0.52), int(recs[Y].max() * 1.4)], ['150', '400']) #

# 设置label
p.set_labels(ax_bottom, XLABEL, YLABEL)

# 设置图例
p.legend(ax_bottom, loc="upper center", ncol=3, anchor=(0.5, 1.15))

# 保存
p.save(savepath)