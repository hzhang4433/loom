##### run by cmd #####
HELP = 'python draw_overall_latency.py -w warehouse -b blocksize'
##### run by cmd #####

Y = "tx_latency"
XLABEL = "Execution Schemes"
YLABEL = "Latency(ms)"

import pandas as pd
import argparse
import sys

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas_for_bar
schemas = schemas_for_bar

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-w", "--warehouse", type=int, required=True, help="warehouse: 1 or 20")
parser.add_argument("-b", "--blocksize", type=int, required=True, help="blocksize: 400 or 1600")
args = parser.parse_args()


#################### 数据准备 ####################
# recs = pd.read_csv('../../exp_results/overall/overall.csv')
recs = pd.read_csv('../../exp_results/0optme/overall/overall.csv')
assert args.blocksize in recs['block_size'].unique()
blocksize = args.blocksize
recs = recs[recs['block_size'] == blocksize]
warehouse = args.warehouse
recs = recs[recs['warehouse'] == warehouse]
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

savepath = f'overall_latency.pdf'
# savepath = f'../../pics/overall/block-overall-latency-{warehouse}:{blocksize}.pdf'

#################### 画图 ####################
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


for idx, (schema, color) in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    # print(records[Y])
    p.bar(
        ax_bottom,
        xdata=[schema],
        ydata=records[Y],
        color=color, legend_label=schema,
        width=0.7,
        hatch=['//', '\\\\', '--', '||', '++', 'xx'][idx % 6],
    )

# 设置X轴标签
# ax.set_xlim(-0.6, 1.6)
ax_bottom.set_xticks(range(len(schemas)))
ax_bottom.set_xticklabels(
    [schema[:3] + '.' if schema in ['Harmony'] else 
     schema[:3] + '.' if schema in ['Fractal'] else
     schema[:3] + '.' if schema in ['OptME'] else
     schema[:3] + '.' + schema[7:] if schema == 'HarmonyIB' else
     schema for (schema, _) in schemas]
)

# 自适应Y轴变化
# ax_bottom.set_ylim(0, (recs[recs['protocol'] == 'Loom'][Y].max()) * 1.25)
# ax_top.set_ylim(recs[Y].max() * 0.4, recs[Y].max() * 1.4)
# p.format_yticks(ax_bottom, max_y_data=int((recs[recs['protocol'] == 'Loom'][Y].max()) * 1.2), step_num=4)
p.format_yticks(ax_bottom, step=15 if blocksize == 40 else 70, step_num=5)
# ax_top.set_yticks([int(recs[Y].max() * 0.52), int(recs[Y].max() * 1.4)], ['150', '400']) #

# 设置label
p.set_labels(ax_bottom, XLABEL, YLABEL)

# 设置图例
# p.legend(ax_bottom, loc="upper center", ncol=3, anchor=(0.5, 1.23), columnspacing=1.6)#, columnspacing=2.5
p.legend(ax_bottom, loc="upper center", ncol=3, anchor=(0.5, 1.23), columnspacing=2.2)#, columnspacing=2.5

# 保存
p.save(savepath)