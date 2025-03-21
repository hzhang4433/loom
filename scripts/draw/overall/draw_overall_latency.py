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
recs = pd.read_csv('../../exp_results/overall/overall.csv')
assert args.blocksize in recs['block_size'].unique()
blocksize = args.blocksize
recs = recs[recs['block_size'] == blocksize]
warehouse = args.warehouse
recs = recs[recs['warehouse'] == warehouse]
inner_schemas = recs['protocol'].unique()
print(inner_schemas)

savepath = f'block-overall-latency-{warehouse}:{blocksize}.pdf'
# savepath = f'../../pics/overall/block-overall-latency-{warehouse}:{blocksize}.pdf'

#################### 画图 ####################
p = MyPlot(1, 1)
p.fig.clear()
gs = p.fig.add_gridspec(4, 4, hspace=0.4)
ax_bottom: plt.Axes = p.fig.add_subplot(gs[1:, :])
ax_top: plt.Axes = p.fig.add_subplot(gs[:1, :])
p.init(ax_bottom)
p.init(ax_top)
ax_bottom.grid(axis=p.grid, linewidth=p.border_width)
ax_bottom.set_axisbelow(True)
ax_top.grid(axis=p.grid, linewidth=p.border_width)
ax_top.set_axisbelow(True)
ax_bottom.spines.top.set_visible(False)
ax_top.spines.bottom.set_visible(False)
d = 0.5  # proportion of vertical to horizontal extent of the slanted line
kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
            linestyle="none", color='k', mec='k', mew=p.tick_width, clip_on=False)
ax_top.plot([0, 1], [0, 0], transform=ax_top.transAxes, **kwargs)
ax_bottom.plot([0, 1], [1, 1], transform=ax_bottom.transAxes, **kwargs)

for idx, (schema, color) in enumerate(schemas):
    # if schema in ['Serial', 'Calvin']: continue
    records = recs[recs['protocol'] == schema]
    # print(schema, '\t', (1000000 / records[Y] * 100).max())
    p.bar(
        ax_bottom,
        xdata=[schema],
        ydata=records[Y],
        color=color, legend_label=schema,
        width=0.7,
        hatch=['//', '\\\\', '--', '||', '++', 'xx'][idx % 6],
    )
    p.bar(
        ax_top,
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
    [schema[:4] + '.' if schema in ['Harmony', 'Fractal'] else 
     schema[:4] + '.' + schema[7:] if schema == 'HarmonyIB' else
     schema for (schema, _) in schemas]
)
# ax_bottom.set_xticklabels([schema[:4] if schema not in ['AriaFB'] else schema for (schema, _) in schemas])
ax_top.set_xticks([])

# 自适应Y轴变化
ax_bottom.set_ylim(0, ((recs[recs['protocol'] == 'Loom'][Y].max()) * 1.25) if blocksize == 1600 else ((recs[recs['protocol'] == 'Fractal'][Y].max()) * 1.25))
ax_top.set_ylim(recs[Y].max() * 0.4, recs[Y].max() * 1.22) # 1.4
p.format_yticks(ax_bottom, max_y_data=(int((recs[recs['protocol'] == 'Loom'][Y].max()) * 1.2)) if blocksize==1600 else (int((recs[recs['protocol'] == 'Fractal'][Y].max()) * 1.2)), step_num=4)
ax_top.set_yticks([int(recs[Y].max() * 0.62), int(recs[Y].max() * 1.22)], ['150', '400'] if blocksize == 1600 else None) # *0.52 *1.4 for 1600

# 设置label
p.set_labels(ax_bottom, XLABEL, YLABEL)
ax_bottom.set_ylabel('Latency(ms)', loc='top')

# 设置图例
p.legend(ax_bottom, loc="upper center", ncol=3, anchor=(0.5, 1.7))

# 保存
p.save(savepath)