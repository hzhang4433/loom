##### run by cmd #####
HELP = 'python3 draw_ablation_latency.py -t threads'
##### run by cmd #####

X = "contention"
Y = "block_latency"
XLABEL = "Contention"
YLABEL = "Latency(ms)"
# XLABEL = "冲突度"
# YLABEL = "延迟(毫秒)"

import pandas as pd
import argparse
import sys
import numpy as np

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot
from Schemas import schemas_for_ablation_bar
schemas = schemas_for_ablation_bar

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument("-t", "--threads", type=int, required=True, help="threads")
args = parser.parse_args()

# savepath = f'../../pics/ablation/bench_ablation_latency.pdf'
# savepath = f'./bench_ablation_latency.pdf'
savepath = f'./ablation_latency.pdf'

#################### 数据准备 ####################
recs = pd.read_csv(f'../exp_results/ablation/bench_ablation_tps.csv')
assert args.threads in recs['threads'].unique()
threads = args.threads
# fliter the records with the given threads
recs = recs[recs['threads'] == threads]
print(recs)

recs_high = recs[recs['warehouse'] == 1]
# print(recs_high)
recs_low = recs[recs['warehouse'] == 20]
# print(recs_low)

inner_schemas = recs['protocol'].unique()
print(inner_schemas)

# #################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

# 横坐标：两个 contention 值：Low 和 High
contentions = [20, 1]
contentions_dict = {
    20: 'Medium', 
    1 : 'High',
}
# contentions_dict = {
#     20: '中级', 
#     1 : '高级',
# }
# 协议部分
# protocols = inner_schemas
protocols = list(inner_schemas)

# Create a dictionary to store data for each protocol under each contention level
data = {protocol: {'execution': [], 'rollback': [], 're-execution': []} for protocol in protocols}
colors = ['#4c7da8', '#e8ac64', '#9c504e']
hatches=['||', '++', 'xx', '//', '\\\\', '++']

# Iterate over each contention value
for contention in contentions:
    # Iterate over each protocol
    for protocol in protocols:
        # Filter records for the current protocol and contention level
        records = recs[(recs['protocol'] == protocol) & (recs['warehouse'] == contention)]
        # Append the mean block latency to the data for the current protocol
        data[protocol]['execution'].append(records['execution_latency'].mean())
        data[protocol]['rollback'].append(records['rollback_latency'].mean())
        data[protocol]['re-execution'].append(records['reExecute_latency'].mean())
# print(data)

# 绘制柱状图，分别为低冲突和高冲突下每个协议的表现
width = 0.22  # 柱子的宽度

x = np.arange(len(contentions))
for idx, (protocol, color) in enumerate(schemas):
    # 每个 contention (Low/High) 下绘制 3 个协议的柱状图
    x_pos = [_ + (idx - 1) * 0.28 for _ in range(len(contentions))]
    # x_pos = x + (idx - len(protocols)/2) * width + gap/2

    p.bar_new(
        ax,
        xdata=x_pos,  # 调整偏移量以避免重叠
        ydata=data[protocol]['execution'],
        color=colors[0], 
        legend_label=None,
        width=width,
        hatch=hatches[idx],  # 每个协议对应不同的花纹
    )

    # plot rollback
    p.bar_new(
        ax,
        xdata=x_pos,
        ydata=data[protocol]['rollback'],
        bottom=data[protocol]['execution'],
        color=colors[1], 
        legend_label=None,
        width=width,
        hatch=hatches[idx],  # 每个协议对应不同的花纹
    )

    # plot re-execution
    p.bar_new(
        ax,
        xdata=x_pos,
        ydata=data[protocol]['re-execution'],
        bottom=np.array(data[protocol]['execution']) + np.array(data[protocol]['rollback']),
        color=colors[2], 
        legend_label=None,
        width=width,
        hatch=[hatches[idx]],  # 每个协议对应不同的花纹
    )

# 设置X轴标签
ax.set_xticks(range(len(contentions)))
ax.set_xticklabels([contentions_dict[contention] for contention in contentions])
ax.set_xlim(-0.6, 1.6)

# 自适应Y轴变化
p.format_yticks(ax, step=45, step_num=5)

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
protocol_handles = [
    plt.Rectangle((0, 0), 1, 1, facecolor='white', hatch=hatches[i], edgecolor='black')
    for i in range(len(protocols))
]

execution_patch = plt.Rectangle((0, 0), 1, 1, color=colors[0])
rollback_patch = plt.Rectangle((0, 0), 1, 1, color=colors[1])
re_execution_patch = plt.Rectangle((0, 0), 1, 1, color=colors[2])

# 使用 ax.figure.legend() 分别为两个图例创建图例对象
legend_protocols = ax.figure.legend(
    handles=protocol_handles, 
    # labels=['Loom', 'Loom+', 'Loom++'], 
    labels=protocols, 
    loc='upper center', 
    ncol=len(protocols), 
    bbox_to_anchor=(0.51, 1),
    columnspacing=2.65,
    frameon=False
)
legend_parts = ax.figure.legend(
    handles=[execution_patch, rollback_patch, re_execution_patch], 
    # labels=['预执行', '回滚', '重执行'], 
    labels=['Pre-Execution', 'Rollback', 'Re-Execution'], 
    loc='upper left',
    bbox_to_anchor=(0.14, 0.87),
    handletextpad=0.5,  # 缩小图例图标和文字之间的水平间距
    labelspacing=0.2,   # 缩小图例条目之间的垂直间距
    # frameon=False
)

# 保存
p.save(savepath)
