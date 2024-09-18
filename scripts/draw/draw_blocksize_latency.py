##### run by cmd #####
HELP = 'python draw_blocksize_latency.py -f file_path -w warehouse -t thread'
##### run by cmd #####

X = "block_size"
Y = "tx_latency"
XLABEL = "Block Size(X100)"
YLABEL = "Latency(ms)"

import pandas as pd
import argparse
import sys
import re

sys.path.extend(['.', '..', '../..'])
from plot.parse import parse_records_from_file
import matplotlib.pyplot as plt
from plot.plot import MyPlot

#################### 参数解析 ####################
parser = argparse.ArgumentParser(HELP)
parser.add_argument('-f', '--file', type=str, required=True, help='file to plot')
parser.add_argument("-w", "--warehouse", type=str, required=True, help="warehouse: warehouse number")
parser.add_argument("-t", "--thread", type=str, required=True, help="thread: thread number")
args = parser.parse_args()
file: str = args.file
# assert args.workload in ['smallbank', 'ycsb']
warehouse = args.warehouse
# assert args.contention in ['uniform', 'skewed']
thread_num = args.thread

savepath = f'../pics/bench_blocksize_{warehouse}:{thread_num}_latency_.pdf'


#################### 数据准备 ####################
if (file.endswith('csv')):
    recs = pd.read_csv(file)
else:
    df = pd.DataFrame(columns=['protocol', 'warehouse', 'threads', 'table_partition', 'commit', 'overhead', 'rollback', 'tx_latency', 'block_latency', 'tps'])
    with open(file, 'r') as f:
        content = f.read()
        c_list = content.split('#COMMIT-')
        for c in c_list[1:]:
            c = c.split('CONFIG-')
            hash = c[0]
            c = c[1].split('\n')
            cc = c[0]
            result_str = c[1]

            # 在 result_str 中提取 warehouse 数值
            warehouse_match = re.search(r'TPCC:(\d+):', result_str)
            warehouse = int(warehouse_match.group(1)) if warehouse_match else None

            num_threads = int(cc.split(':')[1])
            table_partitions = int(cc.split(':')[2])
            commit = float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
            execution = float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
            overhead = float(re.search(r'overhead\s+([\d.]+)', result_str).group(1))
            rollback = float(re.search(r'rollback\s+([\d.]+)', result_str).group(1))
            tx_latency = float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1))
            block_latency = float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1))
            tps = float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1))
            df.loc[len(df)] = {
                'protocol': cc.split(':')[0] if cc.split(':')[-1] != 'FALSE' else 'LoomNIB', 
                'warehouse': warehouse,
                'threads': num_threads, 
                'table_partition': table_partitions, 
                'commit': commit,
                # 'abort': execution - commit,
                'overhead': overhead,
                'rollback': rollback,
                'tx_latency': tx_latency,
                'block_latency': block_latency,
                'tps': tps,
            }
    recs = df
schemas = recs['protocol'].unique()
print(schemas)

# schemas = [
#     # 里面是 (协议名称, 颜色(RGB格式)的元组)
#     ('Calvin'           ,       '#45C686'),
#     ('Aria'             ,       '#ED9F54'),
#     ('AriaRe'           ,       '#ED9F54'),
#     ('Sparkle'          ,       '#8E5344'),
#     ('Spectrum'         ,       '#8E5344'),
# ]

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
ax.grid(axis=p.grid, linewidth=p.border_width)
p.init(ax)

# for idx, (schema, color) in enumerate(schemas):
marker_list = ['v', 's', 'o', '^', '<', '>', 'D', 'h'] 
for idx, schema in enumerate(schemas):
    records = recs[recs['protocol'] == schema]
    # print(records[Y])
    p.plot(
        ax,
        xdata=records[X],
        ydata=records[Y],
        color=None, legend_label=schema,
        # marker=marker_list[idx % len(marker_list)]
    )

print(recs['block_size'].unique())

# 设置X轴标签
ax.set_xticks([int(t) for t in recs['block_size'].unique()])
ax.set_xticklabels([str(int(t) // 100) for t in recs['block_size'].unique()])

# 自适应Y轴变化
# p.format_yticks(ax, suffix='K', step_num=4)
# ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍

# 设置label
p.set_labels(ax, XLABEL, YLABEL)

# 设置图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))

# 保存
p.save(savepath)