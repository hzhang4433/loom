import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import re
import time
import sys
import itertools

sys.path.extend(['.', '..', '../..'])
from plot.plot import MyPlot

workload = 'TPCC'
repeat = 20
times_to_tun = 2
warehouse = 40 #1 20 60
block_num = 2
thread_num = 48 #36
table_partition = 9973
timestamp = int(time.time())

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'block_size', 'warehouse', 'threads', 'table_partition', 'commit', 'overhead', 'rollback', 'tx_latency', 'block_latency', 'tps'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    with open(f'./exp_results/bench_blocksize_{warehouse}:{thread_num}_{timestamp}', 'w') as f:
        # list(range(50, 101, 10)) / list(range(100, 1501, 100)) / [1000]
        for block_size in itertools.chain(range(25, 100, 25), range(100, 1001, 100)):
            protocols = [
                f"Serial:{1}:{table_partition}",
                # f"Aria:{thread_num}:{table_partition}:FALSE",
                f"Aria:{thread_num}:{table_partition}:TRUE",
                # f"Harmony:{thread_num}:{table_partition}:FALSE",
                f"Harmony:{thread_num}:{table_partition}:TRUE",
                f"Moss:{thread_num}:{table_partition}",
                f"Loom:{thread_num}:{table_partition}:TRUE:FALSE",
                f"Loom:{thread_num}:{table_partition}:TRUE:TRUE",
            ]
            for cc in protocols:
                sum_commit = 0
                sum_execution = 0
                sum_overhead = 0
                sum_rollback = 0
                sum_tx_latency = 0
                sum_block_latency = 0
                sum_tps = 0

                if cc.split(':')[0] in ['Moss', 'Loom']:
                    is_nest = 'TRUE'
                else:
                    is_nest = 'FALSE'
                
                print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
                f.write(f"#COMMIT-{hash} CONFIG-{cc}\n")
                print(f'Protocol: {cc} {workload}:{warehouse}:{block_size}:{block_num}:{is_nest} {times_to_tun}s')
                f.write(f'Protocol: {cc} {workload}:{warehouse}:{block_size}:{block_num}:{is_nest} {times_to_tun}s' + '\n')

                if cc.split(':')[0] == 'Loom' and cc.split(':')[-1] == 'TRUE':
                    tx_latency = float('inf')
                    block_latency = float('inf')
                    tps = float('-inf')
                elif cc.split(':')[0] == 'Loom' and cc.split(':')[-1] == 'FALSE':
                    tx_latency = float('-inf')
                    block_latency = float('-inf')
                    tps = float('inf')
                succeed_repeat = 0
                for _ in range(repeat):
                    try:
                        result = subprocess.run(["../build/bench", cc, f"{workload}:{warehouse}:{block_size}:{block_num}:{is_nest}", f"{times_to_tun}s"], **conf)
                        result_str = result.stderr.decode('utf-8').strip()
                        f.write(result_str + '\n')
                        sum_commit += float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
                        sum_execution += float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
                        sum_overhead += float(re.search(r'overhead\s+([\d.]+)', result_str).group(1))
                        sum_rollback += float(re.search(r'rollback\s+([\d.]+)', result_str).group(1))
                        if cc.split(':')[0] == 'Loom' and cc.split(':')[-1] == 'TRUE':
                            tx_latency = min(tx_latency, float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1)))
                            block_latency = min(block_latency, float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1)))
                            tps = max(tps, float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1)))
                        elif cc.split(':')[0] == 'Loom' and cc.split(':')[-1] == 'FALSE':
                            tx_latency = max(tx_latency, float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1)))
                            block_latency = max(block_latency, float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1)))
                            tps = min(tps, float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1)))
                        else:
                            sum_tx_latency += float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_block_latency += float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_tps += float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1))
                        succeed_repeat += 1
                    except Exception as e:
                        print(e)
                df.loc[len(df)] = {
                    'protocol': cc.split(':')[0] if cc.split(':')[-1] != 'FALSE' else 'LoomNIB', 
                    'block_size': block_size,
                    'warehouse': warehouse,
                    'threads': thread_num,
                    'table_partition': table_partition, 
                    'commit': sum_commit / succeed_repeat,
                    'overhead': sum_overhead / succeed_repeat,
                    'rollback': sum_rollback / succeed_repeat,
                    'tx_latency': tx_latency if (cc.split(':')[0] == 'Loom') else sum_tx_latency / succeed_repeat,
                    'block_latency': block_latency if (cc.split(':')[0] == 'Loom') else sum_block_latency / succeed_repeat,
                    'tps': tps if (cc.split(':')[0] == 'Loom') else sum_tps / succeed_repeat,
                }
                print(df)
    df.reset_index(inplace=True)
    df.to_csv(f'./exp_results/bench_blocksize_{warehouse}:{thread_num}_{timestamp}.csv', index=False)

# Plot the results
# for tps
    recs = df
    X, XLABEL = "block_size", "Block Size(X100)"
    Y, YLABEL = "tps", "Troughput(Txn/s)"
    p = MyPlot(1, 1)
    ax: plt.Axes = p.axes
    ax.grid(axis=p.grid, linewidth=p.border_width)
    p.init(ax)
    for idx, schema in enumerate(recs['protocol'].unique()):
        records = recs[recs['protocol'] == schema]
        p.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    ax.set_xticks([int(t) for t in recs['block_size'].unique()])
    ax.set_xticklabels([str(int(t) // 100) for t in recs['block_size'].unique()])
    p.format_yticks(ax, suffix='K')
    # ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍
    p.set_labels(ax, XLABEL, YLABEL)
    p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))
    p.save(f'./pics/bench_blocksize_{warehouse}:{thread_num}_tps_{timestamp}.pdf')

# for latency
    recs = df
    X, XLABEL = "block_size", "Block Size(X100)"
    Y, YLABEL = "tx_latency", "Latency(ms)"
    p2 = MyPlot(1, 1)
    ax: plt.Axes = p2.axes
    ax.grid(axis=p2.grid, linewidth=p2.border_width)
    p2.init(ax)
    for idx, schema in enumerate(recs['protocol'].unique()):
        records = recs[recs['protocol'] == schema]
        p2.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    ax.set_xticks([int(t) for t in recs['block_size'].unique()])
    ax.set_xticklabels([str(int(t) // 100) for t in recs['block_size'].unique()])
    # p2.format_yticks(ax, suffix='K')
    # ax.set_ylim(None, p2.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍
    p2.set_labels(ax, XLABEL, YLABEL)
    p2.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))
    p2.save(f'./pics/bench_blocksize_{warehouse}:{thread_num}_latency_{timestamp}.pdf')