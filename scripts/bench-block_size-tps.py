import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import re
import time
import sys

sys.path.extend(['.', '..', '../..'])
from plot.plot import MyPlot

workload = 'TPCC'
repeat = 10
times_to_tun = 3
warehouse = 1
block_num = 2
thread_num = 36
table_partition = 9973
timestamp = int(time.time())

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'warehouse', 'threads', 'table_partition', 'commit', 'overhead', 'rollback', 'tx_latency', 'block_latency', 'tps'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    with open(f'./exp_results/bench_block-size_{timestamp}', 'w') as f:
        for block_size in [1000]:
            protocols = [
                # f"Serial:{table_partition}:{1}",
                # # f"Aria:{thread_num}:{table_partition}:FALSE",
                # f"Aria:{thread_num}:{table_partition}:TRUE",
                # # f"Harmony:{thread_num}:{table_partition}:FALSE",
                # f"Harmony:{thread_num}:{table_partition}:TRUE",
                f"Moss:{thread_num}:{table_partition}",
                # f"Loom:{thread_num}:{table_partition}:TRUE:TRUE",
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
                print(f'../bench {cc} {workload}:{warehouse}:{block_size}:{block_num}:{is_nest} {times_to_tun}s')
                f.write(f'../bench {cc} {workload}:{warehouse}:{block_size}:{block_num}:{is_nest} {times_to_tun}s' + '\n')

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
                        sum_tx_latency += float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1))
                        sum_block_latency += float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1))
                        sum_tps += float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1))
                        succeed_repeat += 1
                    except Exception as e:
                        print(e)
                df.loc[len(df)] = {
                    'protocol': cc.split(':')[0] if cc.split(':')[-1] != 'FALSE' else 'AriaFB', 
                    'warehouse': warehouse,
                    'threads': thread_num,
                    'table_partition': table_partition, 
                    'commit': sum_commit / succeed_repeat,
                    'overhead': sum_overhead / succeed_repeat,
                    'rollback': sum_rollback / succeed_repeat,
                    'tx_latency': sum_tx_latency / succeed_repeat,
                    'block_latency': sum_block_latency / succeed_repeat,
                    'tps': sum_tps / succeed_repeat,
                }
                print(df)
    df.reset_index(inplace=True)
    df.to_csv(f'./exp_results/bench_block-size_{timestamp}.csv', index=False)

## Plot the results
    # recs = df
    # X, XLABEL = "threads", "Threads"
    # Y, YLABEL = "commit", "Troughput(Txn/s)"
    # p = MyPlot(1, 1)
    # ax: plt.Axes = p.axes
    # ax.grid(axis=p.grid, linewidth=p.border_width)
    # p.init(ax)
    # for idx, schema in enumerate(recs['protocol'].unique()):
    #     records = recs[recs['protocol'] == schema]
    #     p.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    # ax.set_xticks([int(t) for t in recs['threads'].unique()])
    # p.format_yticks(ax, suffix='K')
    # # ax.set_ylim(None, p.max_y_data * 1.15)       # 折线图的Y轴上限设置为数据最大值的1.15倍
    # p.set_labels(ax, XLABEL, YLABEL)
    # p.legend(ax, loc="upper center", ncol=3, anchor=(0.5, 1.25))
    # p.save(f'exp_results/bench_results_{timestamp}.pdf')
