import subprocess
import matplotlib.pyplot as plt
import pandas as pd
import re
import time
import sys

sys.path.extend(['.', '..', '../..'])
from plot.plot import MyPlot

workload = 'TPCC'
repeat = 30
times_to_tun = 3
block_size = 1600
block_num = 2
warehouse = 1
table_partition = 9973
timestamp = int(time.time())

def get_name(protocol):
    if protocol.split(':')[-2] == 'TRUE' and protocol.split(':')[-1] == 'TRUE':
        return 'Loom'
    elif protocol.split(':')[-2] == 'TRUE' and protocol.split(':')[-1] == 'FALSE':
        return 'LoomFR'
    elif protocol.split(':')[-2] == 'FALSE' and protocol.split(':')[-1] == 'TRUE':
        return 'LoomMP'
    elif protocol.split(':')[-2] == 'FALSE' and protocol.split(':')[-1] == 'FALSE':
        return 'LoomRaw'

if __name__ == '__main__':
    df = pd.DataFrame(columns=['protocol', 'block_size', 'warehouse', 'threads', 'table_partition', 'commit', 'overhead', 'rollback', 'rollback_ratio', 'tx_latency', 'block_latency', 'execution_latency', 'rollback_latency', 'reExecute_latency', 'concurrency_ratio', 'tps'])
    conf = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
    hash = subprocess.run(["git", "rev-parse", "HEAD"], **conf).stdout.decode('utf-8').strip()
    with open(f'../exp_results/ablation/bench_ablation_{warehouse}:{block_size}_{timestamp}', 'w') as f:
        # list(range(8, 49, 4)) / [36, 40, 44, 48]
        for thread_num in list(range(24, 49, 4)):
            protocols = [
                # f"Loom:{thread_num}:{table_partition}:FALSE:FALSE", # Loom-raw
                # f"Loom:{thread_num}:{table_partition}:TRUE:FALSE", # Loom-FR
                f"Loom:{thread_num}:{table_partition}:FALSE:TRUE", # Loom-MP
                # f"Loom:{thread_num}:{table_partition}:TRUE:TRUE", # Loom
            ]
            for cc in protocols:
                sum_commit = 0
                sum_execution = 0
                sum_overhead = 0
                sum_rollback = 0
                sum_tx_latency = 0
                sum_block_latency = 0
                sum_tps = 0
                sum_rollback_ratio = 0
                sum_execution_latency = 0
                sum_rollback_latency = 0
                sum_reExecute_latency = 0
                sum_concurrency_ratio = 0
                is_nest = 'TRUE'
                
                print(f"#COMMIT-{hash}",  f"CONFIG-{cc}")
                f.write(f"#COMMIT-{hash} CONFIG-{cc}\n")
                print(f'Protocol: {cc} {workload}:{warehouse}:{block_size}:{block_num}:{is_nest} {times_to_tun}s')
                f.write(f'Protocol: {cc} {workload}:{warehouse}:{block_size}:{block_num}:{is_nest} {times_to_tun}s' + '\n')
                
                if cc.split(':')[-1] == 'TRUE':
                    tx_latency = float('inf')
                    block_latency = float('inf')
                    tps = float('-inf')
                # elif cc.split(':')[-2] == 'TRUE' and cc.split(':')[-1] == 'FALSE':
                #     tx_latency = float('-inf')
                #     block_latency = float('-inf')
                #     tps = float('inf')
                concurrency_ratio = 0
                succeed_repeat = 0
                for _ in range(repeat):
                    try:
                        result = subprocess.run(["../../build/bench", cc, f"{workload}:{warehouse}:{block_size}:{block_num}:{is_nest}", f"{times_to_tun}s"], **conf)
                        result_str = result.stderr.decode('utf-8').strip()
                        f.write(result_str + '\n')
                        sum_commit += float(re.search(r'commit\s+([\d.]+)', result_str).group(1))
                        sum_execution += float(re.search(r'execution\s+([\d.]+)', result_str).group(1))
                        sum_overhead += float(re.search(r'overhead\s+([\d.]+)', result_str).group(1))
                        sum_rollback += float(re.search(r'rollback\s+([\d.]+)', result_str).group(1))
                        sum_rollback_ratio += float(re.search(r'rollback ratio\s+([\d.]+)', result_str).group(1))
                        if cc.split(':')[-1] == 'TRUE':
                            tx_latency = min(tx_latency, float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1)))
                            block_latency = min(block_latency, float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1)))
                            tps = max(tps, float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1)))
                            temp_concurrency_ratio = float(re.search(r'concurrency ratio\s+([\d.]+)', result_str).group(1))
                            if (temp_concurrency_ratio > concurrency_ratio):
                                concurrency_ratio = temp_concurrency_ratio
                                reExecute_latency = float(re.search(r're-execute latency\s+([\d.]+)\s+ms', result_str).group(1))
                                rollback_latency = float(re.search(r'rollback latency\s+([\d.]+)\s+ms', result_str).group(1))
                                execution_latency = float(re.search(r'execute latency\s+([\d.]+)\s+ms', result_str).group(1))
                        # elif cc.split(':')[-2] == 'TRUE' and cc.split(':')[-1] == 'FALSE':
                        #     tx_latency = max(tx_latency, float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1)))
                        #     block_latency = max(block_latency, float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1)))
                        #     tps = min(tps, float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1)))
                        #     temp_concurrency_ratio = float(re.search(r'concurrency ratio\s+([\d.]+)', result_str).group(1))
                        #     if (temp_concurrency_ratio > concurrency_ratio):
                        #         concurrency_ratio = temp_concurrency_ratio
                        #         reExecute_latency = float(re.search(r're-execute latency\s+([\d.]+)\s+ms', result_str).group(1))
                        #         rollback_latency = float(re.search(r'rollback latency\s+([\d.]+)\s+ms', result_str).group(1))
                        #         execution_latency = float(re.search(r'execute latency\s+([\d.]+)\s+ms', result_str).group(1))
                        else:
                            sum_tx_latency += float(re.search(r'tx latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_block_latency += float(re.search(r'block latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_tps += float(re.search(r'tps\s+([\d.]+)\s+tx/s', result_str).group(1))
                            sum_execution_latency += float(re.search(r'execute latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_rollback_latency += float(re.search(r'rollback latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_reExecute_latency += float(re.search(r're-execute latency\s+([\d.]+)\s+ms', result_str).group(1))
                            sum_concurrency_ratio += float(re.search(r'concurrency ratio\s+([\d.]+)', result_str).group(1))
                        succeed_repeat += 1
                    except Exception as e:
                        print(e)
                df.loc[len(df)] = {
                    'protocol': get_name(cc),
                    'block_size': block_size,
                    'warehouse': warehouse,
                    'threads': thread_num,
                    'table_partition': table_partition, 
                    'commit': sum_commit / succeed_repeat,
                    'overhead': sum_overhead / succeed_repeat,
                    'rollback': sum_rollback / succeed_repeat,
                    'rollback_ratio': sum_rollback_ratio / succeed_repeat,
                    'tx_latency': tx_latency if (cc.split(':')[-1] == 'TRUE') else sum_tx_latency / succeed_repeat,
                    'block_latency': block_latency if (cc.split(':')[-1] == 'TRUE') else sum_block_latency / succeed_repeat,
                    'execution_latency': execution_latency if (cc.split(':')[-1] == 'TRUE') else sum_execution_latency / succeed_repeat,
                    'rollback_latency': rollback_latency if (cc.split(':')[-1] == 'TRUE') else sum_rollback_latency / succeed_repeat,
                    'reExecute_latency': reExecute_latency if (cc.split(':')[-1] == 'TRUE') else sum_reExecute_latency / succeed_repeat,
                    'concurrency_ratio': concurrency_ratio if (cc.split(':')[-1] == 'TRUE') else sum_concurrency_ratio / succeed_repeat,
                    'tps': tps if (cc.split(':')[-1] == 'TRUE') else sum_tps / succeed_repeat,
                }
                print(df)
    df.reset_index(inplace=True)
    df.to_csv(f'../exp_results/ablation/bench_ablation_{warehouse}:{block_size}_{timestamp}.csv', index=False)

# Plot the results
# for tps
    recs = df
    X, XLABEL = "threads", "Threads"
    Y, YLABEL = "tps", "Troughput(Txn/s)"
    p = MyPlot(1, 1)
    ax: plt.Axes = p.axes
    ax.grid(axis=p.grid, linewidth=p.border_width)
    p.init(ax)
    for idx, schema in enumerate(recs['protocol'].unique()):
        records = recs[recs['protocol'] == schema]
        p.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    ax.set_xticks([int(t) for t in recs['threads'].unique()])
    p.format_yticks(ax, suffix='K')
    p.set_labels(ax, XLABEL, YLABEL)
    p.legend(ax, loc="upper center", ncol=2, anchor=(0.5, 1.25))
    p.save(f'../pics/ablation/bench_ablation_{warehouse}:{block_size}_tps_{timestamp}.pdf')
    
# for latency
    recs = df
    X, XLABEL = "threads", "Threads"
    Y, YLABEL = "tx_latency", "Latency(ms)"
    p2 = MyPlot(1, 1)
    ax: plt.Axes = p2.axes
    ax.grid(axis=p2.grid, linewidth=p2.border_width)
    p2.init(ax)
    for idx, schema in enumerate(recs['protocol'].unique()):
        records = recs[recs['protocol'] == schema]
        p2.plot(ax, xdata=records[X], ydata=records[Y], color=None, legend_label=schema,)
    ax.set_xticks([int(t) for t in recs['threads'].unique()])
    p2.set_labels(ax, XLABEL, YLABEL)
    p2.legend(ax, loc="upper center", ncol=2, anchor=(0.5, 1.25))
    p2.save(f'../pics/ablation/bench_ablation_{warehouse}:{block_size}_latency_{timestamp}.pdf')