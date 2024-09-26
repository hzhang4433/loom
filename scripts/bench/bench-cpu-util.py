import psutil
import time
import csv
import os

# 监测时长（秒）
DURATION = 10

# 采样间隔（秒）
INTERVAL = 1
# 采样间隔（毫秒）
INTERVAL_MS = 25

# 初始化统计变量
cpu_count = 48
samples = []

# 设置 CPU 亲和性，只使用 48 号及之后的 CPU 核心
os.sched_setaffinity(0, range(48, psutil.cpu_count()))

# 监测 CPU 性能
end_time = time.time() + DURATION
while time.time() < end_time:
    # cpu_times = psutil.cpu_times_percent(interval=INTERVAL, percpu=True)
    cpu_times = psutil.cpu_percent(interval=None, percpu=True)
    total_user = 0
    total_system = 0
    total_idle = 0
    for i, cpu_percent in enumerate(cpu_times):
        if i >=0 and i < 48:
            # print("cpu", i, ":", cpu_percent)
            total_user += cpu_percent
    avg_user = total_user / cpu_count
    # avg_system = total_system / cpu_count
    # avg_idle = total_idle / cpu_count
    avg_idle = 100 - avg_user
    # samples.append((avg_user, avg_system, avg_idle))
    samples.append((avg_user, avg_idle))
    # print(f"Average User CPU Usage: {avg_user:.2f}%")
    time.sleep(INTERVAL_MS / 1000.0)

output_file = '../exp_results/ablation/cpu_usage_loom.csv'
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Sample', 'Average_CPU_Util', 'Average_Idle'])
    for i, (avg_user, avg_idle) in enumerate(samples, 1):
        writer.writerow([i, avg_user, avg_idle])