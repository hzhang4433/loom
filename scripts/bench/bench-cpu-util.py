import psutil
import time
import csv

# 监测时长（秒）
DURATION = 2

# 采样间隔（秒）
INTERVAL = 1
# 采样间隔（毫秒）
INTERVAL_MS = 10

# 初始化统计变量
cpu_count = 48
samples = []

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
            print("cpu", i, ":", cpu_percent)
            total_user += cpu_percent
    avg_user = total_user / cpu_count
    # avg_system = total_system / cpu_count
    # avg_idle = total_idle / cpu_count
    avg_idle = 100 - avg_user
    # samples.append((avg_user, avg_system, avg_idle))
    samples.append((avg_user, avg_idle))
    time.sleep(INTERVAL_MS / 1000.0)

# # 输出结果
# for i, (avg_user, avg_system, avg_idle) in enumerate(samples, 1):
#     print(f"Sample {i}:")
#     print(f"Average User CPU Usage: {avg_user:.2f}%")
#     print(f"Average System CPU Usage: {avg_system:.2f}%")
#     print(f"Average Idle CPU Usage: {avg_idle:.2f}%")
#     print()

# 保存结果到文件
# output_file = 'cpu_usage.csv'
# with open(output_file, mode='w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerow(['Sample', 'Average User CPU Usage (%)', 'Average System CPU Usage (%)', 'Average Idle CPU Usage (%)'])
#     for i, (avg_user, avg_system, avg_idle) in enumerate(samples, 1):
#         writer.writerow([i, avg_user, avg_system, avg_idle])

output_file = 'cpu_usage.csv'
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Sample', 'Average User CPU Usage (%)', 'Average Idle CPU Usage (%)'])
    for i, (avg_user, avg_idle) in enumerate(samples, 1):
        writer.writerow([i, avg_user, avg_idle])