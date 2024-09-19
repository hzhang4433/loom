import matplotlib.pyplot as plt
import numpy as np

# 数据
block_sizes = [50, 75, 100, 400, 700, 1000]
throughput = [200, 300, 600, 1200, 1800, 2100]  # 假设 throughput 数据

# 创建图形
plt.figure(figsize=(8, 6))

# 自定义横坐标位置，使得50, 75, 100, 400, 700, 1000间隔相等
uniform_ticks = np.arange(len(block_sizes))

# 绘制图形，将block_sizes按均匀刻度绘制
plt.plot(uniform_ticks, throughput, marker='o', linestyle='-', color='b')

# 设置自定义刻度和标签
plt.xticks(uniform_ticks, block_sizes)

# 添加虚线，表示100的位置
y_max = 600
plt.vlines(x=2, ymin=0, ymax=y_max, color='gray', linestyle='--', linewidth=1)  # 因为100在列表中是第3个

# 添加标题和标签
plt.title('Throughput vs Block Size')
plt.xlabel('Block Size')
plt.ylabel('Throughput (tx/s)')

# 显示网格
# plt.grid(True, which="both", ls="--")

# 显示图形
plt.savefig("./")
