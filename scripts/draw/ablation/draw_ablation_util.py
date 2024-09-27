##### run by cmd #####
HELP = 'python draw_ablation_util.py'
##### run by cmd #####

X = "Sample"
Y = "Average_CPU_Util"
XLABEL = "Time(s)"
YLABEL = "CPU Util(%)"

import pandas as pd
import numpy as np
import sys
from scipy.interpolate import make_interp_spline

sys.path.extend(['.', '..', '../..'])
import matplotlib.pyplot as plt
from plot.plot import MyPlot

#################### 数据准备 ####################
# 读取 csv 文件中的数据
def read_csv(file_path):
    data = pd.read_csv(file_path)
    return data[X].tolist(), data[Y].tolist()

# 从每个文件中读取数据
time1, cpu_util1 = read_csv('../../exp_results/ablation/cpu_usage_loom.csv')
time2, cpu_util2 = read_csv('../../exp_results/ablation/cpu_usage_loomFR.csv')
time3, cpu_util3 = read_csv('../../exp_results/ablation/cpu_usage_loomRaw.csv')

savepath = '../../pics/ablation/bench_ablation_cpu-util.pdf'
# savepath = 'bench_ablation_cpu-util.pdf'

#################### 画图 ####################
p = MyPlot(1, 1)
ax: plt.Axes = p.axes
# ax.grid(False)
p.init(ax)

# 为了使曲线更加平滑，使用样条插值
def smooth_curve(x, y):
    # 生成更多的点来平滑曲线
    x_new = np.linspace(min(x), max(x), 300)  # 300个插值点
    spl = make_interp_spline(x, y, k=3)  # 使用三次样条插值
    y_smooth = spl(x_new)
    return x_new, y_smooth

# 为时间轴创建 0 到 100 秒范围的值
time_axis = np.linspace(0, 100, len(time1))

# 绘制时序图
x_smooth3, y_smooth3 = smooth_curve(time_axis, cpu_util3)
p.plot(ax, x_smooth3, y_smooth3, legend_label="LoomRaw", marker="None", nogrid=True, color="#D0CECE") # D0CECE/BFBFBF
# ax.lines[-1].set_linestyle('--')  # 设置为虚线

x_smooth2, y_smooth2 = smooth_curve(time_axis, cpu_util2)
p.plot(ax, x_smooth2, y_smooth2, legend_label="LoomFR", marker="None", nogrid=True, color="#5075BF")
ax.lines[-1].set_linestyle('--')  # 设置为虚线

x_smooth1, y_smooth1 = smooth_curve(time_axis, cpu_util1)
p.plot(ax, x_smooth1, y_smooth1, legend_label="Loom", marker="None", nogrid=True, color="#F28807")
# ax.lines[-1].set_linestyle('--')  # 设置为虚线

# 设置X轴标签
ax.set_xticks(np.arange(0, 101, 10))

# 设置Y轴标签
ax.set_yticks(np.arange(0, 101, 25))

# 设置坐标轴标签
p.set_labels(ax, XLABEL, YLABEL)
ax.set_ylabel(YLABEL, loc='center', labelpad=0)


# 添加图例
p.legend(ax, loc="upper center", ncol=3, anchor=(0.48, 1.25), columnspacing=3) #, anchor=(0.5, 1.2), columnspacing=0.5

# 保存图像
p.save(savepath)
