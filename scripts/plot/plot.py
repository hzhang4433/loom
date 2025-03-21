from collections.abc import Iterable
from typing import List
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib import rcParams
import os

from plot.common import order_handles_labels

class MyPlot:
    ##### 参数 #####
    language = 'chinese'

    # 全局
    # font = 'Arial'          # 字体
    # font = 'Microsoft YaHei'
    # font = 'Times New Roman + SimSun'
    # font = r'E:/spectrum/字体合并补全工具/Times+SimSun.ttf'
    # 字体加载
    plot_dir_root = os.path.dirname(os.path.abspath(__file__))
    # font_path = os.path.join(plot_dir_root, 'Fonts', 'msyh.ttc')
    font_path = os.path.join(plot_dir_root, 'Fonts', 'msyhbd.ttc')
    # font_path = r'D:\0-Spectrum/字体合并补全工具/Times+SimSun.ttf'
    font_manager.fontManager.addfont(font_path)
    prop = font_manager.FontProperties(fname=font_path)
    # print(prop.get_name())  # 显示当前使用字体的名称

    # 字体设置
    rcParams['font.family'] = 'sans-serif' # 使用字体中的无衬线体
    # rcParams['font.sans-serif'] = prop.get_name()  # 根据名称设置字体
    # rcParams['font.sans-serif'] = 'Helvetica'  # 根据名称设置字体
    rcParams['font.sans-serif'] = 'Microsoft YaHei'  # 根据名称设置字体
    # rcParams['font.weight'] = 'bold' # 设置刻度标签粗细
    # rcParams['font.size'] = 10 # 设置字体大小
    rcParams['axes.unicode_minus'] = False # 使坐标轴刻度标签正常显示正负号

    dpi = 300
    # weight = 'normal'
    weight = 'bold'

    # 图例相关
    anchor = (0.5, 1.23)    # 相对位置
    # legend_word_size = 13   # 图例字体大小
    # legend_word_size = 20   # 图例字体大小 for b_w big
    # legend_word_size = 17   # 图例字体大小 for re-execution and rollback big
    # legend_word_size = 15   # 图例字体大小 for overall/thread/ablation big
    legend_word_size = 11   # 图例字体大小 for re-execution and rollback
    # legend_word_size = 12   # 图例字体大小 for time serial

    # 画布相关
    # figsize = (5.5, 4)     # 画布大小 origin
    # figsize = (5.8, 4.2)     # 画布大小 for bold b_w
    # figsize = (5.8, 3.8)     # 画布大小 for bold b_w bigfont
    # figsize = (5.8, 4.0)     # 画布大小 for bold thread
    # figsize = (5.8, 4)     # 画布大小 for overall/thread big
    # figsize = (6.5, 4.5)   # 画布大小 for 4 columns
    # figsize = (8, 2.5)   # 画布大小 for time series
    # figsize = (5.3, 4)      # 画布大小 for rollback
    figsize = (5.3, 4)      # 画布大小 for bold re-execution and rollback
    # figsize = (5.1, 3.7)      # 画布大小 for bold re-execution and rollback bigfont
    # figsize = (6.98, 4.7)   # 画布大小 for bold ablation/big
    facecolor = 'white'     # 背景颜色

    # 边框相关
    border_width = 1.5      # 边框宽度
    grid = 'y'              # 网格线

    # 刻度相关
    tick_length = 10            # 刻度长度
    tick_width = border_width   # 刻度宽度默认和边框宽度相同
    # tick_word_size = 15         # 刻度字体大小
    tick_word_size = 20         # 刻度字体大小 for other
    # tick_word_size = 17         # 刻度字体大小 for bold overall
    # tick_word_size = 10         # 刻度字体大小 for time serial
    tick_toward = 'out'         # 刻度朝向

    # label标签相关
    label_config_dic = {
        # 'family': font,
        'weight': weight,       # 粗细：normal bold
        'size': 20,             # 大小 for other
        # 'size': 17,             # 大小 for bold overall
        # 'size': 12,             # 大小 for time serial
    }

    # 线相关
    line_width = 2              # 线的宽度

    # 点相关
    # marker_list = ['o', 's', 'v', '^', '<', '>', '1', '2', '3', '4']    # 点的形状
    marker_list = ['X', 'p', '>', '<', '^', 'v', 's', 'o']        # 点的形状 pop
    marker_size = 7                                                      # 点大小

    ##### 变量 #####
    fig: plt.Figure = None
    axes: List[plt.Axes] = None

    max_y_data = 0
    count = 0

    def init(self, ax):
        ax: plt.Axes = ax
        ax.spines['bottom'].set_linewidth(self.border_width)
        ax.spines['left'].set_linewidth(self.border_width)
        ax.spines['top'].set_linewidth(self.border_width)
        ax.spines['right'].set_linewidth(self.border_width)
        ax.tick_params(
            which='major', 
            length=self.tick_length, 
            width=self.tick_width, 
            labelsize=self.tick_word_size,
            direction=self.tick_toward,
        )
        ax.set_axisbelow(True)

    def __init__(
        self,
        nrows: int,     # 行数
        ncols: int,     # 列数
        figsize: tuple=None,
        kwargs: dict=None
    ):
        # plt.rcParams['font.sans-serif'] = [self.font]
        # rcParams['font.sans-serif'] = self.prop.get_name()  # 根据名称设置字体
        # 'dejavusans', 'dejavuserif', 'cm', 'stix', 'stixsans', 'custom'
        rcParams['mathtext.fontset'] = 'stix' # stix' if self.language == 'chinese' else 'dejavusans'
        rcParams['mathtext.default'] = 'regular'
        # rcParams['text.usetex'] = True
        plt.rcParams['font.size'] = self.legend_word_size
        plt.rcParams['font.weight'] = self.weight
        plt.rcParams ['savefig.dpi'] = 300
        
        self.fig, self.axes = plt.subplots(
            nrows=nrows, 
            ncols=ncols,
            figsize=figsize or self.figsize,
            facecolor=self.facecolor,
            **kwargs if kwargs else {}
        )

        if isinstance(self.axes, Iterable):
            for axs in self.axes:
                if isinstance(axs, Iterable):
                    for ax in axs:
                        self.init(ax)
                else: self.init (axs)
        else: self.init(self.axes)

        self.marker_list = ['X', 'p', '>', '<', '^', 'v', 's', 'o']
    
    def plot(
        self,
        ax: plt.Axes,
        xdata: list,
        ydata: list,
        legend_label: str,
        color: str,
        marker: str=None,
        nogrid: bool=False,
        suffix: str='K' # [None, 'K', 'W', 'M']
    ):
        self.count += 1
        ax.plot(
            xdata,
            ydata,
            color=color,
            label=legend_label,
            marker=None if marker == 'None' else marker or self.marker_list.pop(),
            markersize=self.marker_size,
            linewidth=self.line_width
        )
        if not nogrid: ax.grid(axis=self.grid, linewidth=self.border_width)
        if max(ydata) > self.max_y_data: self.max_y_data = max(ydata)

    def bar(
        self,
        ax: plt.Axes,
        xdata: list,
        ydata: list,
        legend_label: str,
        color: str,
        bottom: list=None,
        width: float=0.3,
        hatch: str=None,
        nogrid: bool=False
    ):
        self.count += 1
        ax.bar(
            xdata,
            ydata,
            bottom=bottom,
            color=color,
            label=legend_label,
            hatch=hatch,
            width=width,
            ec='black', ls='-', lw=1
        )
        if not nogrid: ax.grid(axis=self.grid, linewidth=self.border_width)
        if max(ydata) > self.max_y_data: self.max_y_data = max(ydata + bottom if bottom else ydata)

    def bar_new(
        self,
        ax: plt.Axes,
        xdata: list,
        ydata: list,
        legend_label: str,
        color: str,
        bottom: list=None,
        width: float=0.3,
        hatch: str=None,
        nogrid: bool=False
    ):
        self.count += 1
        ax.bar(
            xdata,
            ydata,
            bottom=bottom,
            color=color,
            label=legend_label,
            hatch=hatch,
            width=width,
            ec='black', ls='-', lw=1
        )
        if not nogrid:
            ax.grid(axis=self.grid, linewidth=self.border_width)
        
        # 检查 bottom 是否存在，确保数组操作正确
        if bottom is not None:
            combined = np.array(ydata) + np.array(bottom)
        else:
            combined = np.array(ydata)

        # 使用 np.max 处理 combined 数据
        if np.max(combined) > self.max_y_data:
            self.max_y_data = np.max(combined)

    def set_labels(
        self,
        ax: plt.Axes,
        xlabel: str=None,
        ylabel: str=None,
    ):
        if xlabel: ax.set_xlabel(xlabel, self.label_config_dic)
        if ylabel: ax.set_ylabel(ylabel, self.label_config_dic)

    def format_yticks(
        self, 
        ax: plt.Axes, 
        max_y_data: int=None,
        step: int=None, 
        step_num: int=4,
        suffix: str=None # [None, 'K', 'W', 'M']
    ):
        if not max_y_data: max_y_data = int(self.max_y_data)
        if not step: 
            step = max_y_data // step_num
            step = step // (10 ** ((len(str(step))-1) // 2)) * (10 ** ((len(str(step))-1) // 2))
            print(max_y_data, step)
        if suffix: 
            suffix = suffix.upper()
            suffix_map = {
                None: 1,
                'K': 1000,
                'W': 10000,
                'M': 1000000,
            }
            # if step > 5 * suffix_map[suffix]: step =  step // (5 * suffix_map[suffix]) * (5 * suffix_map[suffix])
            ax.set_yticks(
                range(0, max_y_data, step), 
                [str(x // suffix_map[suffix]) + suffix if x >= suffix_map[suffix] else str(x) for x in range(0, max_y_data, step)]
            )
        else:
            # ax.set_yticks(
            #     range(0, max_y_data, step), 
            #     [str(x) for x in range(0, max_y_data, step)]
            # )
            ax.set_yticks(
                range(0, step*step_num, step), 
                [str(x) for x in range(0, step*step_num, step)]
            )

    def format_yticks_float(
        self, 
        ax: plt.Axes, 
        max_y_data: float=None,
        step: float=None, 
        step_num: int=4,
        suffix: str=None # [None, 'K', 'W', 'M']
    ):
        if suffix: 
            suffix = suffix.upper()
            suffix_map = {
                None: 1,
                'K': 1000,
                'W': 10000,
                'M': 1000000,
            }
            if step > 5 * suffix_map[suffix]: step =  step // (5 * suffix_map[suffix]) * (5 * suffix_map[suffix])
            ax.set_yticks(
                np.arange(0, max_y_data + step, step),
                # range(0, max_y_data, step), 
                # [str(x // suffix_map[suffix]) + suffix if x >= suffix_map[suffix] else str(x) for x in range(0, max_y_data, step)]
            )
        else:
            ax.set_yticks(
                np.arange(0, max_y_data + step, step),
                # [str(x) for x in np.arange(0, max_y_data + step, step)]
                [0, 0.25, 0.5, 0.75, 1]
                # [0, 0.5, 1]
                # [str(x) if x != 0.0 else '0' for x in np.arange(0, max_y_data + step, step)]
                # range(0, max_y_data, step), 
                # [str(x) for x in range(0, max_y_data, step)]
            )

    def legend(
        self, 
        ax: plt.Axes, 
        loc="upper center", 
        ncol=3, 
        anchor=None,
        frameon=False,
        handles=None,
        labels=None,
        columnspacing=None,
        handletextpad=None,
        labelspacing=None,
        handlelength=None,
        kwargs=None
    ):
        if not handles and not labels:
            handles, labels = ax.get_legend_handles_labels()
            handles, labels = order_handles_labels(handles, labels)
        legend = ax.legend(
            handles=handles,
            labels=labels,
            loc=loc, 
            ncol=ncol, 
            bbox_to_anchor=anchor or self.anchor, 
            frameon=frameon,
            columnspacing=columnspacing,
            handletextpad=handletextpad,
            labelspacing=labelspacing,
            handlelength=handlelength,
            prop=kwargs
        )

    def save(self, path, bbox_inches='tight'):
        plt.savefig(path, bbox_inches=bbox_inches)