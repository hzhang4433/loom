color_palette = [
    '#4c7da8', #蓝
    '#9c504e', #红
    '#e8ac64', #橙
    '#e5c875', #黄
    '#3d6664', #深绿
    '#614636', #棕
    '#a780b7', #紫
]
# schemas = [
#     # 里面是 (协议名称, 颜色(RGB格式)的元组)
#     ('Loom'         ,       '#C80000'),
#     ('Aria'         ,       '#C85C00'),
#     ('Harmony'      ,       '#C8C800'),
#     ('HarmonyIB'    ,       '#00C85C'),
#     ('Fractal'         ,    '#003CC8'),
#     ('Serial'       ,       '#5C00C8')
# ]
schemas = [
    # 里面是 (协议名称, 颜色(RGB格式)的元组)
    ('Loom'         ,       '#d62728'),  # 红色
    ('Fractal'      ,       '#9467bd'),  # 紫色
    ('Harmony'      ,       '#2ca02c'),  # 绿色
    ('HarmonyIB'    ,       '#8c564b'),  # 棕色
    ('Aria'         ,       '#ff7f0e'),  # 橙色
    ('Serial'       ,       '#1f77b4')   # 蓝色
]

schemas_for_ablation = [
    # 里面是 (协议名称, 颜色(RGB格式)的元组)
    ('LoomRaw_20'     ,       '#9467bd'),  # 紫色
    ('LoomRaw_1'      ,       '#1f77b4'),  # 蓝色
    ('LoomFR_20'      ,       '#8c564b'),  # 棕色
    ('LoomFR_1'       ,       '#2ca02c'),  # 绿色
    ('Loom_20'        ,       '#d62728'),  # 红色
    ('Loom_1'         ,       '#ff7f0e'),  # 橙色
]

schemas_for_ablation_bar = [
    # 里面是 (协议名称, 颜色(RGB格式)的元组)
    ('LoomRaw'     ,        '#9b8bb9'),  # 紫色，饱和度降低
    ('LoomFR'      ,        '#db9e67'),  # 橙色，饱和度降低
    ('Loom'        ,        '#c85a59'),  # 红色，饱和度降低
]

schemas_for_bar = [
    # 里面是 (协议名称, 颜色(RGB格式)的元组)
    ('Loom'         ,       '#c85a59'),  # 红色，饱和度降低
    ('Fractal'      ,       '#7a93c1'),  # 蓝色，饱和度降低
    ('Harmony'      ,       '#7ab77a'),  # 绿色，饱和度降低
    ('HarmonyIB'    ,       '#db9e67'),  # 橙色，饱和度降低
    ('Aria'         ,       '#8d7876'),  # 棕色，饱和度降低
    ('Serial'       ,       '#9b8bb9')   # 紫色，饱和度降低
]

# schemas_for_pre = [
#     # 里面是 (协议名称, 颜色(RGB格式)的元组)
#     ('SpectrumPreSched' ,       '#9400D3'),  # 深紫色
#     ('Loom'         ,       '#d62728'),  # 红色
#     ('SpectrumNoPartial',       '#595959'),  # 灰色
#     ('Aria'          ,       '#8c564b'),  # 棕色
#     ('Harmony'             ,       '#2ca02c'),  # 绿色
#     ('HarmonyIB'           ,       '#ff7f0e'),  # 橙色
#     ('Fractal'           ,       '#1f77b4'),  # 蓝色
# ]