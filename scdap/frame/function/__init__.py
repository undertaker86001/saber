"""

@create on: 2020.01.25
"""
__all__ = [
    'BaseFunction',
    # 识别算法基类，识别算法必须继承该类
    'BaseDecision',
    # 评价算法基类，单健康评价算法必须继承该类
    'BaseEvaluation',
    # 以固定时间固定间隔计算健康度的单健康度评价算法
    # 基类中将拥有两个时间变量，其中一个为计算间隔，另一个为下一次计算健康度的时间next_analysis_time
    # 每次实时数据中的时间大于或等于next_analysis_time则基类将调用analysis()方法计算健康度结果并设置到结果容器中
    # 故必须实现 analysis()方法用于计算健康度数值
    # 例如:
    # analysis_second: 600
    # 则代表每10分钟计算一次健康度
    # 计算的时间为 0m/10m/20m/30m/40m/50m/
    'TimingEvaluation',
    'BaseIntegration',

    # 'MDBaseDecision',
    # 'MDBaseEvaluation',
    # 综合算法基类，所有单健康度综合算法必须继承该类
    # 'MDBaseIntegration',

    # 汇总算法
    'BaseSummary',

    'get_health_defines',
    'fset',
    'parser_id'
]

from typing import List

from scdap.flag import option_key
from scdap.util.parser import parser_id
from .base import BaseFunction
# 普通算法基类
from .decision import BaseDecision
from .evaluation import BaseEvaluation
from .timing_evaluation import TimingEvaluation
from .intergration import BaseIntegration

# 多设备数据联动算法基类
# 传入的container与result将包含多个设备的数据容器
from .md_decision import MDBaseDecision
from .md_evaluation import MDBaseEvaluation
from .md_intergration import MDBaseIntegration

from .summary import BaseSummary

from .fset import fset


def get_health_defines(option: dict) -> List[int]:
    """
    根据配置文件直接获取function_name
    function_name主要用于设置健康度名称
    """
    # 放在外面会造成递归调用报错
    func = list()
    for f in option.get(option_key.decision, list()) + \
            option.get(option_key.evaluation, list()) + \
            option.get(option_key.other, list()):
        f = f[option_key.function]

        if isinstance(f, str):
            f = fset.get_function_class(f)

        if not f.is_health_function():
            continue

        health_define = f.get_health_define()
        if isinstance(health_define, list):
            func.extend(health_define)
        else:
            func.append(health_define)

    return func
