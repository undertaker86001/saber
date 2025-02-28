"""

@create on: 2020.01.20

标签
"""
from ._option_key import option_key
from .define_base import DefineBase
from ._event_type import event_type, QualityInspectionItem
from .item_class import HealthDefineItem
# 特征名称标签
# 在设计算法的过程中需要设置算法所需的特征或者是获取历史数据的过程中需要指定获取特定的特征
# 使用的方法为：
# from scdap.flag import column
# class YourFunction1000(BaseFunction):
#     column = {column.meanhf, column.meanlf, ...}
from ._column import column, ColumnItem, format_column, convert_column
