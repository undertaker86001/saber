"""

@create on: 2020.07.08
"""
__all__ = ['MDBaseEvaluation']

from abc import ABCMeta
from typing import List

from .evaluation import BaseEvaluation
from .base import MD_TYPE_DR, MD_TYPE_DC


class MDBaseEvaluation(BaseEvaluation, metaclass=ABCMeta):
    """
    评价算法
    单健康度
    多设备数据同步
    """
    container: MD_TYPE_DC
    # 计算结果结果容器
    result: MD_TYPE_DR

    @staticmethod
    def is_mdfunction():
        return True
