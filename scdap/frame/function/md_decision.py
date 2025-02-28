"""

@create on: 2020.07.08
"""
__all__ = ['MDBaseDecision']

from abc import ABCMeta

from .decision import BaseDecision
from .base import MD_TYPE_DR, MD_TYPE_DC


class MDBaseDecision(BaseDecision, metaclass=ABCMeta):
    """
    识别算法
    多设备数据同步
    """
    container: MD_TYPE_DC
    # 计算结果结果容器
    result: MD_TYPE_DR

    @staticmethod
    def is_mdfunction():
        return True
