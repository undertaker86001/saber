"""

@create on: 2020.07.08
"""
__all__ = ['MDBaseIntegration']

from abc import ABCMeta, abstractmethod

from .base import MD_TYPE_DR, MD_TYPE_DC
from .intergration import BaseIntegration


class MDBaseIntegration(BaseIntegration, metaclass=ABCMeta):
    """
    综合算法
    多设备数据同步
    """
    container: MD_TYPE_DC
    # 计算结果结果容器
    result: MD_TYPE_DR

    @abstractmethod
    def is_realtime_function(self) -> bool:
        pass

    @staticmethod
    def is_mdfunction():
        return True
