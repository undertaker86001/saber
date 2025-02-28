"""

@create on: 2021.05.10
"""
from typing import List, Tuple
from abc import ABCMeta

from .base import BaseFunction


class BaseSummary(BaseFunction, metaclass=ABCMeta):
    """
    综合算法
    允许多设备
    """
    def is_realtime_function(self) -> bool:
        return False

    @staticmethod
    def get_function_type():
        return 'summary'

    def get_health_define(self) -> List[str]:
        return []

    def get_column(self) -> List[str]:
        return []
