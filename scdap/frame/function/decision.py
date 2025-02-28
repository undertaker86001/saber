"""

@create on 2020.02.25

"""
from typing import List
from abc import ABCMeta
from .base import BaseFunction


class BaseDecision(BaseFunction, metaclass=ABCMeta):
    """
    识别算法
    """

    @staticmethod
    def get_function_type():
        return 'decision'

    def get_health_define(self) -> List[str]:
        return []
