"""

@create on 2020.02.25

"""
from abc import ABCMeta, abstractmethod
from .evaluation import BaseEvaluation


class BaseIntegration(BaseEvaluation, metaclass=ABCMeta):
    """
    综合算法
    允许多设备
    """
    
    @abstractmethod
    def is_realtime_function(self) -> bool:
        pass

    @staticmethod
    def get_function_type():
        return 'other'
