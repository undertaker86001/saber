"""

@create on: 2021.01.05
"""
from typing import List
import numpy as np

from scdap.flag import column
from scdap.frame.function import BaseIntegration


class Other4(BaseIntegration):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num = 20

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name() -> str:
        return 'other4'

    @staticmethod
    def get_information() -> dict:
        """
        :return: 获取算法的简介与信息
        """
        return {
            'author': '',
            'description': '',
            'email': '',
            'version': ''
        }

    def get_health_define(self) -> List[str]:
        return ['health']

    def compute(self):
        result = 0
        for col in self.get_column():
            r = getattr(self.container, f'get_{col}')()
            if isinstance(r, np.ndarray):
                result += np.sum(r)
            else:
                result += r
        status = int(result) % self.num
        self.result.set_score(status)

    def set_parameter(self, parameter: dict):
        self.num = parameter.get('num', 20)

    def reset(self):
        pass


function = Other4
