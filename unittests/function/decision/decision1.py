"""

@create on: 2020.12.16
"""
from typing import List
from datetime import datetime, timedelta

import numpy as np

from scdap.flag import column
from scdap.frame.function import BaseDecision


class Decision1(BaseDecision):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num = 20

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name() -> str:
        return 'decision1'

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

    def compute(self):
        result = 0
        for col in self.get_column():
            r = getattr(self.container, f'get_{col}')()
            if isinstance(r, np.ndarray):
                result += np.sum(r)
            else:
                result += r
        status = int(result) % self.num
        self.result.set_status(status)
        self.result.add_alarm(start=datetime.now() - timedelta(hours=1), stop=datetime.now(), message='event')
        # self.result.set_part_info(
        #     start=datetime.now() - timedelta(hours=1), stop=datetime.now(), message='part', ptype='test'
        # )
        self.result.set_part('part')

    def set_parameter(self, parameter: dict):
        self.num = parameter.get('num', 20)

    def reset(self):
        pass


function = Decision1
