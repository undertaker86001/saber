"""

@create on 2020.02.26

专门针对综合算法
算法必须为需要累积数据的综合算法
算法组中有且只能有一个综合算法
"""
from .base import BaseWorker

from scdap.data import Result


class IntegrationStackWorker(BaseWorker):
    result_api_kwargs = {
        'decision': {
        },
        'evaluation': {
        },
        'other': {
        },
    }

    @staticmethod
    def get_worker_name() -> str:
        return 'integration_stack'

    def is_realtime_worker(self) -> bool:
        return False

    @staticmethod
    def process_type() -> str:
        return 'program'

    def compute(self):
        for device, container, result in self._drive_data(False):
            for function in self._other:
                self._run_function(function, device, container, result)
        self._print_result()

    def _print_result(self, result: Result = None, position: int = None):
        if not self._show_compute_result:
            return

        for result in self._crimp.generator_result():
            for i in range(result.size()):
                super()._print_result(result, i)


worker_class = IntegrationStackWorker
__enabled__ = True

