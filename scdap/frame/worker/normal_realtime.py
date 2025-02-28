"""

@create on 2020.02.25
"""
from .base import BaseWorker


class NormalRealTimeWorker(BaseWorker):
    """
    实时算法工作组
    """
    result_api_kwargs = {
        'decision': {
            'allow_score': False,
            'allow_add_result': False,
        },
        'evaluation': {
            'allow_add_result': False,
        },
        'other': {
        },
    }

    @staticmethod
    def get_worker_name() -> str:
        return 'normal_realtime'

    def is_realtime_worker(self) -> bool:
        return True

    @staticmethod
    def process_type() -> str:
        return 'program'

    def compute(self):
        for device, container, result in self._drive_data(True):
            for fun in self._decision:
                self._run_function(fun, device, container, result)

            for fun in self._evaluation:
                self._run_function(fun, device, container, result)

            self._print_result(result)


worker_class = NormalRealTimeWorker
__enabled__ = True

