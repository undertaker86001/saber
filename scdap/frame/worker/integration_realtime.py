"""

@create on 2020.02.25

专门针对综合算法
算法必须为实时的综合算法
算法组中有且只能有一个综合算法
"""

from .base import BaseWorker


class IntegrationRealTimeWorker(BaseWorker):
    result_api_kwargs = {
        'decision': {
        },
        'evaluation': {
        },
        'other': {
            'allow_score': True,
            'allow_add_result': False,
        },
    }

    @staticmethod
    def get_worker_name() -> str:
        return 'integration_realtime'

    def is_realtime_worker(self) -> bool:
        return True

    @staticmethod
    def process_type() -> str:
        return 'program'

    def compute(self):
        for device, container, result in self._drive_data(True):
            for function in self._other:
                self._run_function(function, device, container, result)

            self._print_result(result)


worker_class = IntegrationRealTimeWorker
__enabled__ = True
