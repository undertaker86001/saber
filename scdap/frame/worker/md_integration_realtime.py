"""

@create on 2020.02.25

专门针对综合算法
算法必须为实时的综合算法
算法组中有且只能有一个综合算法
"""
from .md_base import MDBaseWorker


class MDIntegrationRealTimeWorker(MDBaseWorker):
    result_api_kwargs = {
        'decision': {
        },
        'evaluation': {
        },
        'other': {
            "allow_add_result": False
        },
    }

    @staticmethod
    def get_worker_name() -> str:
        return 'md_integration_realtime'

    def is_realtime_worker(self) -> bool:
        return True

    def compute(self):
        for devices, containers, results in self._drive_data(True):
            for function in self._other:
                self._run_function(function, devices, containers, results)

            self._print_result(results)


worker_class = MDIntegrationRealTimeWorker
__enabled__ = False
