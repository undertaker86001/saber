"""

@create on: 2020.10.09
"""
from .md_base import MDBaseWorker


class MDNormalRealTimeWorker(MDBaseWorker):
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
        return 'md_normal_realtime'

    def is_realtime_worker(self) -> bool:
        return True

    def compute(self):
        for devices, containers, results in self._drive_data(True):

            for fun in self._decision:
                self._run_function(fun, devices, containers, results)

            for fun in self._evaluation:
                self._run_function(fun, devices, containers, results)

        self._print_result()


worker_class = MDNormalRealTimeWorker
__enabled__ = True

