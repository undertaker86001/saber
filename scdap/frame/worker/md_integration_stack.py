"""

@create on: 2020.07.08
"""
from .md_base import MDBaseWorker


class MDIntegrationStackWorker(MDBaseWorker):
    result_api_kwargs = {
        'decision': {
        },
        'evaluation': {
        },
        'other': {
        },
    }

    modify_other_cr = [True, True]

    @staticmethod
    def get_worker_name() -> str:
        return 'md_integration_stack'

    def is_realtime_worker(self) -> bool:
        return False

    def compute(self):
        for devices, containers, results in self._drive_data(False):
            for function in self._other:
                self._run_function(function, devices, containers, results)

        self._print_result()


worker_class = MDIntegrationStackWorker
__enabled__ = False

