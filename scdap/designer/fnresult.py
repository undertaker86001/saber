"""

@create on: 2021.01.08
"""
from scdap.data import ResultList, FeatureList


class FinalResult(object):
    def __init__(self, algorithm_id: str, info: list, container: FeatureList, result: ResultList):
        self.algorithm_id = algorithm_id
        self.info = info
        self.container = container
        self.result = result
