"""

@create on: 2021.01.13
"""
from typing import List

import numpy as np

from scdap.flag import column
from scdap.frame.function import BaseDecision, BaseEvaluation, BaseIntegration


def sum_f(lr_data, hr_data, bandspectrum, peakfreqs, peakpowers):
    return int(np.sum(np.hstack(list(hr_data) + [bandspectrum, peakfreqs, peakpowers, lr_data])))


def realtimedecision1_compute(lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache: list):
    """
    计算方法, 外界可以调用
    """
    status = sum_f(lr_data, hr_data, bandspectrum, peakfreqs, peakpowers)
    cache.append(status)
    return int(np.sum(cache))


class RealtimeDecision1(BaseDecision):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = list()
        self.parameter = 0

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name() -> str:
        return 'realtimedecision1'

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
        status = realtimedecision1_compute(
            self.container.get_lrdata(), self.container.get_hrdata(), self.container.get_time(),
            self.container.get_bandspectrum(), self.container.get_peakfreqs(), self.container.get_peakpowers(),
            self.cache
        )
        self.result.set_status(status)

    def set_parameter(self, parameter: dict):
        self.parameter = parameter['parameter']

    def reset(self):
        pass


def evaluation2_compute(status, lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache: list):
    """
    计算方法, 外界可以调用
    """
    score = status + sum_f(lr_data, hr_data, bandspectrum, peakfreqs, peakpowers)
    cache.append(score)
    return int(np.sum(cache)) % 99 + 1


class Evaluation2(BaseEvaluation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = list()
        self.parameter = 0

    def get_health_define(self) -> List[str]:
        return ['a']

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name() -> str:
        return 'evaluation2'

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
        score = evaluation2_compute(
            self.result.get_status(), self.container.get_lrdata(), self.container.get_hrdata(),
            self.container.get_time(), self.container.get_bandspectrum(),
            self.container.get_peakfreqs(), self.container.get_peakpowers(),
            self.cache
        )
        self.result.set_score(0, score)

    def set_parameter(self, parameter: dict):
        self.parameter = parameter['parameter']

    def reset(self):
        pass


def evaluation3_compute(status, lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache: list):
    """
    计算方法, 外界可以调用
    """
    s = sum_f(lr_data, hr_data, bandspectrum, peakfreqs, peakpowers)
    score1 = s % 99 + 1
    cache.append(score1)
    score2 = s * 2 % 99 + 1
    cache.append(score2)
    return score1, score2


class Evaluation3(BaseEvaluation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = list()
        self.parameter = 0

    def get_health_define(self) -> List[str]:
        return ['c', 'b']

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name() -> str:
        return 'evaluation3'

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
        s1, s2 = evaluation3_compute(
            self.result.get_status(), self.container.get_lrdata(), self.container.get_hrdata(),
            self.container.get_time(), self.container.get_bandspectrum(),
            self.container.get_peakfreqs(), self.container.get_peakpowers(),
            self.cache
        )
        self.result.set_total_score(s1, s2)

    def set_parameter(self, parameter: dict):
        self.parameter = parameter['parameter']

    def reset(self):
        pass


def stackdecision4_compute(lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache):
    """
    计算方法, 外界可以调用
    """
    s = int(np.sum(lr_data) + np.sum(hr_data) + np.sum(bandspectrum) + np.sum(peakfreqs) + np.sum(peakpowers))
    status = s % 10

    cache['status'].append(status)
    cache['time'].append(time)
    cache['count'] += 1
    if cache['count'] >= cache['parameter']:
        p = s % 2
        size = len(cache['status'])
        size = size if p else int(size / 2) + 1
        status = cache['status'][:size]
        time = cache['time'][:size]
        del cache['status'][:size]
        del cache['time'][:size]
        cache['count'] = 0
        return status, time
    return [], []


class StackDecision4(BaseDecision):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = dict()
        self.parameter = 10

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return False

    @staticmethod
    def get_function_name() -> str:
        return 'stackdecision4'

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
        status, time = stackdecision4_compute(
            self.container.get_lrdata(), self.container.get_hrdata(), self.container.get_time(),
            self.container.get_bandspectrum(), self.container.get_peakfreqs(), self.container.get_peakpowers(),
            self.cache[self.container.get_algorithm_id()]
        )
        for s, t in zip(status, time):
            self.result.add_result(s, t)

    def set_parameter(self, parameter: dict):
        for dev in self.devices:
            self.cache[dev] = {
                'status': list(),
                'time': list(),
                'count': 0,
                'parameter': parameter['parameter']
            }
        self.parameter = parameter['parameter']

    def reset(self):
        pass


def realtimeintegration5_compute(lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache):
    status = realtimedecision1_compute(lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache)
    s1, s2 = evaluation3_compute(status, lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache)
    return status, s1, s2


class RealtimeIntegration5(BaseIntegration):

    def get_health_define(self) -> List[str]:
        return ['test1', 'test2']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = list()
        self.parameter = 0

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name() -> str:
        return 'realtimeintegration5'

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
        status, s1, s2 = realtimeintegration5_compute(
            self.container.get_lrdata(), self.container.get_hrdata(), self.container.get_time(),
            self.container.get_bandspectrum(), self.container.get_peakfreqs(), self.container.get_peakpowers(),
            self.cache
        )
        self.result.set_status(status)
        self.result.set_total_score(s1, s2)

    def set_parameter(self, parameter: dict):
        self.parameter = parameter['parameter']

    def reset(self):
        pass


def stackintegration6_compute(lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache):
    status, time = stackdecision4_compute(lr_data, hr_data, time, bandspectrum, peakfreqs, peakpowers, cache)
    score = list()
    for s, t in zip(status, time):
        s1, s2 = evaluation3_compute(s, lr_data, hr_data, t, bandspectrum, peakfreqs, peakpowers, cache['score_cache'])
        score.append([s1, s2])
    return status, time, score


class StackIntegration6(BaseIntegration):
    def get_health_define(self) -> List[str]:
        return ['test1', 'test2']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = dict()
        self.parameter = 10

    def get_column(self) -> List[str]:
        return column.total_column

    def is_realtime_function(self) -> bool:
        return False

    @staticmethod
    def get_function_name() -> str:
        return 'stackintegration6'

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
        status, time, score = stackintegration6_compute(
            self.container.get_lrdata(), self.container.get_hrdata(), self.container.get_time(),
            self.container.get_bandspectrum(), self.container.get_peakfreqs(), self.container.get_peakpowers(),
            self.cache[self.container.get_algorithm_id()]
        )
        for s1, t, s2 in zip(status, time, score):
            self.result.add_result(s1, t, *s2)

    def set_parameter(self, parameter: dict):
        for dev in self.devices:
            self.cache[dev] = {
                'status': list(),
                'time': list(),
                'count': 0,
                'parameter': parameter['parameter'],
                'score_cache': list()
            }
        self.parameter = parameter['parameter']

    def reset(self):
        pass
