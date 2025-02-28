"""

@create on: 2021.01.26
"""
from typing import List
from unittest import TestCase
from datetime import datetime, timedelta

import pytest

from scdap.flag import column, ColumnItem
from scdap.frame.function import TimingEvaluation, BaseFunction


class EvaluationTest1(TimingEvaluation):

    def analysis(self) -> List[int]:
        return [0]

    def get_analysis_second(self) -> int:
        return 60

    def get_health_define(self) -> List[str]:
        return ['test']

    def is_realtime_function(self) -> bool:
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = list()
        self.parameter = 0

    def get_column(self):
        return column.total_column

    @staticmethod
    def get_function_name() -> str:
        return 'decisiontest1'

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

    def set_parameter(self, parameter: dict):
        self.parameter = parameter['parameter']

    def reset(self):
        pass


class TestEvaluation(TestCase):
    def test_type(self):
        function = EvaluationTest1(0, [], 0, -1)
        function.initial()
        assert isinstance(function, BaseFunction)
        assert issubclass(EvaluationTest1, BaseFunction)
        assert issubclass(EvaluationTest1, TimingEvaluation)
        assert function.is_health_function()

    def test_initial(self):
        function = EvaluationTest1(0, [], 0, -1)
        function.initial()
        assert function.get_health_size() == len(function.get_health_define())

    def test_initial_with_error_column(self):
        class Test1(EvaluationTest1):
            def get_column(self):
                return ['a', 'b']

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_column
        with pytest.raises(Exception):
            function.initial()

    def test_initial_with_error_get_information(self):
        class Test1(EvaluationTest1):
            def get_information(self) -> dict:
                return {
                    'author': '',
                }

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_information
        with pytest.raises(Exception):
            function.initial()

        class Test1(EvaluationTest1):
            def get_information(self) -> dict:
                return {
                    'version': ''
                }

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_information
        with pytest.raises(Exception):
            function.initial()

    def test_set_cr(self):
        function = EvaluationTest1(0, [], 0, -1)
        # 因为接口不检查类型，所以可以随便设置数值
        # 但是事实上禁止这么做, 只是为了方便测试
        function.set_cr(1, 2)
        assert function.container == 1
        assert function.result == 2
        assert function.__cr__ == [1, 2]

    def test_compute(self):
        class Test1(EvaluationTest1):
            def get_analysis_second(self) -> int: return 10

            def get_health_define(self) -> List[str]:
                return ['test', 'none']

            def get_column(self):
                return [column.meanhf]

            def analysis(self) -> List[int]:
                return [10]

        class CAPI(object):
            def __init__(self):
                self.time = datetime.min

            def get_status(self): return 1

            def get_meanhf(self): return 2

            def get_time(self):
                return self.time

            def get_algorithm_id(self): return 0

        class RAPI(object):
            def __init__(self):
                self.score = list()
                self.time = datetime.min

            def get_algorithm_id(self): return 0

            def get_time(self):
                return self.time

            def get_status(self): return 3

            def set_total_score(self, *args):
                self.score.append(args)

            def get_total_score(self):
                return self.score

        self._test_compute(Test1, CAPI(), RAPI(), 20)
        self._test_compute(Test1, CAPI(), RAPI(), 30)
        self._test_compute(Test1, CAPI(), RAPI(), 35)

        class Test1(EvaluationTest1):
            def get_analysis_second(self) -> int: return 60

            def get_health_define(self) -> List[str]:
                return ['test', 'none']

            def get_column(self):
                return [column.meanhf]

            def analysis(self) -> List[int]:
                return [10, 12, 13]

        self._test_compute(Test1, CAPI(), RAPI(), 59)
        self._test_compute(Test1, CAPI(), RAPI(), 60)
        self._test_compute(Test1, CAPI(), RAPI(), 65)
        self._test_compute(Test1, CAPI(), RAPI(), 121)

    def _test_compute(self, fclass, capi, rapi, size):
        function = fclass(0, [], 0, -1)
        function.initial()
        for i in range(size):
            rapi.time = capi.time = capi.time + timedelta(seconds=1)
            function.set_cr(capi, rapi)
            function.compute()

        assert rapi.score == [tuple(function.analysis())] * int(size / function.get_analysis_second())

    def test_implement_error_pure_method(self):
        class Test1(EvaluationTest1):
            def get_health_define(self) -> List[str]:
                return ['test', 'hello']

            # 数量必须与get_health_define一样
            def get_default_score(self) -> List[int]:
                return [0]

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_information
        with pytest.raises(Exception):
            function.initial()

        class Test1(EvaluationTest1):
            def get_health_define(self) -> List[str]:
                return ['test', 'hello']

            # 数量必须与get_health_define一样
            def get_health_info(self) -> List[dict]:
                return [{}]

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_information
        with pytest.raises(Exception):
            function.initial()

    def test_initial_with_global_parameter(self):
        # 动态配置的算法全局配置
        global_parameter = {
            'analysis_second': 60,
            'health_define': ['hello', 'test'],
            'default_score': [1, 1],
            'health_info': [
                {
                    "threshold": [70, 40],
                    "recommendation": [0, 1, 2],
                    'cn_name': 'cn_name',
                    'en_name': 'en_name'
                } for _ in range(2)
            ],
        }

        class Test1(EvaluationTest1):
            def get_health_define(self) -> List[str]:
                return ['test']

        function = Test1(0, [], 0, -1, global_parameter=global_parameter)
        function.initial()

        health_info = [{**hi, 'function_id': function.get_function_id(), 'health_define': hd}
                       for hi, hd in zip(global_parameter['health_info'], global_parameter['health_define'])]

        assert function.get_health_size() == len(global_parameter['health_define'])
        assert function.get_health_define() == global_parameter['health_define']
        assert function.get_health_info() == health_info
        assert function.get_default_score() == global_parameter['default_score']
        assert function.get_analysis_second() == global_parameter['analysis_second']

    def test_check_analysis_second(self):
        # analysis_second范围必须大于0， 并且为整型变量
        class Test1(EvaluationTest1):
            def get_analysis_second(self) -> int:
                return 0

        function = Test1(0, [], 0, -1)
        with pytest.raises(Exception):
            function.initial()

        class Test1(EvaluationTest1):
            def get_analysis_second(self) -> int:
                return -1

        function = Test1(0, [], 0, -1)
        with pytest.raises(Exception):
            function.initial()

        class Test1(EvaluationTest1):
            def get_analysis_second(self) -> int:
                return '11'

        function = Test1(0, [], 0, -1)
        with pytest.raises(Exception):
            function.initial()

        class Test1(EvaluationTest1):
            def get_analysis_second(self) -> int:
                return 1.1

        function = Test1(0, [], 0, -1)
        with pytest.raises(Exception):
            function.initial()
