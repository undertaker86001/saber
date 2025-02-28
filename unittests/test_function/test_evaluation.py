"""

@create on: 2021.01.26
"""
from typing import List
from unittest import TestCase

import pytest

from scdap.flag import column
from scdap.frame.function import BaseEvaluation, BaseFunction


class EvaluationTest1(BaseEvaluation):
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

    def compute(self):
        pass

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
        assert issubclass(EvaluationTest1, BaseEvaluation)
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
        class CAPI(object):
            def get_status(self): return 1

        class RAPI(object):
            def __init__(self):
                self.status = 0

            def set_status(self, status: int):
                self.status = status

        class Test1(EvaluationTest1):
            def compute(self):
                self.result.set_status(self.container.get_status())

        capi = CAPI()
        rapi = RAPI()
        function = Test1(0, [], 0, -1)
        # 因为接口不检查类型，所以可以随便设置数值
        # 但是事实上禁止这么做, 只是为了方便测试
        function.set_cr(capi, rapi)
        function.compute()
        assert rapi.status == capi.get_status()

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
            'health_define': ['hello', 'test'],
            'default_score': [1, 1],
            'health_info': [
                {
                    "threshold": [70, 40],
                    "recommendation": [0, 1, 2],
                    'en_name': 'en_name',
                    'cn_name': 'cn_name'
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
