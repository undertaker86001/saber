"""

@create on: 2021.01.13
"""
from typing import List
from unittest import TestCase

import pytest

from scdap.flag import column
from scdap.frame.function import BaseDecision, BaseFunction


class DecisionTest1(BaseDecision):
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


class TestDecision(TestCase):
    def test_type(self):
        function = DecisionTest1(0, [], 0, -1)
        function.initial()
        assert isinstance(function, BaseFunction)
        assert issubclass(DecisionTest1, BaseFunction)
        assert issubclass(DecisionTest1, BaseDecision)
        assert not function.is_health_function()

    def test_initial(self):
        function = DecisionTest1(0, [], 0, -1)
        function.initial()

    def test_initial_with_error_column(self):
        class Test1(DecisionTest1):
            def get_column(self):
                return ['a', 'b']

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_column
        with pytest.raises(Exception):
            function.initial()

    def test_initial_with_error_get_information(self):
        class Test1(DecisionTest1):
            def get_information(self) -> dict:
                return {
                    'author': '',
                }

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_information
        with pytest.raises(Exception):
            function.initial()

        class Test1(DecisionTest1):
            def get_information(self) -> dict:
                return {
                    'version': ''
                }

        function = Test1(0, [], 0, -1)
        # 配置了错误的get_information
        with pytest.raises(Exception):
            function.initial()

    def test_set_cr(self):
        function = DecisionTest1(0, [], 0, -1)
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

        class Test1(DecisionTest1):
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
