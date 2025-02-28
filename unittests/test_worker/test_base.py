"""

@create on: 2021.01.10
"""
from datetime import datetime
from typing import Type, List

import pytest

from scdap.wp import Context
from scdap.frame.function import BaseDecision, BaseEvaluation
from scdap.flag import column, convert_column

from unittests.flist_utils import random_flist


class DecisionTest1(BaseDecision):
    def get_column(self): return []

    def is_realtime_function(self): return True

    @staticmethod
    def get_function_name(): return 'decisiontest1'

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

    def set_parameter(self, parameter: dict): pass

    def reset(self): pass


class EvaluationTest2(BaseEvaluation):

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_name(): return 'test2'

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

    def get_health_define(self) -> List[str]:
        return ['hello', 'hello2']

    def compute(self):
        pass

    def set_parameter(self, parameter: dict):
        pass

    def reset(self):
        pass


class TestBaseWorker(object):
    @classmethod
    def setup_class(cls):
        from scdap import config
        config.ID_REFLICT_BY_API = False

    def test_initial_worker_with_realtime_function(self):
        class Test1(DecisionTest1):
            def is_realtime_function(self): return True

        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        context.initial_worker('normal_realtime', [{'function': Test1}], [], [])

        with pytest.raises(Exception):
            context.initial_worker('normal_stack', [{'function': Test1}], [], [])

        class Test1(DecisionTest1):
            def is_realtime_function(self): return False

        with pytest.raises(Exception):
            context.initial_worker('normal_realtime', [{'function': Test1}], [], [])

    def test_initial_worker_with_function_count(self):
        # 至少需要配置一个算法
        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        # 必须至少配置一个算法
        with pytest.raises(Exception):
            context.initial_worker('normal_realtime', [], [], [])

    def test_initial_worker_with_column(self):
        # 检查算法类中的get_column配置
        self._test_initial_worker_with_column([column.feature1])
        self._test_initial_worker_with_column([column.feature2])
        self._test_initial_worker_with_column([column.feature3])
        self._test_initial_worker_with_column([column.feature4])
        self._test_initial_worker_with_column(column.hr_column)

        self._test_initial_worker_with_column([column.meanhf])
        self._test_initial_worker_with_column([column.meanlf])
        self._test_initial_worker_with_column([column.mean])
        self._test_initial_worker_with_column([column.std])
        self._test_initial_worker_with_column(column.lr_column)

        self._test_initial_worker_with_column([column.peakfreqs])
        self._test_initial_worker_with_column([column.peakpowers])
        self._test_initial_worker_with_column([column.bandspectrum])

        self._test_initial_worker_with_column([column.meanhf, column.feature1])
        self._test_initial_worker_with_column(column.total_column)

        # 必须通过scdap.flag.column.xxx对需要的特征进行配置
        with pytest.raises(Exception):
            self._test_initial_worker_with_column(
                ['hello'])

    def _test_initial_worker_with_column(self, col: list):
        class Test1(DecisionTest1):
            def get_column(self):
                return col

        # 测试column配置
        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        # 必须至少配置一个算法
        context.initial_crimp()
        context.initial_worker('normal_realtime', [{'function': Test1}])
        context.bind_worker_and_crimp()
        base_c = [column.status, column.time]

        col = convert_column(column, col)
        if column.has_hrtime(col):
            base_c.append(column.hrtime)

        c = [*col, *base_c]
        # 比对column是否配置正确
        assert set(context.worker.get_column()[dev]) == set(c)

    def test_function_get_api(self):
        # 默认接口有get_time/get_status
        class Test1(DecisionTest1):
            def get_column(self): return []

            def compute(self):
                self.container.get_time(), self.container.get_status()

        self._test_function_get_api(Test1, False)

        # 配置低分特征将默认提供get_lrdata以及配置的特征的接口
        class Test1(DecisionTest1):
            def get_column(self): return [column.meanhf, column.meanlf, column.mean, column.std]

            def compute(self):
                self.container.get_meanhf(), self.container.get_lrdata()
                self.container.get_time(), self.container.get_status()

        self._test_function_get_api(Test1, False)

        # 调用没有配置的特征的接口
        class Test1(DecisionTest1):
            def get_column(self): return [column.meanhf]

            def compute(self): self.container.get_meanlf()

        self._test_function_get_api(Test1, True)

        # 没有配置任何高分特征但是调用高分数据
        class Test1(DecisionTest1):
            def get_column(self): return [column.meanhf]

            def compute(self): self.container.get_hrdata()

        self._test_function_get_api(Test1, True)

        # 没有配置任何高分特征但是调用高分时间
        class Test1(DecisionTest1):
            def get_column(self): return [column.meanhf]

            def compute(self): self.container.get_hrtime()

        self._test_function_get_api(Test1, True)

        # 没有配置任何高分特征但是调用高分时间
        class Test1(DecisionTest1):
            def get_column(self): return [column.feature1]

            def compute(self): self.container.get_feature1()

        self._test_function_get_api(Test1, False)

        # 配置高分特征调用高分时间
        class Test1(DecisionTest1):
            def get_column(self): return [column.feature1]

            def compute(self): self.container.get_hrtime()

        self._test_function_get_api(Test1, False)

        # 配置高分特征调用获取所有高分特征数据
        class Test1(DecisionTest1):
            def get_column(self): return [column.feature1, column.feature2, column.feature3, column.feature4]

            def compute(self): self.container.get_hrdata()

        self._test_function_get_api(Test1, False)

        # 配置高分特征调用低分特征
        class Test1(DecisionTest1):
            def get_column(self): return [column.feature1]

            def compute(self): self.container.get_meanhf()

        self._test_function_get_api(Test1, True)

        # 配置高分特征调用低分特征
        class Test1(DecisionTest1):
            def get_column(self): return [column.feature1]

            def compute(self): self.container.get_lrdata()

        self._test_function_get_api(Test1, True)

    def _test_function_get_api(self, function_class, raise_exc: bool):
        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        # 必须至少配置一个算法
        context.initial_crimp()
        context.initial_worker('normal_realtime', [{'function': function_class}])
        context.bind_worker_and_crimp()
        # 生成一段数据测试
        flists = dict()
        for dev in context.devices:
            columns = context.worker.get_column()[dev]
            columns = list(map(str, columns))
            flist = random_flist(dev, columns, 1)
            flist._algorithm_id = dev
            context.crimp.get_container(dev).flist.extend_itemlist(flist)
            flists[dev] = flist

        if raise_exc:
            with pytest.raises(NotImplementedError):
                context.worker.compute()
        else:
            context.worker.compute()

    def test_function_send_api_normal_worker_decision(self):
        """
        normal_stask/normal_realtime都需要测试
        两个针对识别算法差异较大
        """
        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return True

            def compute(self):
                self.result.add_result(0, datetime.now())

        # 实时算法工作组不允许使用add_result
        self._test_function_send_api('normal_realtime', Test1, None, True)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.add_result(0, datetime.now())
        # 堵塞算法工作组允许使用add_result
        self._test_function_send_api('normal_stack', Test1, None, False)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.add_result(0, datetime.now(), 0)

        # 堵塞算法工作组允许使用add_result但是识别算法不允许通过add_result配置健康度数值
        self._test_function_send_api('normal_stack', Test1, None, True, Exception)

        class Test1(DecisionTest1):
            def compute(self):
                self.result.set_score(0)

        # 实时算法工作组不允许使用配置健康度相关的接口
        self._test_function_send_api('normal_realtime', Test1, None, True)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.set_score(0)

        self._test_function_send_api('normal_stack', Test1, None, True)

        class Test1(DecisionTest1):
            def compute(self):
                self.result.get_score(0)

        # 实时算法工作组不允许使用配置健康度相关的接口
        self._test_function_send_api('normal_realtime', Test1, None, True)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.get_score(0)

        self._test_function_send_api('normal_stack', Test1, None, True)

        class Test1(DecisionTest1):
            def compute(self):
                self.result.set_total_score()

        # 实时算法工作组不允许使用配置健康度相关的接口
        self._test_function_send_api('normal_realtime', Test1, None, True)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.set_total_score()

        self._test_function_send_api('normal_stack', Test1, None, True)

        class Test1(DecisionTest1):
            def compute(self):
                self.result.get_total_score()

        # 实时算法工作组不允许使用配置健康度相关的接口
        self._test_function_send_api('normal_realtime', Test1, None, True)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.get_total_score()

        self._test_function_send_api('normal_stack', Test1, None, True)

        class Test1(DecisionTest1):
            def compute(self):
                self.result.get_prev_score(0)

        # 实时算法工作组不允许使用配置健康度相关的接口
        self._test_function_send_api('normal_realtime', Test1, None, True)

        class Test1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.get_prev_score(0)

        self._test_function_send_api('normal_stack', Test1, None, True)

    def test_function_send_api_normal_worker_evaluation(self):
        class RealtimeTest1(DecisionTest1):
            def compute(self):
                self.result.set_status(0)

        class StackTest1(DecisionTest1):
            def is_realtime_function(self):
                return False

            def compute(self):
                self.result.add_result(0, datetime.now())

        class Test2(EvaluationTest2):
            def compute(self):
                self.result.add_result(0, datetime.now())

        # 实时算法工作组不允许使用add_result
        # self._test_function_send_api('normal_realtime', RealtimeTest1, Test2, True)
        # 堵塞算法工作组的评价算法不允许使用add_result
        self._test_function_send_api('normal_stack', StackTest1, Test2, True)

        class Test2(EvaluationTest2):
            def compute(self):
                self.result.set_status(1)
                assert self.result.get_status() == 1

                now = datetime.now()
                self.result.set_time(now)
                assert self.result.get_time() == now

                self.result.set_score(90)
                assert self.result.get_score() == 90

                self.result.set_score(0, 80)
                assert self.result.get_score(0) == 80

                self.result.set_score(1, 70)
                assert self.result.get_score(1) == 70

                self.result.set_total_score(85, 75)
                assert self.result.get_total_score() == [85, 75]

        # 实时算法工作组不允许使用add_result
        self._test_function_send_api('normal_realtime', RealtimeTest1, Test2, False)
        # 堵塞算法工作组不允许使用add_result
        self._test_function_send_api('normal_stack', StackTest1, Test2, False)

    def _test_function_send_api(self, worker_class, decision, evaluation,
                                raise_exc: bool, exception: Type[Exception] = NotImplementedError):
        dev = "100"
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        # 必须至少配置一个算法
        if decision:
            decision = [{"function": decision}]
        if evaluation:
            evaluation = [{"function": evaluation}]
        context.initial_crimp()
        context.initial_worker(worker_class, decision, evaluation)
        context.bind_worker_and_crimp()
        # 生成一段数据测试
        flists = dict()
        for dev in context.devices:
            columns = context.worker.get_column()[dev]
            columns = list(map(str, columns))
            flist = random_flist(dev, columns, 1)
            flist._algorithm_id = dev
            context.crimp.get_container(dev).flist.extend_itemlist(flist)
            flists[dev] = flist

        if raise_exc:
            with pytest.raises(exception):
                context.worker.compute()
        else:
            context.worker.compute()
