"""

@create on: 2021.01.13
"""
import pytest
from scdap.wp import Context

from unittests.flist_utils import random_flist

from .function import Evaluation2, evaluation2_compute
from .function import Evaluation3, evaluation3_compute
from .function import StackDecision4, stackdecision4_compute


class TestStackRealtime(object):
    @classmethod
    def setup_class(cls):
        from scdap import config
        config.ID_REFLICT_BY_API = False

    def test_initial_worker(self):
        class Test5(StackDecision4):

            @staticmethod
            def get_function_name() -> str:
                return 'test5'

        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        # 只能配置一个识别算法
        with pytest.raises(Exception):
            context.initial_worker(
                'normal_stack',
                [{'function': StackDecision4}, {'function': Test5}],
            )

    def test_run_simple_dev_worker_nostack(self):
        self._test_run_worker(['100'], 100, 1)

    def test_run_multi_dev_worker_nostack(self):
        self._test_run_worker(['100', '101', '102'], 100, 1)

    def test_run_simple_dev_worker_stack(self):
        self._test_run_worker(['100'], 100, 10)

    def test_run_multi_dev_worker_stack(self):
        self._test_run_worker(['100', '101', '102'], 100, 10)

    def _test_run_worker(self, dev, size, param):
        context = Context(dev[0], 'program', dev, [dev[0]], 0, 'mediator', False)
        context.initial_crimp()
        context.initial_worker(
            'normal_stack',
            [{'function': StackDecision4}],
            [{'function': Evaluation2}, {'function': Evaluation3}],
        )
        context.bind_worker_and_crimp()
        worker = context.worker
        # 配置参数
        worker.set_parameter(parameter={4: {'parameter': param}, 2: {'parameter': 3}, 3: {'parameter': 4}})
        decision = worker._fid_to_func[4]
        evaluation1 = worker._fid_to_func[2]
        evaluation2 = worker._fid_to_func[3]
        # 检查参数是否正确配置
        assert decision.parameter == param
        assert evaluation1.parameter == 3
        assert evaluation2.parameter == 4
        # 生成一段数据测试
        flists = dict()
        for dev in context.devices:
            columns = list(map(str, worker.get_column()[dev]))
            flist = random_flist(dev, columns, size)
            flist._algorithm_id = dev
            context.crimp.get_container(dev).flist.extend_itemlist(flist)
            flists[dev] = flist

        worker.compute()

        status = {dev: list() for dev in context.devices}
        time = {dev: list() for dev in context.devices}
        score = {dev: list() for dev in context.devices}
        dcache = {dev: {
            'status': list(),
            'time': list(),
            'count': 0,
            'parameter': param
        } for dev in context.devices}
        e1cache = list()
        e2cache = list()
        # 调用相同的算法方法计算结果
        for dev in context.devices:
            flist = flists[dev]
            dev_status = status[dev]
            dev_time = time[dev]
            for i in range(size):
                s, t = stackdecision4_compute(
                    flist.get_lrdata(i), flist.get_hrdata(i), flist.get_time(i),
                    flist.get_bandspectrum(i), flist.get_peakfreqs(i), flist.get_peakpowers(i), dcache[dev])
                dev_status.extend(s)
                dev_time.extend(t)

        for dev in context.devices:
            flist = flists[dev]
            dev_status = status[dev]
            dev_time = time[dev]
            for i in range(len(dev_status)):
                s2 = evaluation2_compute(
                    dev_status[i], flist.get_lrdata(i), flist.get_hrdata(i), dev_time[i],
                    flist.get_bandspectrum(i), flist.get_peakfreqs(i), flist.get_peakpowers(i), e1cache
                )
                s3, s4 = evaluation3_compute(
                    dev_status[i], flist.get_lrdata(i), flist.get_hrdata(i), dev_time[i],
                    flist.get_bandspectrum(i), flist.get_peakfreqs(i), flist.get_peakpowers(i), e2cache
                )
                score[dev].append([s2, s3, s4])

        # 验证worker结果是否相同
        for dev in context.devices:
            assert context.crimp.get_result(dev).size(), len(score[dev])
            assert status[dev] == list(context.crimp.get_result(dev).rlist.get_all_status())
            assert score[dev] == list(context.crimp.get_result(dev).rlist.get_all_score())
