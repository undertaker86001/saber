"""

@create on: 2021.01.14
"""
from scdap.wp import Context

from unittests.flist_utils import random_flist

from .function import stackintegration6_compute, StackIntegration6


class TestNormalRealtime(object):
    @classmethod
    def setup_class(cls):
        from scdap import config
        config.ID_REFLICT_BY_API = False

    def test_run_simple_dev_worker_nostack(self):
        self._test_run_worker(['100'], 1000, 1)

    def test_run_multi_dev_worker_nostack(self):
        self._test_run_worker(['100', '101', '102'], 1000, 1)

    def test_run_simple_dev_worker(self):
        self._test_run_worker(['100'], 1000, 10)

    def test_run_multi_dev_worker(self):
        self._test_run_worker(['100', '101', '102'], 1000, 10)

    def _test_run_worker(self, dev, size, param):
        context = Context(dev[0], 'program', dev, [dev[0]], 0, 'mediator', False)
        context.initial_crimp()
        context.initial_worker(
            'integration_stack',
            other=[{'function': StackIntegration6}],
        )
        context.bind_worker_and_crimp()
        worker = context.worker
        # 配置参数
        worker.set_parameter(parameter={6: {'parameter': param}})
        decision = worker._fid_to_func[6]
        # 检查参数是否正确配置
        assert decision.parameter == param
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
            'parameter': param,
            'score_cache': list()
        } for dev in context.devices}
        # 调用相同的算法方法计算结果
        for dev in context.devices:
            flist = flists[dev]
            dev_status = status[dev]
            dev_time = time[dev]
            dev_score = score[dev]
            for i in range(size):
                s1, t, s2 = stackintegration6_compute(
                    flist.get_lrdata(i), flist.get_hrdata(i), flist.get_time(i),
                    flist.get_bandspectrum(i), flist.get_peakfreqs(i), flist.get_peakpowers(i), dcache[dev])
                dev_status.extend(s1)
                dev_time.extend(t)
                dev_score.extend(s2)

        # 验证worker结果是否相同
        for dev in context.devices:
            assert context.crimp.get_result(dev).size() == len(score[dev])
            assert status[dev] == list(context.crimp.get_result(dev).rlist.get_all_status())
            assert score[dev] == list(context.crimp.get_result(dev).rlist.get_all_score())
