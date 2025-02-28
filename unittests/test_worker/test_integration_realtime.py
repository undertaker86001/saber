"""

@create on: 2021.01.14
"""
import pytest

from scdap.wp import Context

from unittests.flist_utils import random_flist

from .function import RealtimeIntegration5, realtimeintegration5_compute


class TestIntegrationRealtime(object):
    @classmethod
    def setup_class(cls):
        from scdap import config
        config.ID_REFLICT_BY_API = False

    def test_initial_worker(self):
        class Test6(RealtimeIntegration5):

            @staticmethod
            def get_function_name() -> str:
                return 'test6'

            def is_realtime_function(self) -> bool:
                return False

        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'mediator', False)
        # 只允许配置实时算法
        with pytest.raises(Exception):
            context.initial_worker(
                'integration_realtime',
                other=[{'function': Test6}],
            )

    def test_run_simple_dev_worker(self):
        self._test_run_worker(['100'], 1000)

    def test_run_multi_dev_worker(self):
        self._test_run_worker(['100', '101', '102'], 1000)

    def _test_run_worker(self, dev, size):
        context = Context(dev[0], 'program', dev, [dev[0]], 0, 'mediator', False)
        context.initial_crimp()
        context.initial_worker(
            'integration_realtime',
            other=[{'function': RealtimeIntegration5}],
        )
        context.bind_worker_and_crimp()
        worker = context.worker
        # 配置参数
        worker.set_parameter(parameter={5: {'parameter': 2}})
        decision = worker._fid_to_func[5]

        # 检查参数是否正确配置
        assert decision.parameter == 2

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
        score = {dev: list() for dev in context.devices}
        dcache = list()
        # 调用相同的算法方法计算结果
        for i in range(size):
            for dev in context.devices:
                flist = flists[dev]
                s1, s2, s3 = realtimeintegration5_compute(
                    flist.get_lrdata(i), flist.get_hrdata(i), flist.get_time(i),
                    flist.get_bandspectrum(i), flist.get_peakfreqs(i), flist.get_peakpowers(i), dcache)

                status[dev].append(s1)
                score[dev].append([s2, s3])
        # 验证worker结果是否相同
        for dev in context.devices:
            assert context.crimp.get_result(dev).size() == size
            assert status[dev] == list(context.crimp.get_result(dev).rlist.get_all_status())
            assert score[dev] == list(context.crimp.get_result(dev).rlist.get_all_score())
