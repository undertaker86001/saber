"""

@create on: 2021.01.10
"""
from datetime import datetime

import numpy as np

from scdap.core.mq import DataBroadcast, DataGetter
from scdap.data import FeatureList
from scdap.transfer.rabbitmq.get.coder import get_feature_list_encoder, get_feature_list_decoder

from unittests.config import rabbitmq_config
from unittests import rlist_utils
from unittests.util import randint, rand_timestamp, rand_string_array


class TestRabbitMQTransfer(object):
    conf = rabbitmq_config

    exchange = 'dap-cicd-test-gateway'
    result_queue = 'py.compute.result'

    @classmethod
    def setup_class(cls):
        from scdap import config
        config.ID_REFLICT_BY_API = False

        from scdap.core.mq.base import MQBaseClass
        MQBaseClass.clear_instance()

        from scdap import config
        config.FUNCTION_LIB = 'unittests.function'
        from scdap.frame.function import fset
        fset.reload()

    @classmethod
    def teardown_class(cls):
        from scdap.core.mq.base import MQBaseClass
        MQBaseClass.clear_instance()

    def send_data_broacast(self, broacast: DataBroadcast, device, obj, exchange):
        broacast.broadcast(exchange, obj, f'scene.{device}')

    @staticmethod
    def get_features(device: str = None, size=10, fsize: int = 24):
        device = str(device or randint(0, 200))

        feature_dict = {
            'algorithmId': device,
            'nodeId': int(device),
            'data': [{
                'meanHf': randint(0, 100000),
                'meanLf': randint(0, 100000),
                'mean': randint(0, 100000),
                'std': randint(0, 100000),

                # 注意对于时间相关的数值为毫秒时间戳, 并且前三位尽量为000
                # 在操作过程中因为涉及到转型会丢失小部分精度故可能出现某一个时间点误差为1毫秒导致检测不通过的情况
                'time': rand_timestamp(),
                'feature1': rand_string_array(fsize, 10),
                'feature2': rand_string_array(fsize, 10),
                'feature3': rand_string_array(fsize, 10),
                'feature4': rand_string_array(fsize, 10),
                'bandSpectrum': rand_string_array(fsize, 10),
                'peakFreqs': rand_string_array(randint(0, 10), 10),
                'peakPowers': rand_string_array(randint(0, 10), 10),

                'status': randint(0, 10),
                'customFeature': rand_string_array(fsize, 10),
                'temperature': randint(-100, 100),
                'extend': {'test': str(randint(0, 100))}
            } for _ in range(size)]}
        return feature_dict

    def test_get_coder(self):
        size = 10

        encoder = get_feature_list_encoder()
        decoder = get_feature_list_decoder()

        flist = FeatureList()
        feature_dict = self.get_features(size=size)
        decoder.decode(feature_dict, flist)

        # 检查编码类
        edict = encoder.encode(flist)
        assert edict['nodeId'] == feature_dict['nodeId']
        assert edict['algorithmId'] == feature_dict['algorithmId']
        for f1, f2 in zip(edict['data'], feature_dict['data']):
            # 原始数据默认没有hrtime
            # 编码器会默认编码hrtime, 所以可以直接手动删除
            f1.pop('hrtime', None)
            assert f1 == f2

        self._comp(flist, feature_dict)

    def _comp(self, flist, feature_dict):
        assert flist.node_id == feature_dict['nodeId']
        assert flist.size() == len(feature_dict['data'])

        for f, item in zip(flist, feature_dict['data']):
            assert f.meanhf == float(item['meanHf'])
            assert f.meanlf == float(item['meanLf'])
            assert f.mean == float(item['mean'])
            assert f.std == float(item['std'])

            assert f.status == float(item['status'])
            assert f.time == datetime.fromtimestamp(int(item['time'] / 1000))

            assert f.feature1.tolist() == np.exp(np.fromstring(item['feature1'], np.float, sep=',')).tolist()
            assert f.feature2.tolist() == np.exp(np.fromstring(item['feature2'], np.float, sep=',')).tolist()
            assert f.feature3.tolist() == np.exp(np.fromstring(item['feature3'], np.float, sep=',')).tolist()
            assert f.feature4.tolist() == np.exp(np.fromstring(item['feature4'], np.float, sep=',')).tolist()

            assert f.bandspectrum.tolist() == np.exp(np.fromstring(item['bandSpectrum'], np.float, sep=',')).tolist()
            assert f.peakfreqs.tolist() == np.exp(np.fromstring(item['peakFreqs'], np.float, sep=',')).tolist()
            assert f.peakpowers.tolist() == np.exp(np.fromstring(item['peakPowers'], np.float, sep=',')).tolist()
            assert f.customfeature.tolist() == np.fromstring(item['customFeature'], np.float, sep=',').tolist()
            assert f.temperature == int(item['temperature'])
            assert f.extend == item['extend']

    def test_dev_get_controller(self):
        from scdap.wp import Context

        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'rabbitmq', False)
        context.initial_worker('normal_realtime', [{'function': 'decision1'}])
        context.initial_crimp({'dump_error_data': False})
        context.bind_worker_and_crimp()

        from scdap.transfer.rabbitmq.get import RabbitMQGetController
        controller = RabbitMQGetController(context, **self.conf, get_timeout=3, exchange_name=self.exchange)
        broadcast = DataBroadcast(**self.conf)

        broadcast.add_exchange(self.exchange)
        fdict = self.get_features(dev)
        for o in fdict['data']:
            self.send_data_broacast(broadcast, dev, o, self.exchange)

        for _ in fdict['data']:
            controller()

        self._comp(context.crimp.get_container(dev).flist, fdict)

    def test_dev_get_controller_multi_dev(self):
        from scdap.wp import Context

        dev = '200'
        context = Context(dev, 'program', ['201', '202', '203'], [dev], 0, 'rabbitmq', False)
        context.initial_worker('normal_realtime', [{'function': 'decision1'}])
        context.initial_crimp({'dump_error_data': False})
        context.bind_worker_and_crimp()
        from scdap.transfer.rabbitmq.get import RabbitMQGetController
        controller = RabbitMQGetController(context, **self.conf, get_timeout=3, exchange_name=self.exchange)
        broadcast = DataBroadcast(**self.conf)
        size = 10
        broadcast.add_exchange(self.exchange)
        fdicts = dict()
        for dev in context.devices:
            fdict = self.get_features(dev, size)
            for o in fdict['data']:
                self.send_data_broacast(broadcast, dev, o, self.exchange)
            fdicts[dev] = fdict

        for _ in range(len(context.devices) * size):
            controller()

        for dev in context.devices:
            self._comp(context.crimp.get_container(dev).flist, fdicts[dev])

    def get_result(self, algorithm_id: str = None, size: int = 10):
        from scdap.data import ResultList
        algorithm_id = algorithm_id or randint(0, 1000)
        node_id = int(algorithm_id)
        rlist = ResultList(algorithm_id, node_id)
        rdict = list()

        def flatten(item):
            response = list()
            for val in item.values():
                response.extend(val)
            return response

        for i in range(size):
            rd = rlist_utils.random_item_dict()
            rlist.append_dict(**rd)
            result = rlist.get_last_ref()
            rdict.append({
                "nodeId": rlist.node_id,
                "algorithmId": rlist.algorithm_id,
                "dataTime": int(result.time.timestamp() * 1000),
                "status": result.status,
                "health": dict(zip(result.health_define, result.score)),
                "event": [{
                    'nodeId': event.node_id,
                    'algorithmId': event.algorithm_id,
                    "type": event.etype,
                    "alarmTime": int(event.time.timestamp() * 1000),
                    "startTime": int(event.start.timestamp() * 1000),
                    "endTime": int(event.stop.timestamp() * 1000),
                    "message": event.message,
                    "name": event.name,
                    "score": event.score,
                    "status": event.status,
                    "code": event.code,
                    "checkResult": event.check_result,
                    "detail": event.detail,
                    'extend': event.extend
                } for event in result.event],
                "statItem": {
                    'time': int(result.time.timestamp() * 1000),
                    'status': result.stat_item.status,
                    'size': result.stat_item.size,
                    'score': result.stat_item.score
                }
            })
        return rlist, rdict

    def test_send_coder(self):
        from scdap.transfer.rabbitmq import get_result_list_encoder

        encoder = get_result_list_encoder()
        rlist, rdict = self.get_result()
        for a, b in zip(encoder.encode(rlist), rdict):
            assert a == b

    def test_dev_send_controller(self):
        from scdap.wp import Context
        dev = '100'
        context = Context(dev, 'program', [], [dev], 0, 'rabbitmq', False)
        context.initial_worker('normal_realtime', [{'function': 'decision1'}])
        context.initial_crimp()
        context.bind_worker_and_crimp()
        from scdap.transfer.rabbitmq.send import RabbitMQSendController
        controller = RabbitMQSendController(
            context, **self.conf, exchange_name='', queue_name=self.result_queue
        )
        getter = DataGetter(**self.conf)
        getter.add_node('', self.result_queue)

        rdicts = list()
        try:
            r = context.crimp.get_result(dev)
            rlist, rdict = self.get_result(r.get_algorithm_id())
            r.rlist.extend_itemlist(rlist)
            controller()
            for i in range(rlist.size()):
                rdicts.extend([data.data for data in getter.get_data()])
        finally:
            getter.declare_queue(self.result_queue)

        assert rdict == rdicts
