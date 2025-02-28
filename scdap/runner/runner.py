"""

@create on: 2021.04.19
下面是一段demo工装的runner使用例子

option = {
  "description": "1288-减速机7465-master",
  "tag": "1290",
  "devices": [],
  "worker": "normal_realtime",
  "decision": [
    {
      "function": "motor61",
      "all_channel": false,
      "select_feature": [0,1,2,3],
      "threshold": [40000000000.0,2000000000.0,15000000.0,200],
      "select_status": [0,1],
      "fea_run_width": 3,
      "transition_width": 5
    }
  ],
  "evaluation": [
    {
      "function": "hreducer88",
      "select_feature": [0,22],
      "threshold": [504356323043.226,23000407.1458213],
      "coefficient": [116.790470256473,105.786516857955]
    }
  ],
  "other": [],
  "extra": {
    "send": {
      "queue_name": "py.demo.result",
      "has_feature": true
    }
  }
}

runner = Runner('1290', option=option, load_mode='local')
data = {
    'Time': 111111111111,
    'MeanHf': 100,
    'MeanLf': 100,
    'Mean': 1,
    'STD': 1,
    'Feature1': '1.1,1.2',
    'Feature2': '1.1,1.2',
    'Feature3': '1.1,1.2',
    'Feature4': '1.1,1.2',
    'BandSpectrum': '1.1,1.2',
    'PeakFreqs': '1.1,1.2',
    'PeakPowers': '1.1,1.2',
    'Temperature': 50,
    'CustomFeature': '1.1,1.2',
}
for i in range(10):
    data['Time'] += 1
    result = runner.run('1290', data)
    print(result)
"""
import functools
from typing import List, Dict

from scdap import config
from scdap.wp import WorkerProcess
from scdap.api import health_define
from scdap.gop.loc import load_loc_program
from scdap.gop.func import get_program_option
from scdap.transfer.runner.get import RunnerGetController
from scdap.transfer.runner.send import RunnerSendController


class Runner(WorkerProcess):
    get_controller: RunnerGetController
    send_controller: RunnerSendController

    @property
    def process_type(self) -> str:
        return 'program'

    def __init__(self, tag: str, option: dict = None, load_mode: str = 'http', debug: bool = False):
        """
        启动一个runner
        在普通模式下, 如scdap.program启动的进程, 其
        数据来源是rabbitmq/http等外部服务, 通过的是scdap.transfer.rabbbitmq/http中获取数据, 再把数据放入到数据容器中
        局限的地方在于上述的两个数据交互方式的入口一个是来源rabbitmq队列或者http的接口
        他们并不提供一个显式的输入输入方式，也就是没有一个接口可以把其他地方来的数据塞入到数据容器中

        那么当其他服务需要启动算法并且从其他非正常来源中获取数据的时候就会有些麻烦，
        因为没有接口可以让外部的服务将非正常来源的数据塞入到数据容器中，
        所以runner的存在意义就是给其他外部服务提供一个算法调用的接口，外部服务可以通过runner启动一个算法
        并且可以通过调用result = runner.run(features)接口将特征塞入到算法中进行计算，并且其也会返回一段result以展示最终的计算结果

        :param tag: 点位id
        :param option: 点位配置
        :param load_mode: 点位配置的读取模式, http/sql/local, 如果option=None则将根据该参数进行读取
        :param debug:
        """
        tag = str(tag)

        load_net_mode = load_mode
        if load_mode == 'local':
            load_loc_program()
        if option is not None:
            load_net = False
            load_loc = True
        else:
            option = get_program_option(tag, net_load_mode=load_mode)
            load_net = config.LOAD_NET_OPTION
            load_loc = config.LOAD_LOCAL_OPTION

        self.logger_info(f'数据获取来源配置: '
                         f'net_mode: {load_net_mode}, '
                         f'from_loc: {load_loc}, '
                         f'from_net: {load_net}, '
                         f'from_reg: True.')

        super().__init__(
            tag, option, 'runner', debug,
            load_net=load_net, load_loc=load_loc, load_net_mode=load_net_mode
        )

        self._columns = dict()

        self.set_parameter()

    def get_health_defail(self) -> Dict[str, dict]:
        """
        获取健康度详细信息
        {
            algorithm_id_1(int): [{
                ...
            }]
        }
        :return:
        """
        get_health_define = functools.partial(health_define.get_health_define, load_mode=self.context.net_load_mode)
        return {
            dev: list(map(get_health_define, hd))
            for dev, hd in self._context.worker.get_health_define().items()
        }

    def run(self, algorithm_id: str, data: dict) -> List[dict]:
        """
        将特征数据放入算法中进行计算
        大小写无关

        data = {
            'Time': 111111111111,
            'MeanHf': 100,
            'MeanLf': 100,
            'Mean': 1,
            'STD': 1,
            'Feature1': '1.1,1.2',
            'Feature2': '1.1,1.2',
            'Feature3': '1.1,1.2',
            'Feature4': '1.1,1.2',
            'BandSpectrum': '1.1,1.2',
            'PeakFreqs': '1.1,1.2',
            'PeakPowers': '1.1,1.2',
            'Temperature': 50,
            'CustomFeature': '1.1,1.2',
        }
        -> result
        {
            "nodeId": int,
            "algorithmId": str,
            "dataTime": timestamp,
            "status": int,
            "health": {
                "trend": score1,
                "stab": score2,
                ...
            },
            "event": [...]
        }
        详细的数据结构可以至scdap.transfer.rabbitmq.send.coder中查看

        :param algorithm_id:
        :param data:
        :return:
        """
        self.get_controller.put_feature(algorithm_id, data)
        self.call_controllers()
        return self.send_controller.get_result()

    def serve_forever(self):
        pass
