"""

@create on: 2021.01.06
"""
import os
import pickle

from typing import Dict, Union
from collections import Counter
from datetime import datetime, timedelta

from scdap import config
from scdap.program import Program
from scdap.flag import option_key
from scdap.data import FeatureList
from scdap.api import device_history
from scdap.gop.check import dump_option
from scdap.util.parser import parser_id
from scdap.logger import logger, LoggerInterface
from scdap.transfer.designer.get import DesignerGetController
from scdap.transfer.designer.send import DesignerSendController
from scdap.gop.func import register_parameter, clear_register_parameter, register_option, get_parameter

from .fnresult import FinalResult


class Designer(LoggerInterface):
    def interface_name(self):
        return 'designer'

    @staticmethod
    def process_class():
        return Program

    @property
    def process_type(self):
        return 'program'

    def __init__(self, tag: str, *, auto: bool = False,
                 clock_time: int = None, devices: list = None, worker='normal_realtime',
                 decision: list = None, evaluation: list = None, other: list = None,
                 std_level: str = None, extra: dict = None):
        """
        该类别用于算法的测试与设计，请设计人员使用该类进行算法设计与调试

        :param tag: 设备标签，每个设备组都拥有一个主算法点位编号作为设备标签
        :param auto: 是否自动从配置存放地点读取配置与参数，假设为True，则auto后面的参数无用，类将自动读取配置数据与参数数据，
                        为False请在创建类的时候详细输入各项配置参数
        :param devices: 设备组，假设该算法需要多设备联动，则请输入所有需要联动的设备，可不包括主设备标签
        :param worker:  算法工作组类型，请详细阅读scdap.frame.worker.__init__中关于各类算法工作组的介绍后进行设置，假设需要自行设计
                        也可传入设计后的算法工作组的类，例如假设设计了一个算法工作组ExampleWorker()，则设置worker=ExampleWorker
        :param decision: 识别算法集合，为列表，根据需要添加所需的算法，包含的算法需要以字典的方式储存：
                        decision: [{'function': 'dfunction', 'parameter': '...', ...}]
        :param evaluation: 评价算法集合，为列表，根据需要添加所需的算法，与decision类似的设置方法
        :param other: 其他算法集合，为列表，根据需要添加所需的算法，与decision类似的设置方法
        :param std_level: 日志输出登记: DEBUG/SECO/INFO/WARNING/ERROR
        :param extra: 其他需要设置的参数
        """
        if not isinstance(tag, str):
            logger.warning('警告, 请配置tag为字符串.')

        tag = str(tag)

        if devices:
            for dev in devices:
                if not isinstance(dev, str):
                    logger.warning('警告, 请配置devices为包含字符串的列表.')

        if devices is not None:
            devices = list(map(str, devices))

        self.auto = auto

        load_loc = config.LOAD_LOCAL_OPTION
        load_net = config.LOAD_NET_OPTION
        load_net_mode = config.LOAD_NET_OPTION_MODE

        self.logger_info(f'数据获取来源配置: '
                         f'net_mode: {load_net_mode}, '
                         f'from_loc: {load_loc}, '
                         f'from_net: {load_net}, '
                         f'from_reg: True.')
        std_level = std_level or config.STDOUT_LEVEL

        logger.set_normal_stdout(std_level)

        # designer模式下容器size不用限制
        config.CONTAINER_MAXLEN = None
        config.RESULT_MAXLEN = None

        if not self.auto:
            # load_loc = False
            # load_net = True
            option = {
                option_key.tag: tag,
                option_key.devices: devices or list(),
                option_key.worker: worker,
                option_key.decision: decision or list(),
                option_key.evaluation: evaluation or list(),
                option_key.other: other or list(),
                option_key.clock_time: clock_time,
                option_key.extra: extra or dict()
            }
            register_option(self.process_type, option)

        self._process = self.process_class()(tag, 'designer', None, config.DEBUG,
                                             load_loc, load_net, load_net_mode,
                                             False)

        self._context = self._process.context
        self._get_controller: DesignerGetController = self._process.get_controller
        self._send_controller: DesignerSendController = self._process.send_controller

    @property
    def tag(self):
        return self._context.tag

    @property
    def devices(self):
        return self._context.devices

    @property
    def worker(self):
        return self._context.worker

    @property
    def column(self):
        return self.worker.get_column()

    @property
    def health_define(self):
        return self.worker.get_health_define()

    @property
    def default_score(self):
        return self.worker.get_default_score()

    @property
    def score_limit(self):
        return self.worker.get_score_limit()

    def clear(self):
        self._process.clear()

    def reset(self):
        self._process.reset()

    def set_parameter(self, parameter: dict = None,
                      gnet: bool = None, gloc: bool = None, net_load_mode: str = None):
        """
        设置参数，算法将读取param中的参数
        传入参数表规则：
        param = {
            fid: dict(),
            fid: dict(),
            ...
        }

        :param parameter: 参数表，如果设置为None则将根据gnet与gloc的设置从对应位置获取阈值
        :param gnet: 是否从数据库中获取阈值
        :param gloc: 是否从本地获取阈值
        :param net_load_mode: 从网络中读取数据的模式 http/sql
        """
        self._process.set_parameter(parameter, gnet, gloc, net_load_mode)

    def register_parameter(self, parameter: dict):
        """
        暂时缓存算法参数至进程本地中
        缓存的参数将拥有最高的获取优先度
        {
            fid: {...},
            fid: {...}
        }

        :param parameter:
        :return:
        """
        for fid, p in parameter.items():
            register_parameter(self.tag, fid, self.process_type, p)

    def clear_register_parameter(self):
        """
        清空缓存的所有算法参数

        :return:
        """
        clear_register_parameter()

    def run(self, start: datetime = None, stop: datetime = None, delta: Union[timedelta, int] = None,
            show: bool = True, data: Dict[str, FeatureList] = None, dump_history: bool = False,
            history_dir: str = 'history') \
            -> Dict[str, FinalResult]:
        """
        输入时间段或者数据, 运行设计者类, 计算结果
        如果配置了data, 则默认从data中获取数据而不自行根据时间段读取数据

        :param history_dir: 在dump_history = True情况下, 配置历史stat数据缓存的位置
        :param start: 数据起始时间
        :param stop:  数据截止时间
        :param delta: 获取数据的间隔
        :param show: 是否格式化输出计算统计结果
        :param data: 数据
        :param dump_history: 是否保存stat状态, 主要是用于上传历史数据
        :return:
        """
        # 配置数据, 配置需要获取的数据时间段准备运行
        if isinstance(delta, int):
            delta = timedelta(seconds=delta)

        if data:
            self._get_controller.set_flist(data)
        else:
            self._get_controller.set_time_range(start, stop, delta)

        while not self._get_controller.is_finished():
            self._process.call_controllers()

        result = dict()
        for dev, (flist, rlist) in self._send_controller.get_cache().items():
            fnresult = FinalResult(dev, [], flist, rlist)
            result[dev] = fnresult

            if show:
                self._show_result(fnresult)

        if dump_history:
            # 因为stat是基于数据时间进行统计
            # 如统计时间为1分钟
            # 他是在判断当前数据时间是否大于等于下一次的统计时间来判断是否需要统计数据
            # 这话意味着如果触发不到上述条件, 则没办法统计最后残存的数据
            # 所以需要强制的统计最后一段数据
            self._process.stat_controller.finish({aid: v.result.get_last_ref() for aid, v in result.items()})
            self.dump_history(start, stop, history_dir)

        return result

    def _show_result(self, fnresult: FinalResult, show_detail: bool = True):
        """
        展示结果

        :param fnresult:
        :param show_detail:
        """
        if fnresult.container.size() == 0:
            radio = 0.
        else:
            radio = round(fnresult.result.size() / fnresult.container.size()) * 100

        print(f'设备信息:', *fnresult.info)
        print(f'特征数据量: {fnresult.container.size()}, '
              f'结果数据量: {fnresult.result.size()}, '
              f'特征数据量/结果数据量: {radio}%')
        status = list(fnresult.result.get_all_status())
        status_counter = Counter(status)
        if show_detail:
            print('设备状态:')
            print('-' * 49)
            print(f"{'id':<6} | {'size':<10} | proportion(%)")
            print('-' * 49)
            print(f"{'total':<6} | {fnresult.result.size():<10} | {1:.2%}")
            for key, value in status_counter.items():
                print(f"{key:<6} | {value:<10} | {value / len(status):.2%}")
            print('-' * 49)

    def dump_option(self, tag: int = None, devices: list = None, clock_time: int = None,
                    path=None, to_json=True, **extra):
        """
        导出指定设备的配置, 详细的配置为designer初始化过程中配置的内容, 在这里只能够指定新的算法点位编号等内容, 但是不能修改配置的算法内容

        :param path: 导出配置的路径，包含文件名,如果为None，则默认导出到根目录下，group_tag.json文件
        :param tag: 设备标签，每个设备组都拥有一个主算法点位编号作为设备标签，如果默认为None，则直接返回目前设计模式设置的设备配置
        :param clock_time: 数据运行的间隔时间
        :param devices: 设备组
        :param to_json: 是否转换成json文件
        :param extra: 其他需要设置的参数，不建议使用
        """
        tag = tag or self._context.tag
        path = path or f'device-option-{self.process_type}-{tag}.json'
        if to_json:
            if path is None:
                raise Exception('请配置一个可读写的路径用于保存配置文件.')
            self.logger_info(f'保存配置文件至-> {path}')
        else:
            path = None
        option = self._process.option
        option[option_key.tag] = tag
        option[option_key.devices] = devices
        option[option_key.clock_time] = clock_time

        if extra:
            option[option_key.extra] = extra

        option.pop(option_key.functions, None)
        for fdict in option[option_key.decision] + option[option_key.evaluation] + option[option_key.other]:
            fname = fdict[option_key.function]
            if not isinstance(fname, str):
                fname = fname.get_function_name()
            parameter = get_parameter(self.tag, parser_id(fname), self.process_type)
            fdict.update(parameter)

        # summary使用
        option[option_key.extra].get(option_key.worker, dict()).pop(option_key.sub_option, None)
        return dump_option(**option, path=path)

    def dump_history(self, start: datetime = None, stop: datetime = None, save_dir: str = 'history') -> dict:
        """
        将结果数据已固定格式抛出
        主要是用于覆盖历史数据至数据库中

        :param start: 起始时间
        :param stop: 截止时间
        :param save_dir: 保存的文件根目录
        :return:
        """
        result = dict()

        for aid, (flist, rlist) in self._send_controller.get_cache().items():
            if rlist.size() == 0:
                print(f'aid={aid}的数据量为0, 跳过该段数据不保存.')
                continue

            start = rlist.get_ref(0).time if start is None else start
            stop = rlist.get_ref(rlist.size() - 1).time if stop is None else stop

            result = device_history.parse_stat_history(
                aid, self._context.algorithm_id2node_id(aid), start, stop,
                self.health_define, self.default_score, self.score_limit, rlist)

            path = os.path.join(
                save_dir, aid,
                f'{aid}_{start.strftime("%Y%m%d-%H%M%S")}_{stop.strftime("%Y%m%d-%H%M%S")}.pkl'
            )

            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))

            with open(path, 'wb') as f:
                pickle.dump(result, f)
            print(f'历史文件已保存至: {path}')

        return result
