"""

@create on: 2021.01.04
"""
import warnings
from datetime import datetime
from typing import List, Type, Optional, Union, Dict, Tuple

from scdap.api import device_define
from scdap.util.parser import parser_id
from scdap.logger import LoggerInterface
from scdap.util.tc import time_ms, time_s
from scdap.gop.func import list_parameter, register_parameter
from scdap.transfer import CRImplementer, get_crimp_names, get_crimp_class
from scdap.frame.worker import get_worker_class, BaseWorker, get_worker_names


class Context(LoggerInterface):
    """
    全局的上下文管理器
    数据结构与算法相关的类等需要在控制器中共享的内容都将保存在这里
    """

    def interface_name(self):
        return 'context'

    def __init__(self, algorithm_id: str, process_type: str,
                 devices: List[str], sub_devices: List[str], clock_time: int,
                 transfer_mode: str, debug: bool = False,
                 load_loc: bool = True, load_net: bool = True,
                 net_load_mode: str = 'http'):
        # 进程主算法点位编号
        self._algorithm_id: str = algorithm_id

        self._node_id: int = device_define.algorithm_id2node_id_batch([algorithm_id], False)[0]

        # 模板识别变量, 主要在某些特殊情况下用于区分相同以相同进程编号运行的多个进程
        self._process_type: str = process_type
        self._debug: bool = debug
        # 进程包含的设备列表
        self._devices: Tuple[str] = tuple(sorted(set(devices + [self._algorithm_id])))
        self._nodes: Tuple[int] = tuple(device_define.algorithm_id2node_id_batch(list(self._devices), False))

        # node(node_id)与device(algorithm_id)的互相映射关系
        self._device2node: Dict[str, int] = dict(zip(self._devices, self._nodes))
        self._node2device: Dict[int, str] = dict(zip(self._nodes, self._devices))

        self._sub_devices: Tuple[str] = tuple(sorted(set(sub_devices + [self._algorithm_id])))

        # sub_devices主要是汇总算法使用的, 因为每一个sub_option中可能其是一个多点位联动算法
        # 所以: len(sub_devices) <= len(devices)
        #       sub_devices 是 devices 的子集
        if not set(self._sub_devices).issubset(self._devices):
            raise self.wrap_exception(ValueError, f'sub_devices必须是devices的子集.')

        self._sub_nodes: Tuple[int] = tuple(map(self.algorithm_id2node_id, self._sub_devices))

        self._clock_time: int = clock_time

        self._load_loc = load_loc
        self._load_net = load_net
        self._net_load_mode = net_load_mode

        if transfer_mode not in get_crimp_names():
            raise self.wrap_exception(
                ValueError, f'请配置正确的transfer_mode, 可用的transfer_mode包括: {get_crimp_names()}'
            )
        # 数据传输模式
        self._transfer_mode: str = transfer_mode
        # 获取数据传输实现类
        self.crimp: CRImplementer = get_crimp_class(self._transfer_mode)()
        # 算法进程
        self.worker: Optional[BaseWorker] = None

    @property
    def algorithm_id(self) -> str:
        return self._algorithm_id

    @property
    def tag(self) -> str:
        return self._algorithm_id

    @property
    def device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用algorithm_id而不是device_id',
                      DeprecationWarning)
        return self._algorithm_id

    def algorithm_id2node_id(self, algorithm_id: str) -> int:
        return self._device2node[algorithm_id]

    def node_id2algorithm_id(self, node_id: int) -> str:
        return self._node2device[node_id]

    @property
    def group_tag(self) -> str:
        return self._algorithm_id

    @property
    def node_id(self) -> int:
        return self._node_id

    @property
    def process_type(self) -> str:
        return self._process_type

    @property
    def clock_time(self) -> int:
        return self._clock_time

    @property
    def devices(self) -> Tuple[str]:
        return self._devices

    @property
    def dsize(self) -> int:
        return len(self._devices)

    @property
    def nodes(self) -> Tuple[int]:
        return self._nodes

    @property
    def sub_devices(self) -> Tuple[str]:
        return self._sub_devices

    @property
    def sub_nodes(self) -> Tuple[int]:
        return self._sub_nodes

    @property
    def main_dsize(self) -> int:
        return len(self._sub_devices)

    @property
    def multi_dev(self) -> bool:
        return self.dsize > 1

    @property
    def transfer_mode(self) -> str:
        return self._transfer_mode

    @property
    def debug(self) -> bool:
        return self._debug

    @property
    def load_loc(self) -> bool:
        return self._load_loc

    @property
    def load_net(self) -> bool:
        return self._load_net

    @property
    def net_load_mode(self):
        return self._net_load_mode

    def systime(self) -> datetime:
        """
        获取当先系统时间
        进程中的获取系统当前时间的操作都尽量使用该接口

        :return: 当前系统时间
        """
        return datetime.now()

    def systimestamp_ms(self) -> int:
        """
        获取当先系统时间(毫秒时间戳)
        进程中的获取系统当前时间的操作都尽量使用该接口

        :return: 当前系统时间
        """
        return time_ms()

    def systimestamp_s(self) -> float:
        return time_s()

    def initial_worker(self, worker_class: Union[str, Type[BaseWorker]],
                       decision: list = None, evaluation: list = None, other: list = None,
                       show_compute_result: bool = False, option: dict = None):
        """
        创建算法工作组
        """
        if isinstance(worker_class, str):
            if worker_class not in get_worker_names():
                raise self.wrap_exception(
                    ValueError,
                    f'请配置正确的worker_class, 可用的worker_class包括: {get_worker_names()}'
                )
            worker_class = get_worker_class(worker_class)

        elif not isinstance(worker_class, BaseWorker):
            raise self.wrap_exception(Exception, f'请确保传入的worker_class继承自BaseWorker.')

        if worker_class.process_type() != self.process_type:
            raise TypeError(f'worker_class的process_type类型错误, '
                            f'worker_class: {worker_class}.process_type必须等于{worker_class.process_type()}, '
                            f'但context.process_type等于{self.process_type}.')

        self.worker = worker_class(
            self.algorithm_id, self.devices, self.crimp,
            decision, evaluation, other, self.debug, show_compute_result, net_load_mode=self.net_load_mode,
            **(option or dict()))

        # 初始化算法工作组
        self.worker.initial()

    def initial_crimp(self, container_option: dict = None, result_option: dict = None):
        """
        初始化数据容器

        :param container_option: 容器配置
        :param result_option: 容器配置
        """
        self.crimp.initial(self, container_option, result_option)

    def bind_worker_and_crimp(self):
        """
        将worker与crimp互相绑定与挂钩

        """
        for container, result in self.crimp.generator_cr():
            container.bind_worker(self.worker)
            result.bind_worker(self.worker)

        self.worker.bind_crimp()

    def reset(self):
        self.worker.reset()
        self.crimp.reset()

    def clear(self):
        self.worker.clear()
        self.crimp.clear()

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
        if gnet is None:
            gnet = self.load_net
        if gloc is None:
            gloc = self.load_loc
        if net_load_mode is None:
            net_load_mode = self.net_load_mode

        parameter = self._load_parameter(parameter, self.worker, gnet, gloc, net_load_mode)

        self.worker.set_parameter(parameter)

    def _load_parameter(self, parameter: dict, worker: BaseWorker, gnet: bool, gloc: bool, net_load_mode: str):
        """
        载入数据, 如果参数parameter传入了对应算法的参数, 则直接使用传入的算法参数
        如果没有则从线上获取参数
        param = {
            fid: dict(),
            fid: dict(),
            ...
        }

        :param parameter:
        :param worker:
        :param gnet:
        :param gloc:
        :param net_load_mode:
        :return:
        """
        for fid, val in (parameter or dict()).items():
            if isinstance(fid, str):
                fid = parser_id(fid)
            # 缓存参数
            # 缓存的参数拥有最高的读取优先度
            register_parameter(self.tag, fid, self.process_type, val)
        parameter = list_parameter(worker.tag, worker.get_function_ids(), worker.process_type(),
                                   gnet, gloc, net_load_mode)
        return parameter

    def print_message(self):
        self.logger_info('-' * 50)
        self.logger_info('context'.center(50))
        self.logger_info('-' * 50)
        self.logger_info(f'debug_mode: {self.debug}')
        self.logger_info(f'tag: {self.tag}')
        self.logger_info(f'node_id: {self.node_id}')
        self.logger_info(f'total_nodes: {self.nodes}')
        self.logger_info(f'sub_nodes: {self.sub_nodes}')
        self.logger_info(f'algorithm_id: {self.algorithm_id}')
        self.logger_info(f'total_devices: {self.devices}')
        self.logger_info(f'sub_devices: {self.sub_devices}')
        self.logger_info(f'clock_time : {self.clock_time}s')
        self.logger_info(f'process_type: {self.process_type}')
        self.logger_info(f'transfer_mode: {self.transfer_mode}')
        self.logger_info('-' * 50)
        self.logger_info('worker'.center(50))
        self.logger_info('-' * 50)
        self.worker.print_message()
        self.logger_info('-' * 50)
