"""

@create on: 2020.01.20

算法工作组基类
"""
import warnings
from copy import deepcopy
from abc import ABCMeta, abstractmethod
from typing import List, Dict, Union, Type, Tuple, Generator, Callable

from scdap.flag import option_key
from scdap.data import Container, Result
from scdap.logger import LoggerInterface
from scdap.transfer.base import CRImplementer

from ..api import APICreater
from ..function import BaseFunction, fset

TYPE_CR = Tuple[Container, Result]
GENERATOR_CR = Generator[TYPE_CR, None, None]

TYPE_DCR = Tuple[str, Container, Result]
GENERATOR_DCR = Generator[TYPE_DCR, None, None]


class BaseWorker(LoggerInterface, metaclass=ABCMeta):
    # 下列参数用于配置算法内调用的容器允许使用的接口
    # 在decorate_crimp中获取后进行配置
    # 根据worker数据驱动方式以及实现方式的不同, 按需在各个子类worker中进行配置
    # 如某些算法的数据接口不支持某些功能则通过配置谢列参数可进行修改与控制

    # 对于container, 具体的参数可查看scdap.frame.api.capi.ContainerAPI.__new__(...)
    # 默认的参数都开启了, 可根据需求关闭
    container_api_kwargs = {
        'decision': {},
        'evaluation': {},
        'other': {},
    }

    # 对于result, 具体的参数可查看scdap.frame.api.rapi.ResultAPI.__new__(...)
    # 默认的参数都开启了, 可根据需求关闭
    result_api_kwargs = {
        'decision': {},
        'evaluation': {},
        'other': {},
    }

    def interface_name(self):
        return 'worker'

    @staticmethod
    @abstractmethod
    def process_type() -> str:
        pass

    @abstractmethod
    def is_realtime_worker(self) -> bool:
        # 匹配算法的堵塞类型是否符合规范
        # 堵塞算法工作组可适用的算法类型: 实时算法/堵塞算法
        # 实时算法工作组可适用的算法类型: 实时算法
        pass

    def __init__(self, tag: str, devices: Tuple[str], crimp: CRImplementer,
                 decision: list = None, evaluation: list = None, other: list = None,
                 debug: bool = False, show_compute_result: bool = False, net_load_mode: str = 'http', **option):

        # 算法工作组标签
        self._tag: str = tag
        # 算法工作组包含的算法点位编号
        # 已经把tag包含进去了
        self._devices: Tuple[str] = devices

        if self._tag not in self._devices:
            raise self.wrap_exception(Exception, f'worker的初始化参数中devices必须包含tag.')

        self._debug: bool = debug
        self._show_compute_result: bool = show_compute_result
        self._net_load_mode = net_load_mode
        # 多设备进程必须使用多设备算法工作组
        if self.multi_dev and not self.can_run_with_multi_dev:
            raise self.wrap_exception(Exception, f'无法运行多个设备.')

        # 算法组
        self._opt_decision = decision or list()
        self._opt_evaluation = evaluation or list()
        self._opt_other = other or list()

        if len(self._opt_decision + self._opt_evaluation + self._opt_other) == 0:
            raise self.wrap_exception(Exception, f'必须至少配置一个算法.')

        # worker参数
        # option.extra.worker
        self._option = option or dict()

        # 识别算法集
        self._decision: List[BaseFunction] = list()

        # 评价算法集
        self._evaluation: List[BaseFunction] = list()

        # 其他算法
        self._other: List[BaseFunction] = list()

        # 算法总和
        self._functions: List[BaseFunction] = list()

        # 算法标签对算法的映射字典
        self._fid_to_func: Dict[int, BaseFunction] = dict()

        # 算法组算法编号
        self._function_ids: List[int] = list()
        # 算法组名称
        self._function_names: List[str] = list()

        # 健康度定义
        self._health_define: Dict[str, List[str]] = dict()

        self._health_define_size = 0
        # 默认的健康度数值
        self._default_score: Dict[str, List[int]] = dict()
        # 健康度数值限制
        # 是否解开限制(封印)
        # 默认的健康度范围未[0, 100]
        # 其中0代表延续之前的健康度数值
        self._score_limit: Dict[str, List[bool]] = dict()
        self._score_reverse: Dict[str, List[bool]] = dict()

        # 算法工作组所需的特征
        self._column: Dict[str, List[str]] = dict()
        # 数据容器
        self._crimp: CRImplementer = crimp
        self._api_creater: APICreater = APICreater()

        # 数据驱动方法
        # 是一个generator
        # 通过for循环逐个获取需要计算的数据
        self._drive_data: Callable[[bool, bool], GENERATOR_DCR] = self._drive_data_simple
        if self.multi_dev:
            self._drive_data: Callable[[bool, bool], GENERATOR_DCR] = self._drive_data_multi

    def __str__(self) -> str:
        return self.get_worker_name()

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> ' \
               f'name: {self.get_worker_name()} functions: {self._function_names}]'

    def _get_option(self, option: dict, key: str, default):
        val = option.get(key, None)
        if val is None:
            return default
        return val

    @property
    def tag(self) -> str:
        return self._tag

    @property
    def algorithm_id(self) -> str:
        return self._tag

    @property
    def device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        return self._tag

    @property
    def devices(self) -> Tuple[str]:
        return self._devices

    @property
    def dsize(self) -> int:
        return len(self._devices)

    @property
    def multi_dev(self) -> bool:
        return self.dsize > 1

    @property
    def debug(self) -> bool:
        return self._debug

    @property
    def can_run_with_multi_dev(self) -> bool:
        """

        :return: 是否可以多设备运行
        """
        return True

    @staticmethod
    @abstractmethod
    def get_worker_name() -> str:
        """
        获取算法工作组名称

        :return: 名称
        """
        pass

    @property
    def name(self) -> str:
        """
        获取算法工作组名称

        :return: 名称
        """
        return self.get_worker_name()

    def get_column(self) -> Dict[str, List[str]]:
        """
        获取算法配置的需要使用到的特征

        :return: 特征需求列表
        """
        return deepcopy(self._column)

    def get_default_score(self) -> Dict[str, List[int]]:
        """
        获取算法配置的默认的健康度数值

        :return: 默认健康度数值列表
        """
        return deepcopy(self._default_score)

    def get_health_define(self) -> Dict[str, List[str]]:
        return deepcopy(self._health_define)

    def get_score_limit(self) -> Dict[str, List[bool]]:
        return deepcopy(self._score_limit)

    def get_score_reverse(self) -> Dict[str, List[bool]]:
        return deepcopy(self._score_reverse)

    def get_function_ids(self) -> List[int]:
        return self._function_ids.copy()

    def _register_decision(self, function: Union[list, tuple, dict]):
        """
        注册识别算法

        :param function: 待注册算法配置列表 {'function': 'function_name...'}
        """
        self._register_function(function, self._decision, 'decision')

    def _register_evaluation(self, function: Union[list, tuple, dict]):
        """
        注册评价算法

        :param function: 待注册算法配置列表 {'function': 'function_name...'}
        """
        self._register_function(function, self._evaluation, 'evaluation')

    def _register_other(self, function: Union[list, tuple, dict]):
        """
        注册其他算法

        :param function: 待注册算法配置列表 {'function': 'function_name...'}
        """
        self._register_function(function, self._other, 'other')

    @classmethod
    def _check_function_type(cls, function: Type[BaseFunction]):
        return issubclass(function, BaseFunction)

    def _register_function(self, function: Union[list, tuple, dict], loc: list, function_type: str):
        """
        注册算法

        :param function: 待注册算法配置列表 {'function': 'function_name...'}
        :param loc: 待放置的算法类型列表 self._decision / self._evaluation / self._other
        :param function_type:
        """
        # 如果是列表类型的传入参数则递归调用并注册
        if isinstance(function, (list, tuple)):
            for o in function:
                self._register_function(o, loc, function_type)
            return

        # 检查输入的算法配置是否有问题
        if not isinstance(function, dict) or option_key.function not in function:
            raise self.wrap_exception(TypeError, '设备配置类型错误, 应配置为{"function": Union[BaseFunction, str], ...}.')

        # 获得算法类型
        # {
        #   'function': Union[BaseFunction, str]
        # }
        # 通过算法名称自动查询与导入算法类
        # 该情况一般在program下使用
        fun_type = function[option_key.function]
        if isinstance(fun_type, str):
            fun_type = fset.get_function_class(fun_type)
        elif self._check_function_type(fun_type):
            fset.register_function(fun_type)
        else:
            raise self.wrap_exception(ValueError,
                                      '参数function配置错误, 应配置为{"function": Union[BaseFunction, str], ...}.')

        if fun_type.get_function_type() != function_type:
            raise self.wrap_exception(TypeError, f'算法类型错误, 算法类型与配置的算法所在的参数位置不匹配.')

        # 初始化算法
        if fun_type.is_health_function():
            sindex = self._health_define_size
        else:
            sindex = -1

        fun = fun_type(
            self.tag, self.devices, len(self._functions), sindex,
            self.debug, self._net_load_mode, function.get(option_key.global_parameter)
        )

        if fun in self._function_names:
            raise self.wrap_exception(Exception, f'算法: {fun.get_function_name()}已经存在, 请不要配置重复的算法.')

        if not fun.is_realtime_function() and self.is_realtime_worker():
            raise self.wrap_exception(
                Exception,
                f'算法: [{fun.get_function_name()}]不是实时算法, '
                f'但算法工作组: [{self.get_worker_name()}]是实时算法工作组.')

        fun.initial()

        # 更新算法工作组配置
        loc.append(fun)

        # 添加算法
        self._functions.append(fun)
        self._function_ids.append(fun.get_function_id())

        self._function_names.append(fun.get_function_name())
        self._fid_to_func[fun.get_function_id()] = fun

        self._health_define_size += len(fun.get_health_define())

    def _bind_function_to_device(self):
        """
        将算法的相关信息按照点位进行登记

        """
        for aid in self.devices:
            sub_column = set()
            health_define = list()
            score_limit = list()
            score_reverse = list()
            default_score = list()

            for function in self._functions:
                # 特征配置
                sub_column.update(function.get_column())

                # 健康度相关配置
                if len(set(health_define).difference(function.get_health_define())) != len(health_define):
                    raise self.wrap_exception(Exception, f'请不要配置具有相同health_define的算法.')

                health_define.extend(function.get_health_define())
                score_limit.extend(function.get_score_limit())
                score_reverse.extend(function.get_score_reverse())
                default_score.extend(function.get_default_score())

            # 校验数量是否一致
            if len(health_define) != len(score_limit) or \
                    len(health_define) != len(default_score) or \
                    len(health_define) != len(score_reverse):
                raise self.wrap_exception(Exception,
                                          f'算法配置的health_define/score_limit/default_score/score_reverse'
                                          f'的返回数据数量不一致.')

            self._column[aid] = list(sub_column)
            self._health_define[aid] = health_define
            self._score_limit[aid] = score_limit
            self._score_reverse[aid] = score_reverse
            self._default_score[aid] = default_score

    def _print_result(self, result: Result = None, position: int = None):
        """
        输出结果日志
        输出顺序不代表实际的计算顺序
        """
        if self._show_compute_result and result:
            self.logger_seco(
                f"[aid: {result.get_algorithm_id()}, nid: {result.get_node_id()}] "
                f"[time: {result.rlist.get_time(position)}] "
                f"[status: {result.rlist.get_status(position)}] "
                f"[score: {result.rlist.get_score(position)}]"
            )

    def _run_function(self, function: BaseFunction, algorithm_id: str, container: Container, result: Result):
        """
        运行算法

        """
        function(self._api_creater.get_capi(function, container), self._api_creater.get_rapi(function, result))

    def bind_crimp(self):
        """
        该方法将在初始化的时候调用
        用于传入初始化后的数据容器
        用于设置于基于worker/function对数据容器进行初始化与定制化修改
        """
        # 创建各算法专属的数据容器
        # 每个算法在调用的时候只使用专属自己的数据容器
        for cont, res in self._crimp.generator_cr():
            for fun in self._decision:
                self._api_creater.register_capi(fun, cont, self.container_api_kwargs['decision'])
                self._api_creater.register_rapi(fun, res, self.result_api_kwargs['decision'])

            for fun in self._evaluation:
                self._api_creater.register_capi(fun, cont, self.container_api_kwargs['evaluation'])
                self._api_creater.register_rapi(fun, res, self.result_api_kwargs['evaluation'])

            for fun in self._other:
                self._api_creater.register_capi(fun, cont, self.container_api_kwargs['other'])
                self._api_creater.register_rapi(fun, res, self.result_api_kwargs['other'])

    def initial(self):
        """
        初始化

        """
        self._initial_function()
        self._bind_function_to_device()

    def _initial_function(self):
        """
        初始化算法, 主要是将算法进行注册和登记

        """
        self._register_decision(self._opt_decision)
        self._register_evaluation(self._opt_evaluation)
        self._register_other(self._opt_other)

    def get_health_info(self) -> Dict[str, dict]:
        """
        获取健康度相关的算法信息
        只有拥有健康度的算法类能获得到内容
        必须在set_parameter()之后调用

        :return: 健康度相关的算法信息列表
        """
        info = list()
        for f in self._functions:
            info.extend(f.get_health_info())
        return {aid: info.copy() for aid in self.devices}

    def print_message(self):
        """
        输出算法工作组配置信息
        """
        self.logger_info(f'worker: {self.name}')
        self.logger_info(f'devices: {self.devices}')
        self.logger_info(f'function: {self._function_names}')
        for aid in self.devices:
            self.logger_info(f'[algorithm_id:{aid}]:')
            self.logger_info(f'column: {self._column[aid]}')
            self.logger_info(f'health_define: {self._health_define[aid]}')
            self.logger_info(f'default_score: {self._default_score[aid]}')
            self.logger_info(f'score_limit: {self._score_limit[aid]}')
            self.logger_info(f'score_reverse: {self._score_reverse[aid]}')

    @abstractmethod
    def compute(self):
        """
        调用各注册算法function.compute()进行计算
        不同的worker拥有不同的调用模式，需要根据需求各自进行相应实现
        """
        pass

    def flush(self):
        [result.flush() for result in self._crimp.generator_result()]

    def clear(self):
        pass

    def reset(self):
        """
        重置算法
        清空容器数据的同时调用所有算法的function.reset()进行重置
        """
        for fun in self._functions:
            # reset不需要设置容器
            fun.set_cr(None, None)
            fun.reset()
            fun.auto_reset()

    def set_parameter(self, parameter: dict):
        """
        设置算法参数，该方法会自动从给定位置读取参数，根据gnet与gloc设置从哪一地方读取参数
        另外，亦可以自行传入参数表设置参数
        parameter格式:
        {
            fid1: {...},
            fid2: {...},
            ...
        }

        :param parameter: 算法参数，默认为None代表让方法自行读取，如果设置了数值则直接从设置的参数中读取
        """
        for fid, param in parameter.items():
            fun = self._fid_to_func[fid]
            try:
                fun.set_parameter(param or dict())
            except KeyError as e:
                raise self.wrap_exception(KeyError, f'[function: {fun.get_function_name()}] '
                                                    f'算法参数设置失败, 算法参数中无法找到参数名称: {e}.')
            except Exception as e:
                raise self.wrap_exception(Exception, f'[function: {fun.get_function_name()}] '
                                                     f'算法参数设置失败, 错误: {e}')

    def disconnect(self, nodes: List[int]):
        """
        断线通知
        """
        for node in nodes:
            self.logger_info(f'{node} 调用断线接口计算.')
            for fun in self._functions:
                fun.set_cr(None, self._api_creater.get_rapi(fun, self._crimp.get_result_by_node(node)))
                fun.disconnect()

    def _drive_data_simple(self, add_result: bool = True) -> GENERATOR_DCR:
        """
        单点位数据生成器
        负责以生成器的方式逐个返回单笔单个数据

        :param add_result: 是否每一次返回一笔特征数据就新增一笔结果数据
        :return:
        """
        container, result = self._crimp.get_cr(self._tag)
        while container.next():
            if add_result:
                result.add_result(0, container.flist.get_time())
            yield self._tag, container, result

    def _drive_data_multi(self, add_result: bool = True) -> GENERATOR_DCR:
        """
        多点位数据生成器
        负责以生成器的方式逐个按顺序一个一个的返回每一个点位的数据

        :param add_result: 是否每一次返回一笔特征数据就新增一笔结果数据
        :return:
        """
        index = 0
        ldcr = self._crimp.copy_ldcr()
        size = self.dsize
        # 多设备轮询进入算法工作组进行计算
        while 1:
            algorithm_id, container, result = ldcr[index]
            if not container.next():
                del ldcr[index]
                size -= 1

                if size == 0:
                    break

                index %= size
                continue

            index = (index + 1) % size
            if add_result:
                result.add_result(0, container.flist.get_time())

            yield algorithm_id, container, result

    def __call__(self, *args, **kwargs):
        self.compute()
        self.flush()
