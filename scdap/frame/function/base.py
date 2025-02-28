"""

@create on: 2020.01.20
"""
import warnings
from functools import update_wrapper
from abc import ABCMeta, abstractmethod
from typing import List, Iterable, Optional, Tuple, Callable, NoReturn, Dict

from scdap.util.parser import parser_id
from scdap.logger import LoggerInterface
from scdap.flag import column, format_column

from ..api import ContainerAPI, ResultAPI

TYPE_C = Optional[ContainerAPI]
TYPE_R = Optional[ResultAPI]

MD_TYPE_DC = Optional[Dict[str, ContainerAPI]]
MD_TYPE_DR = Optional[Dict[str, ResultAPI]]

TYPE_LOG_FUNCTION = Callable[[str], NoReturn]


class BaseFunction(LoggerInterface, metaclass=ABCMeta):
    """
    global_parameter: {
        "health_define":    list[str],
        "default_score":    list[int],
        "extra": {
            "health_define1": {
                "default":  int,    # 与default_score类似, 只是可以定制化的配置某一个health_define的数值
                ...
            }
        }
    }
    global_parameter中的配置优先度:
    extra中配置的数值 < 算法类重载的方法 < 参数中的配置
    """

    container: TYPE_C
    result: TYPE_R

    def __init__(self, tag: str, devices: Tuple[str], findex: int = -1, sindex: int = -1,
                 debug: bool = False, net_load_mode: str = 'http', global_parameter: dict = None):
        """
        初始化算法

        :param tag: 设备组主编号
        :param devices: 设备组编号列表
        :param findex: 该算法在整个算法工作组中所处的位置
        :param sindex: 该算法的健康度在整个算法工作组健康度列表中所处的起始位置
        :param debug: 是否是debug模式
        :param global_parameter: 全局算法配置参数, 可用于在设备配置结构中配置
                                get_health_define/get_default_score/get_health_info等内容
        """
        # 设备组标签
        self.__tag__ = tag
        # 设备组编号列表
        self.__devices__ = devices
        # 总共拥有的设备数量
        self.__dsize__ = len(devices)

        self.__is_debug__ = debug
        self.__net_load_mode__ = net_load_mode
        self.print: TYPE_LOG_FUNCTION = self.logger_seco

        self.logger = self.log = self.print

        # 算法在整个配置的算法工作组中的所有算法所在的位置，或者说是注册的顺序
        self.__findex__: int = findex
        # 全局配置参数
        # 保存在算法点位配置中
        # {
        #   "function": "function_name",
        #   "global_parameter": {
        #       "health_define": [],
        #       "default": []
        #   }
        # }
        # 其他详情可查看self._set_global_parameter(...)
        self.__global_parameter__ = global_parameter or dict()
        # 健康度数值在整个算法工作组中所处的起始位置
        # 详情查看self._sub_initial(self)中的说明
        self.__sindex__: int = sindex
        # 主要是在result-api中需要使用
        # 详细可查看scdap.frame.api.rapi
        self.__score_index__: List[int] = list()
        # 调用scdap.api.health_define接口获取的所配置的健康度详细信息
        self.__health_define_info__: Optional[Dict[str, dict]] = None
        # 健康度是否限制数值范围
        # limit = False -> [-无穷, +无穷]
        # limit = True  -> [0, 100]
        # 通过scdap.api.health_define接口进行配置
        # 所以必须在sqlapi中已经配置的健康度定义才会有该数值
        # 如果不存在则默认为 True
        # 如果需要新增请查看页面gitlab/algorithm/wiki/算法服务框架/新增flag标签配置的wiki
        self.__score_limit__: Optional[List[bool]] = None
        # 健康度的是否是数值越低越健康
        # reverse = False -> 数值越高越健康
        # reverse = True  -> 数值越低越健康
        # 与self.__score_limit__的处理机制类似
        self.__score_reverse__: Optional[List[bool]] = None

        # 实时数据容器
        # 根据算法工作组的类型可能有不同的类型
        # 具体需要通过全局的类型声明进行配置
        self.container = None
        # 计算结果结果容器
        self.result = None
        # [container, result]
        # function.timing_evaluation需要使用到该参数
        # 详细可查看scdap.frame.function.timing_evaluation内相关设计机制
        self.__cr__ = [None, None]

    @staticmethod
    def is_mdfunction() -> bool:
        """
        是否是多点位同步联动算法
        前缀带md的为多点位同步联动算法

        :return:
        """
        return False

    def set_cr(self, container, result):
        """
        传递数据容器至算法内部，由算法工作组调用
        请勿在算法内部使用
        """
        self.container = container
        self.result = result
        self.__cr__[:] = container, result

    def __call__(self, container, result):
        """
        算法调用
        如假设有一个算法: function = Function(BaseFunction): ...
        function.set_cr(container, result)
        function.compute()
        等价于
        function(container, result)

        """
        self.set_cr(container, result)
        self.compute()

    def get_global_parameter(self, key: str):
        """
        获取算法的全局参数

        :param key:
        :return:
        """
        return self.__global_parameter__.get(key)

    @property
    def tag(self) -> str:
        """
        设备组主编号/algorithm_id/tag/group_tag

        :return: 算法点位编号
        """
        return self.__tag__

    def device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        return self.__tag__

    @property
    def algorithm_id(self) -> str:
        """
        设备组主编号/algorithm_id/tag/group_tag

        :return: 算法点位编号
        """
        return self.__tag__

    @property
    def devices(self) -> Tuple[str]:
        """
        设备组配置的算法点位编号列表
        列表中包括主编号(algorithm_id/tag/group_tag)

        :return: 算法点位编号列表
        """
        return self.__devices__

    @property
    def dsize(self) -> int:
        """
        设备组配置的设备数量
        包括主编号(algorithm_id/tag/group_tag)

        :return: 设备数量
        """
        return self.__dsize__

    @property
    def debug(self) -> bool:
        """

        :return: 是否是debug模式
        """
        return self.__is_debug__

    def interface_name(self):
        return 'function:' + self.get_function_name()

    def __str__(self):
        return f'type: {self.get_function_type()}, name: {self.get_function_name()}'

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> {self.__str__()}]'

    @classmethod
    def get_function_id(cls) -> int:
        """
        通过function_name进行解析

        :return: 算法编号
        """
        return parser_id(cls.get_function_name())

    def multi_dev(self) -> bool:
        """
        主要用于与worker相关的校验, 如worker如果允许多点位算法,
        则配置了只允许单点位算法将无法通过检查会抛出错误

        :return: 是否支持多设备进程
        """
        return True

    @staticmethod
    def is_health_function() -> bool:
        """
        主要用于校验

        :return: 是否是计算健康度的算法
        """
        return False

    @abstractmethod
    def is_realtime_function(self) -> bool:
        """
        是否是实时的算法
        涉及到worker的类型检查
        如假设worker只支持实时算法, 则如果配置了非实时算法将会出错
        另外也涉及到result接口调用
        如果配置为实时算法, 意味着无法调用self.result.add_result
        如果配置为非实时算法, 则可以调用self.result.add_result

        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def get_function_type() -> str:
        """
        算法类型, 与数据库sqlapi有关, 主要用于校验算法是否配置在该配置的位置

        :return: 算法类型
        """
        pass

    @staticmethod
    @abstractmethod
    def get_function_name() -> str:
        """
        算法名称, 格式为: function_name + function_id
        必须为全小写的, 并且function_id不允许重复
        如motor24, stab26, trend25等
        必须先行去https://docs.qq.com/sheet/DU0JacmhBZW9oakV5?tab=BB08J2抢占function_id防止多人使用了同一个id

        :return: 算法名称
        """
        pass

    def get_column(self) -> List[str]:
        """
        配置算法所需的特征
        涉及到container允许调用的特征接口
        如配置为[column.meanhf, column.meanlf]
        则只允许调用下列接口
            self.container.get_algorithm_id()
            self.container.get_meanhf()
            self.container.get_meanlf()
            self.container.get_time()
            self.container.get_status()
        像是self.container.get_feature1()等接口无法调用将会报错抛出异常

        :return: 算法所需的特征
        """
        return column.normal_column

    @staticmethod
    @abstractmethod
    def get_information() -> dict:
        """
        与sqlapi中的function-define有关的配置

        :return: 获取算法的简介与信息
        """
        return {
            'author': '',
            'description': '',
            'email': '',
            'version': '',
            'modify_time': '',
        }

    @abstractmethod
    def get_health_define(self) -> List[str]:
        """
        获取健康度定义, 主要负责确认健康度的名称类型以及算法的健康度数量

        :return: 健康度定义列表
        """
        pass

    def get_health_size(self) -> int:
        return len(self.get_health_define())

    def _get_global_parameter_extra_list(self, val: str, default) -> list:
        """
        global_parameter.extra

        :param val:
        :param default:
        :return:
        """
        result = list()
        extra = self.get_global_parameter_extra()
        for hd in self.get_health_define():
            result.append(extra.get(hd, dict()).get(val, default))
        return result

    def get_global_parameter_extra(self) -> dict:
        """
        健康度配置
        一个通用的对健康度相关内容进行配置的地方
        可用于配置许多内容, 未来可能支持报警模块的配置
        "global_parameter": {
            "extra": {
                "trend": {
                    "default":  int     # 默认健康度数值
                },
                ...
            }
        }
        :return:
        """
        return self.__global_parameter__.get('extra', dict())

    def get_default_score(self) -> List[int]:
        """
        健康度数值列表的长度必须与get_health_define()相同
        默认配置为0

        :return: 默认的健康度数值
        """
        return self._get_global_parameter_extra_list('default', 0)

    def _load_health_define_info(self):
        """
        从scdap.api.health_define中载入健康度的详细信息

        :return:
        """
        if self.__health_define_info__ is not None:
            return

        self.__health_define_info__ = dict()

        from scdap.api import health_define
        for hd in self.get_health_define():
            try:
                hd_info = health_define.get_health_define(hd, self.__net_load_mode__)
            except Exception as e:
                self.logger_warning(f'获取健康度信息的接口调用失败: {e}')
                hd_info = dict()
            if hd_info is None:
                self.logger_warning(f'请在scdap_algorithm.flag中配置新的健康度定义配置:{hd}'
                                    f'后提交并完成流水线再使用该健康度定义.')
                hd_info = dict()
            self.__health_define_info__[hd] = hd_info

    def _get_health_info(self, info_list_name: str, key: str, default, typed):
        """
        根据从数据库或接口载入的健康度信息, 解析需要使用的参数key后保存在info_list_name中

        :param info_list_name:
        :param key:
        :param default:
        :param typed:
        :return:
        """
        self._load_health_define_info()

        info_list = getattr(self, info_list_name)
        if info_list is not None:
            return
        info_list = list()
        setattr(self, info_list_name, info_list)
        for hd in self.get_health_define():
            info_list.append(typed(self.__health_define_info__[hd].get(key, default)))

    def get_score_limit(self) -> List[bool]:
        """
        健康度数值限制
        长度必须与get_health_define()相同
        True    -> [0, 100]
        False   -> [-无穷, +无穷]

        :return:
        """
        self._get_health_info('__score_limit__', 'limit', True, bool)
        return self.__score_limit__.copy()

    def get_score_reverse(self) -> List[bool]:
        """
        reverse = True  -> 健康度数值越低代表越健康
        reverse = False -> 健康度数值越高代表越健康

        :return:
        """
        self._get_health_info('__score_reverse__', 'reverse', True, bool)
        return self.__score_reverse__.copy()

    def get_health_info(self) -> List[dict]:
        """
        健康度阈值信息, 包括维护建议
        一般只在本地环境中使用, 如韶关钢铁
        具体的维护建议数值通过接口api.recommendation获取编号
        格式为:
        [{
            "threshold": [t1(int), t2(int)],
            "recommendation": [r1(int), r2(int), r3(int)],
            "cn_name": "...",
            "en_name: "...",
        }]
        列表内字典数量必须与health_define数量相等
        维护建议区间:   r1    ||    r2    ||     r3
        阈值线:              t1          t2
        即:
        score > t1          -> r1
        t1 >= score > t2    -> r2
        t2 >= score         -> r3

        :return: 算法健康度信息
        """
        threshold = self._get_global_parameter_extra_list('threshold', [75, 50])
        recommendation = self._get_global_parameter_extra_list('recommendation', [0, 1, 2])
        cn_name = self._get_global_parameter_extra_list('cn_name', None)
        en_name = self._get_global_parameter_extra_list('en_name', None)
        return [
            {
                'threshold': threshold[i],
                'recommendation': recommendation[i],
                'cn_name': cn_name[i] or hd,
                'en_name': en_name[i] or hd
            } for i, hd in enumerate(self.get_health_define())
        ]

    def _sub_initial(self):
        if self.is_health_function():
            # 完善 get_health_info() 的内容
            health_info = [{
                "function_id": self.get_function_id(),
                "health_define": hd,
                'threshold': info['threshold'],
                'recommendation': info['recommendation'],
                'cn_name': info.get('cn_name') or hd,
                'en_name': info.get('en_name') or hd
            }
                for info, hd in zip(self.get_health_info(), self.get_health_define())
            ]
            _function_wrapper(dict(), self, 'health_info', 'get_health_info', health_info)

            # 算法内会计算的健康度在整个算法工作组中所有健康度算法中的位置
            # 如有一个算法工作组拥有3个评价算法, 则下列算法中__score_index__对应的关系为
            # e1: ['trend', 'stab']     -> [0, 1]
            # e2: ['error', 'blockage'] -> [2, 3]
            # e3: ['health']            -> [4]
            # 总的算法健康度列表: ['trend', 'stab', 'error', 'blockage', 'health']
            #                    |       e1      |        e2         |     e3
            # 该参数用于在调用result.set_score/get_score/set_total_score/get_total_score/...等配置与读取健康度数值的时候使用
            # 该参数只适用于评价算法(会计算健康度的算法)
            self.__score_index__: Tuple[int] = tuple(range(self.__sindex__, self.__sindex__ + self.get_health_size()))

    def initial(self):
        # 通过self.__global_parameter__配置接口
        # 以实现通过配置参数的方式也能够定制化算法的部分接口细节
        self._set_global_parameter(self.__global_parameter__)
        # 初始化, 分开的原因主要是可以让子类自行定制_sub_initial(self)
        self._sub_initial()
        # 检查接口实现是否符合规范
        self._check_self()

    @abstractmethod
    def compute(self):
        """
        算法具体实现
        请算法继承该类后通过实现该方法来实现的具体细节
        worker.compute():
        ->function.set_cr(container, result)
        ->function.compute()
        """
        pass

    @abstractmethod
    def set_parameter(self, parameter: dict):
        """
        设置参数,根据算法参数保存方式自行设计与获取对应参数
        self.container = None
        self.result = None
        worker.set_parameter():
        ->functin.set_parameter(param)

        :param parameter: 参数表
        """
        pass

    def _generator_check_list_type(self, function, types):
        for i, val in enumerate(function()):
            if not isinstance(val, types):
                raise self.wrap_exception(TypeError, f'配置方法{function.__name__}()配置的类型必须为({types}).')
            yield i, val

    def _check_self(self):
        # 多设备进程必须使用多设备算法基类
        if self.dsize > 1 and not self.multi_dev():
            raise self.wrap_exception(Exception, f'只支持单设备.')

        # 算法相关的定义检查
        _check_list(self.wrap_exception, self.get_health_define, self.get_health_size())
        _check_list(self.wrap_exception, self.get_default_score, self.get_health_size())
        _check_list(self.wrap_exception, self.get_health_info, self.get_health_size())
        # _check_list(self.wrap_exception, self.get_score_limit, self.get_health_size())

        # 所有column必须是正确的特征名称
        # 既配置的特征名称必须通过scdap.flag.column进行配置
        for i, col in self._generator_check_list_type(self.get_column, str):
            if not column.has_column(col):
                raise self.wrap_exception(ValueError, f"column: [{col}]不存在.")

        col = set(format_column(self.get_column()))

        _function_wrapper({}, self, 'column', 'get_column', list(col))

        # 健康度数值限制类型检查
        # list(self._generator_check_list_type(self.get_score_limit, bool))

        # 健康度默认数值范围与类型检查
        # for i, val in self._generator_check_list_type(self.get_default_score, int):
        #     if self.get_score_limit()[i] and val > 100 or val < 0:
        #         raise self.wrap_exception(
        #             ValueError,
        #             f'配置默认健康度的方法get_default_score()返回的结果的数值取值范围必须为[0, 100].'
        #         )

        # 信息配置接口检查
        information = self.get_information()
        if not isinstance(information, dict):
            raise self.wrap_exception(TypeError, f'请确保get_information()范围的类型为dict.')

        for key, types, must_has in [['author', str, True], ['version', str, True],
                                     ['email', str, False], ['description', str, False]]:
            if key not in information:
                if must_has:
                    raise self.wrap_exception(KeyError, f'请配置好get_information()内关键字: {key}的相关信息.')
            else:
                if not isinstance(information[key], types):
                    raise self.wrap_exception(TypeError, f'get_information()内关键字: {key}的类型必须为:{types}')

    def _set_global_parameter(self, global_parameter: dict = None):
        """
        算法自用的参数配置, 用于配置
        假设一个算法定义了如下方法:
        class Function(BaseFunction):
            ...
            def get_health_define(self): return ['error']
            def get_default_score(self): return [10]

        假设global_parameter配置了相应的字段如:
        "global_parameter": {
            "extra": {
                "trend": {
                    "default": 85
                }
            },
            "health_define": ["trend", "stab"],
            "default_score": [90, 90]
        }

        那么算法Function的两个方法将被替换为:
        class Function(BaseFunction):
            ...
            def get_health_define(self): return ["trend", "stab"]
            def get_default_score(self): return [90, 90]

        global_parameter就是为了实现该效果
        既可以通过配置点位配置的方式, 来动态的定义算法的相关健康度配置
        优先度为: global_parameter.extra < 类中实现的方法 < global_parameter配置的参数
        """
        if global_parameter is None or not global_parameter:
            return

        _function_wrapper(global_parameter, self, 'column', 'get_column')

        if self.is_health_function():
            _function_wrapper(global_parameter, self, 'health_define', 'get_health_define')
            _function_wrapper(global_parameter, self, 'default_score', 'get_default_score')
            _function_wrapper(global_parameter, self, 'health_info', 'get_health_info')
            # _parameter_wrapper(global_parameter, self, 'health_define_limit', 'get_health_define_limit')
            # _parameter_wrapper(global_parameter, self, 'score_limit', 'get_score_limit')

    def auto_reset(self):
        """
        前置的重置算法
        """
        pass

    @abstractmethod
    def reset(self):
        """
        重置算法
        worker.reset():
        ->function.set_cr(None, None)
        ->function.reset()
        ->functin.auto_reset()
        """
        pass

    def disconnect(self):
        """
        断线通知
        self.container = None
        worker.disconnect():
        ->function.set_cr(None, result)
        ->function.disconnect(param)
        result可以使用add_result(...)
        """
        pass


def _check_list(wrap_exception, f, size: int = None):
    """
    列表校验

    :param wrap_exception:
    :param f:
    :param size:
    :return:
    """
    if not isinstance(f(), Iterable):
        raise wrap_exception(
            TypeError,
            f'配置健康度定义的方法{f.__name__}()返回的结果必须为必须为列表(list/tuple).'
        )
    if size is not None and size != len(f()):
        raise wrap_exception(
            ValueError,
            f'配置默认健康度的数值数量必须与配置的健康度名称{f.__name__}()数量相同.'
        )


def _function_wrapper(parameter: dict, function_obj: BaseFunction, key_name: str, wrap_api_name: str, default=None):
    """
    该方法主要是用于替换算法类内的部分获取算法配置的接口
    通过该方法将允许部分算法类的配置能够在参数内进行配置
    如default_score/score_info/health_define等都能够在算法参数内进行配置
    则假设想要在参数中配置default_score，以替换掉算法类静态配置的接口 function.get_default_score(): return [100, 100, ...]
    则可在算法参数中增加字段: {..., "default_score": [96, 97, ...], ...}
    在调用完function.prev_set_parameter后变为function.get_default_score(): return [96, 97, ...]
    """
    val = parameter.get(key_name, default)
    if val is not None:
        setattr(function_obj, wrap_api_name, update_wrapper(lambda: val, getattr(function_obj, wrap_api_name)))
