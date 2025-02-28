"""

@create on: 2021.01.13
"""
__all__ = ['TimingEvaluation']

from typing import List, Union
from abc import ABCMeta, abstractmethod
from datetime import datetime

from scdap.flag import column
from scdap.data import IFeature
from scdap.util.tc import get_next_time

from .base import _function_wrapper
from .evaluation import BaseEvaluation


class ResultStack(object):
    def __init__(self):
        self.size: int = 0
        self.status: List[int] = list()
        self.time: List[datetime] = list()


class TimingEvaluation(BaseEvaluation, metaclass=ABCMeta):
    """
    评价算法
    固定时间固定间隔计算健康度
    不允许多设备

    数据将根据self.get_column()的配置确定需要累计的特征，状态与时间将自行累积
    self.get_column(): return [column.meanhf, column.meanlf]
    则可调用:
        self.container_stack.time: list
        self.container_stack.status: list
        self.container_stack.meanhf: list
        self.container_stack.meanlf: list
        self.stack_feauture.size: int
        ...
        self.stack_status: list
    对健康度进行计算
    在该类中无需实现compute()方法
    只需实现analysis()方法，并回传健康度数值
    例如:
    analysis_second: 600
    则代表每10分钟计算一次健康度
    计算的时间为 0m/10m/20m/30m/40m/50m/
    """

    def get_analysis_second(self) -> int:
        """
        健康度分析的间隔, 单位为s

        :return: 间隔
        """
        return 600

    def multi_dev(self) -> bool:
        return False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__next_analysis_time__ = None
        # 根据self.column设置需要保存特征的容器与获取特征的方法
        # [list, cr, cr_api_name]
        # 例子: self.column = ['meanHf', 'mean']
        # [self.container_stack.mean, 0(container) ,'get_mean']
        # [self.container_stack.time, 1(result), 'get_time']
        # [self.stack_status, 1(result), 'get_status']
        # [self.container_stack.meanhf, 0(container), 'get_meanhf']
        # ...
        self.__need_stack__ = list()
        # 缓存的特征
        self.container_stack = IFeature()
        # 缓存的结果
        self.result_stack = ResultStack()
        # 缓存的数据数量
        self.stack_size = 0

    def initial(self):
        super().initial()

        # 添加高分时间获取
        col = list(self.get_column())

        if column.has_hrtime(col):
            col.append(column.hrtime)

        if column.time not in col:
            col.append(column.time)

        # 状态缓存
        self.__need_stack__.append((self.result_stack.status, 1, 'get_status'))
        # 时间缓存
        self.__need_stack__.append((self.result_stack.time, 1, 'get_time'))
        # 设置累积容器以及container对应的获取数据方法
        # 将container_stack中的对应特征xxx的list绑定到某一个result.get_xxx方法中
        # 在运行的过程中算法将调用result.get_xxx并将结果保存到对应的list中
        # 如get_column() -> [meanhf, meanlf]
        # 则保存为: [[container_stack.meanhf, container_stack.meanlf]]
        for c in col:
            self.__need_stack__.append((getattr(self.container_stack, c), 0, f'get_{c}'))

    def _set_global_parameter(self, global_parameter: dict = None):
        super()._set_global_parameter(global_parameter)
        _function_wrapper(global_parameter, self, 'analysis_second', 'get_analysis_second')

    def _check_self(self):
        super()._check_self()

        if not isinstance(self.get_analysis_second(), int):
            raise self.wrap_exception(
                TypeError, f'算法: [{self.get_function_name()}]配置的analysis_second必须为整型变量int.'
            )

        if self.get_analysis_second() <= 0:
            raise self.wrap_exception(
                ValueError, f'算法: [{self.get_function_name()}]配置的analysis_second必须为大于0.'
            )

    def compute(self):
        """
        计算基础方法，worker将调用该方法进行计算
        在该类评价算法下，除非特殊需求将不在需要实现该方法
        该方法的调用顺序为：
        ->self.before_compute()
        ->self.on_compute()
            ->...
            ->if need_compute:
            ->    self.result.set_score(self.analysis())
            ->    ...
            ->    self.clear()
        ->self.after_compute()
        ->self.stack_data()
        ->self.after_stack()
        """
        self.before_compute()
        self.on_compute()
        self.after_compute()
        self.stack_data()
        self.after_stack()

    def stack_data(self):
        """
        数据累计，算法类将自行根据self.column对数据进行累计
        累计的数据将放置于对应的与column内名称相同的类变量中
        当然时间与状态也将自行累计
        如：
        self.column = [column.meanhf, column.meanlf]
        则可调用:
            self.result_stack.time: list
            self.result_stack.status: list
            self.container_stack.time: list
            self.container_stack.meanhf: list
            self.container_stack.meanlf: list
            self.stack_size: int
            ...
        对健康度进行计算
        """
        # 从容器中获取数据并累积数据
        for stack_list, cr_index, api_name in self.__need_stack__:
            stack_list.append(getattr(self.__cr__[cr_index], api_name)())
        self.stack_size += 1

    def auto_reset(self):
        self.clear_data()

    def clear_data(self):
        for stack_list, *_ in self.__need_stack__:
            stack_list.clear()
        self.stack_size = 0

    def on_compute(self):
        if self.__next_analysis_time__ is None:
            self.__next_analysis_time__ = get_next_time(self.result.get_time(), self.get_analysis_second())

        # 到达计算健康度的时间点，计算健康度
        if self.result.get_time() < self.__next_analysis_time__:
            return

        if self.stack_size > 0:
            health = self.analysis()

            if isinstance(health, int):
                self.result.set_total_score(health)
            elif isinstance(health, (list, tuple)):
                self.result.set_total_score(*health)
            else:
                raise TypeError('analysis()返回了错误的数据类型.')
            self.clear_data()

        prev_time = self.__next_analysis_time__
        self.__next_analysis_time__ = get_next_time(self.result.get_time(), self.get_analysis_second())
        self.print(
            f'{self.get_function_name()}: '
            f'[algorithm_id: {self.container.get_algorithm_id()}] '
            f'[time: {prev_time}] '
            f'[score: {self.get_health_define()}: {self.result.get_total_score()}] '
            f'[next: {self.__next_analysis_time__}]'
        )
    
    @abstractmethod
    def analysis(self) -> Union[List[int], int]:
        """
        当需要计算健康度时，算法类将调用该方法进行计算
        故该方法必须实现健康度的计算公式于方法
        ->self.on_compute()
            ->...
            ->if need_compute:
            ->    self.result.set_total_score(*self.analysis())
            ->    ...
            ->    self.clear()
        """
        pass

    def before_compute(self):
        pass

    def after_compute(self):
        pass

    def after_stack(self):
        pass
