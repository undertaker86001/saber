"""

@create on: 2020.12.29
"""
import warnings
from functools import partial
from datetime import timedelta, datetime
from typing import Callable, Optional

import numpy as np

from scdap import config
from scdap.logger import LoggerInterface
from scdap.util.tc import lrtime_to_hrtime, DATETIME_MIN
from scdap.data.feature_item import FeatureList, FeatureItem, DEFAULT_TEMPERATURE

DEFAULT_ARRAY = partial(np.zeros, 0, dtype=np.float)


class Container(LoggerInterface):
    """
    数据容器类
    储存所有特征数据相关的内容
    param:
        dump_error_data: bool       是否抛弃重复的时间戳, 或者说是时间比前一笔时间还早的数据
        filter_time: [int, int]     时间过滤机制
                                    根据当前的系统时间时间错误的数据
                                    systime_time: 当前系统时间
                                    假设配置为 [a, b]
                                    则过滤的时间为:
                                    [systime_time - a, systime_time + b]
                                    如果配置为0代表不过滤
                                    在debug模式下不启用
        maxlen: int                 容器数据缓存上限, 一般情况下运行时进程会自动清理数据
                                    但是在堵塞模式下则会根据是否有数据进入到result, 如果没有则会一直缓存
    """
    def interface_name(self):
        return f'container:{self._algorithm_id}'

    def __init__(self, algorithm_id: str, node_id: int,
                 index: int, systime_function: Callable[[], datetime],
                 debug: bool, **option):
        self.index = index
        self._algorithm_id = algorithm_id
        self._node_id = node_id
        self._systime_function = systime_function
        self._debug = debug
        self._option = option

        # 根据decode记录数据时间, 用于方式数据重复或者顺序错误(时间戳ms)
        self._previous_time = DATETIME_MIN

        # 记录时间，用于计算高分时间(datetime)
        self._hr_curr_time = None
        # 高分分辨率, 即每一秒内拥有多少的高分数据
        self._hf_resolution = self._get_option('hf_resolution', config.HF_RESOLUTION)
        # 用于计算高分时间戳
        self._hf_time_delta = np.arange(-self._hf_resolution, 0) * (timedelta(seconds=1 / self._hf_resolution))

        # 容器中最多可存在的数据结构数量
        self._maxlen = self._get_option('maxlen', config.CONTAINER_MAXLEN)

        self._dump_error_data = self._get_option('dump_error_data', config.DUMP_ERROR_DATA)

        # 时间过滤机制
        self._filter_time = self._get_option('filter_time', config.FILTER_DATA_TIME)

        # debug模式下不启用
        if self._debug:
            self._filter_time = [0, 0]
        # 根据当前的系统时间时间错误的数据
        # systime_time: 当前系统时间
        # 假设配置为 [a, b]
        # 则过滤的时间为:
        # [systime_time - a, systime_time + b]
        # 如果配置为0代表不过滤
        # 在debug模式下不启用
        self._early_delta = None if self._filter_time[0] <= 0 else timedelta(seconds=self._filter_time[0])
        self._later_delta = None if self._filter_time[1] <= 0 else timedelta(seconds=self._filter_time[1])

        self._has_high_reso = False

        self.flist: Optional[FeatureList] = None
        self._prev_temperature = DEFAULT_TEMPERATURE

    def __str__(self):
        return f'{self.flist.__str__()}'

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> {self.__str__()}]'

    def _get_option(self, key: str, default):
        val = self._option.get(key)
        if val is None:
            return default
        return val

    def __call__(self, *args, **kwargs) -> FeatureList:
        return self.flist

    def bind_worker(self, worker):
        """
        将容器与worker绑定, 主要是绑定一些worker的配置

        """
        from scdap.flag import column
        from scdap.frame.worker import BaseWorker
        worker: BaseWorker
        wcolumn = worker.get_column()[self.get_algorithm_id()]
        self._has_high_reso = column.has_hrtime(wcolumn)
        self.flist = FeatureList(self._algorithm_id, self._node_id, wcolumn, maxlen=self._maxlen)

    def next(self) -> bool:
        return self.flist.next()

    def reset_position(self):
        self.flist.reset_position()

    def get_device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        return self._algorithm_id

    def get_algorithm_id(self) -> str:
        """
        获取算法点位编号

        :return: 算法点位编号
        """
        return self._algorithm_id

    def get_node_id(self) -> int:
        """
        获取后端使用的nodeid编号

        :return: nodeId
        """
        return self._node_id

    def size(self):
        return self.flist.size()

    def empty(self):
        return self.flist.empty()

    def clear(self):
        self.flist.clear()

    def reset(self):
        self.clear()
        self._previous_time = DATETIME_MIN

    def append(self, feature: FeatureItem) -> int:
        # 时间戳重复/或者说是后来的时间戳时间早于前一次来的数据的时间戳
        if self._dump_error_data and self._previous_time >= feature.time:
            self.logger_warning(
                f'设备: {self.get_algorithm_id()} 数据时间错误. '
                f'数据时间为: {feature.time}, '
                f'该数据时间比前一笔数据时间:{self._previous_time}还要早.'
            )
            return 0

        systime = self._systime_function()
        # 数据时间戳超过当前系统时间过多
        if self._later_delta and feature.time > systime + self._later_delta:
            self.logger_warning(
                f'设备: {self.get_algorithm_id()} 数据时间错误. '
                f'数据时间为: {feature.time}, '
                f'该数据时间超过当前系统时间过多, 将被筛选掉.'
            )
            return 0

        # 数据时间落后当前系统时间过多
        if self._early_delta and feature.time < systime - self._early_delta:
            self.logger_warning(
                f'设备: {self.get_algorithm_id()} 数据时间错误. '
                f'数据时间为: {feature.time}, '
                f'该数据时间落后当前系统时间过多, 将被筛选掉.'
            )
            return 0

        if self._has_high_reso:
            feature.hrtime = lrtime_to_hrtime(self._hr_curr_time, feature.time, self._hf_resolution)

        self._hr_curr_time = self._previous_time = feature.time

        # 当温度<=DEFAULT_TEMPERATURE时一般代表数值不正确,
        # 可能是温度计不工作导致温度无法获取,
        # 届时将配置温度为DEFAULT_TEMPERATURE
        # 所以我们需要将温度自动延续
        # if feature.temperature <= DEFAULT_TEMPERATURE:
        #     feature.temperature = self._prev_temperature
        # self._prev_temperature = feature.temperature
        self.flist.append_item(feature)
        return 1

    def extend(self, flist: FeatureList) -> int:
        return sum(map(self.append, flist))
