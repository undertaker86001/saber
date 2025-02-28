"""

@create on: 2020.12.30
"""
import warnings
from functools import partial
from datetime import datetime
from typing import List, Callable, Union

import numpy as np

from scdap import config
from scdap.logger import LoggerInterface
from scdap.flag import event_type, QualityInspectionItem

from .result_item import ResultList, Event

DEFAULT_ARRAY: Callable[[List[int]], np.ndarray] = partial(np.zeros, 0, dtype=np.float)
DEFAULT_SCORE_ARRAY: Callable[[List[int]], np.ndarray] = partial(np.array, dtype=np.int)


class Result(LoggerInterface):
    """
    maxlen: int         结果容器的最大容量
    """

    def interface_name(self):
        return f'result:{self._algorithm_id}'

    def __init__(self, algorithm_id: str, node_id: int,
                 index: int, systime_function: Callable[[], datetime],
                 debug: bool, **option):

        self.index = index
        self._algorithm_id = algorithm_id
        self._node_id = node_id
        # 一个方法用于获得系统时间
        # 之所以这个搞是为了配置设计者模式的时间
        # 在设计者模式下, 系统时间应该是数据时间而非当前的系统时间
        self._systime_function = systime_function
        self._debug = debug
        self._option = option

        self._maxlen = self._get_option('maxlen', config.RESULT_MAXLEN)

        if self._debug:
            self._maxlen = None

        # 容器中最多可存在的数据结构数量
        self._prev_score: List[int] = list()
        self._health_size: int = 0
        self._health_define: List[str] = list()

        self._score_limit: List[bool] = list()

        self.rlist = ResultList(self._algorithm_id, self._node_id, maxlen=self._maxlen)
        self._flush_index = set()

    def _get_option(self, key: str, default):
        val = self._option.get(key)
        if val is None:
            return default
        return val

    def __str__(self):
        return f'{self.rlist.__str__()}'

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> {self.__str__()}]'

    def __call__(self, *args, **kwargs) -> ResultList:
        return self.rlist

    def next(self) -> bool:
        return self.rlist.next()

    def reset_position(self):
        self.rlist.reset_position()

    def size(self) -> int:
        return self.rlist.size()

    def empty(self) -> bool:
        return self.rlist.empty()

    def clear(self):
        self.rlist.clear()
        self._flush_index.clear()

    def reset(self):
        self.clear()
        self._flush_index.clear()

    def get_algorithm_id(self) -> str:
        return self._algorithm_id

    def get_device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        return self._algorithm_id

    def get_node_id(self) -> int:
        return self._node_id

    def get_health_define(self) -> List[str]:
        return self._health_define

    def flush(self):
        """
        收尾工作, 每一次调用完一轮数据后需要进行收尾工作
        更新与刷入最终的结果
        """
        for index in self._flush_index:
            obj = self.rlist.get_ref(index)
            for o in obj.event:
                # 在最终编码的时候配置最终的实现相关数值
                o.status = obj.status
                o.score = dict(zip(obj.health_define, obj.score))
        self._flush_index.clear()

    def bind_worker(self, worker):
        """
        设置健康度相关的参数
        由调用result初始化的一方在worker初始化完成后调用

        """
        from scdap.frame.worker import BaseWorker
        worker: BaseWorker
        self._health_define = worker.get_health_define()[self.get_algorithm_id()]
        self._prev_score = worker.get_default_score()[self.get_algorithm_id()]
        self._score_limit = worker.get_score_limit()[self.get_algorithm_id()]
        self._health_size: int = len(self._health_define)

    def set_score(self, score_index: int = None, score: int = None):
        """
        设置健康度数值
        """
        if not isinstance(score, int):
            raise ValueError('设置了错误的健康度数值类型.')

        if self._score_limit[score_index]:
            if score == 0:
                score = int(self._prev_score[score_index])
            elif score > 100 or score < 0:
                raise ValueError(f'设置了错误的健康度得分: {score}, '
                                 f'请确保健康度得分处在范围[0, 100]中, 或者通过配置算法参数limit = false.')

        self.rlist.set_simple_score(score_index, score)
        self._prev_score[score_index] = score

    def get_score(self, score_index: int = None) -> int:
        """
        获取指定位置的健康度
        """
        return self.rlist.get_simple_score(score_index)

    def add_result(self, status: int, time: datetime, *score):
        score = list(score) if score else self._prev_score.copy()
        self.rlist.append_dict(status=status, time=time,
                               health_define=self._health_define.copy(),
                               score=score)

        self.rlist.next()

    def add_result_without_score(self, status: int, time: datetime, *args, **kwargs):
        if args or kwargs:
            raise TypeError('禁止在调用该接口时配置健康度相关的内容.')
        self.add_result(status, time)

    def get_prev_score(self, score_index: int) -> int:
        return int(self._prev_score[score_index])

    def set_score_force(self, score_index: int = None, score: int = 0):
        """
        因为健康度遇见0延续的特性, 在某些情景加需要重置健康度为0可能就无法实现
        所以在此新增一个接口用于强制设置某一个数值的健康度

        :param score_index:
        :param score:
        :return:
        """
        if not isinstance(score, int):
            raise ValueError('设置了错误的健康度数值类型.')

        if self._score_limit[score_index] and (score > 100 or score < 0):
            raise ValueError(f'设置了错误的健康度得分: {score}, '
                             f'请确保健康度得分处在范围[0, 100]中, 或者通过配置算法参数limit = false.')

        self.rlist.set_simple_score(score_index, score)
        self._prev_score[score_index] = score

    def _check_event_args(self, key: str, val, types, can_none: bool = True):
        if can_none and val is None:
            return
        if not isinstance(val, types):
            raise ValueError(f'在添加事件时传入错误的参数{key}, 类型必须为: {types}')

    def _add_event(self, etype: int, name: str = '',
                   start: datetime = None, stop: datetime = None,
                   message: str = '', code: int = 0,
                   check_result: int = 0, detail: str = '', extend: dict = None):

        if not event_type.has_val(etype):
            raise ValueError('配置了错误的event_type, 请确保配置的事件必须源自event_type.')

        self._check_event_args('name', name, str, False)
        self._check_event_args('start', start, datetime, True)
        self._check_event_args('stop', stop, datetime, True)
        self._check_event_args('message', message, str, False)
        self._check_event_args('code', code, int, False)
        self._check_event_args('check_result', check_result, int, False)
        self._check_event_args('detail', detail, str, False)
        self._check_event_args('extend', extend, dict, True)

        # time必须存在
        event = Event(
            etype,
            self._algorithm_id,
            self._node_id,
            # 这里只是暂时缓存了数据
            # 实际在编码准备发送数据的时候会配置最终的的分数数值
            self.rlist.get_status(),
            # 这里只是暂时缓存了数据
            # 实际在编码准备发送数据的时候会配置最终的的分数数值
            dict(zip(self._health_define, self.rlist.get_score())),
            name,
            self._systime_function(),
            start,
            stop,
            message,
            code,
            check_result,
            detail,
            extend
        )

        self.rlist.set_event(event)
        # 如上面注释说的, 需要在最终时段确认最终的状态
        # 因为无法确定什么时候需要在结束时的由调用方刷入最终的状态
        self._flush_index.add(self.rlist.get_position())

        event_name = event_type.get_itemname_by_val(etype)
        self.logger_info(f'[触发事件: {event_name}]: [{event}]')

    # --------------------------------------------------------------------
    # 事件接口
    # 有新的事件接口请往下增加
    # 并且请至scdap.flag.event_type中按编号顺序递增一个新的编号
    # 另外需要至scdap.frame.api.rapi.ResultAPI中新增接口的说明
    # 注意请按照格式添加接口： def add_xxxxxx_event(self, ....)
    # 每新增一个接口请标注好数据交互的说明，方便后人查看
    # --------------------------------------------------------------------

    def add_alarm_event(self, *, health_define: str, start: datetime = None,
                        stop: datetime = None, message: str = ''):
        """
        添加报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 0
        name: 指定的需要报警的健康度名称
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        message: 备注
        报警事件将通过配置的name中保存的health_define, 从score中获取所需的数值
        如: name = ["trend"], score = {"trend": 99, "error": 1}

        则其会获取trend的健康度作为报警事件入库

        :param health_define: 报警事件所代表的健康度名称(health_define), **必填**
        :param start: 事件起始事件, 可以不存在
        :param stop: 事件结束时间, 可以不存在
        :param message: 事件信息
        """
        if health_define not in self._health_define:
            raise ValueError(f'调用add_alarm_event()时name必须为算法配置的健康度名称: {self._health_define}.')
        self._add_event(etype=event_type.alarm, name=health_define, start=start, stop=stop, message=message)

    def add_period_event(self, *, health_define: Union[str, List[str]],
                         start: datetime, stop: datetime, message: str = ''):
        """
        添加周期性事件
        使用到的全部参数(指传输给后端会使用的字段):
        type: 1
        name: 指定的周期性健康度名称, 如果该周期存在多共个周期性健康度分数, 则使用(,)隔开
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        message: 备注
        周期性事件将通过配置的name中保存的health_define, 从score中获取所需的数值
        如: name = ["trend"], score = {"trend": 99, "error": 1}
        则其会获取trend的健康度作为周期性事件入库

        :param health_define: 报警事件所代表的健康度名称(health_define), 如果周期内存在多个分数, 则传入一个列表 **必填**
        :param start: 事件起始事件, **必填**
        :param stop: 事件结束时间, **必填**
        :param message: 事件信息
        """

        if isinstance(health_define, str):
            health_defines = [health_define]
        else:
            health_defines = health_define

        for health_define in health_defines:
            if health_define not in self._health_define:
                raise ValueError(f'调用add_alarm_event()时name必须为算法配置的健康度名称: {self._health_define}.')
        health_defines = ','.join(health_defines)
        self._add_event(etype=event_type.period, name=health_defines, start=start, stop=stop, message=message)

    def add_part_event(self, name: str, start: datetime,
                       stop: datetime, message: str = ''):
        """
        添加加工周期事件(或者是一次质检结果(OK/NG))
        使用到的全部参数(指传输给后端会使用的字段):
        type: 2
        name: 加工零件的名称
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        message: 备注

        :param name: 加工零件名称, **必填**
        :param start: 事件起始事件, **必填**
        :param stop: 事件结束时间, **必填**
        :param message: 事件信息
        """
        self._add_event(etype=event_type.part, name=name, start=start, stop=stop, message=message)

    # 因为后端还未进行修改，如果需要优化可以如下面这么整
    # 主要是考虑到可能某一个加工零件需要入库健康度分数
    # 旧版本的接口不提供加工零件的健康度
    # def add_part_event(self, *, health_define: Union[str, List[str]], part_name: str, start: datetime,
    #                    stop: datetime, message: str = ''):
    #     """
    #     添加工事件(后端未修改, 在这里提醒一下
    #     使用到的全部参数(指传输给后端会使用的字段):
    #     type: 2
    #     name: 指定的加工结束后需要使用的健康度名称, 如果该周期存在多共个周期性健康度分数, 则使用(,)隔开
    #     start: 事件起始时间
    #     stop: 事件结束时间
    #     score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
    #     detail: 加工零件的名称
    #     message: 备注
    #
    #     :param health_define: 加工事件所代表的健康度名称(health_define), 如果周期内存在多个分数, 则传入一个列表 **必填**
    #     :param part_name: 加工零件名称, **必填**
    #     :param start: 事件起始事件, **必填**
    #     :param stop: 事件结束时间, **必填**
    #     :param message: 事件信息
    #     """
    #
    #     if isinstance(health_define, str):
    #         health_defines = [health_define]
    #     else:
    #         health_defines = health_define
    #
    #     for health_define in health_defines:
    #         if health_define not in self._health_define:
    #             raise ValueError(f'调用add_alarm_event()时name必须为算法配置的健康度名称: {self._health_define}.')
    #     health_defines = ','.join(health_defines)
    #
    #     self._add_event(etype=event_type.part, name=health_defines,
    #                     detail=part_name, start=start, stop=stop, message=message)

    def add_quality_inspection(self, *, qi: QualityInspectionItem, detail: str, start: datetime,
                               stop: datetime, message: str = ''):
        """
        添加质检结果
        使用到的全部参数(指传输给后端会使用的字段):
        type: 3
        name: 质检结果中文名称, 按道理不应该传，但是后端处理起来方便一些，所以就传了
        code: 具体的质检事件类型分类编号，具体可以查看 scdap.flag.QualityInspectionItem
        check_result: 之间类型的大类分类编号，具体可以查看 scdap.flag.QualityInspectionItem
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        detail: 质检设备型号
        message: 备注，可以在前端显示一条信息展示出来

        :param detail: 质检设备型号
        :param qi: 质检结果类型, **必填**
        :param start: 质检事件起始时间, **必填**
        :param stop: 质检事件结束事件, **必填**
        :param message: 其他信息，可以在前端显示一条信息展示出来
        :return:
        """
        if not isinstance(qi, QualityInspectionItem):
            raise TypeError('qi配置了错误的类型, 请确保qi传入的数值是QualityInspectionItem.')
        self._add_event(etype=event_type.quality_inspection, start=start, stop=stop, name=qi.cn_name,
                        code=qi.code, check_result=qi.check_result, message=message, detail=detail)

    def add_operation_start(self, *, start: datetime, message: str = ''):
        """
        抛出一次操作的起始时间事件
        主要是用于通知前后端事件的开始
        方便前后端进行相关数据的显示

        使用到的全部参数(指传输给后端会使用的字段):
        type: 4
        start: 事件起始时间
        message: 备注

        :param start: 操作起始时间, **必填**
        :param message: 其他信息
        :return:
        """
        self._add_event(etype=event_type.operation_start, start=start, message=message)

    def add_operation_stop(self, *, stop: datetime, message: str = ''):
        """
        抛出一次操作的结束时间事件
        主要是用于通知前后端事件的开始
        方便前后端进行相关数据的显示

        使用到的全部参数(指传输给后端会使用的字段):
        type: 5
        stop: 事件结束时间
        message: 备注

        :param stop: 操作结束时间, **必填**
        :param message: 其他信息
        :return:
        """
        self._add_event(etype=event_type.operation_stop, stop=stop, message=message)

    def add_extend_event(self, *, extend: str, message: str = ''):
        """
        以事件的形式传输传感器数据至后端

        使用到的全部参数(指传输给后端会使用的字段):
        type: 6
        name: 需要到界面显示的某一些信息, 主要是在质检中使用
        message: 备注

        :param extend: 传感器数据, **必填**
        :param message: 其他信息
        :return:
        """
        self._add_event(etype=event_type.extend_event, name=extend, message=message)

    def add_show_message_event(self, *, message: str):
        """
        展示某些算法给出的提示信息
        没有任何实际效果
        只是用来向界面的查看人员展示信息而已

        使用到的全部参数(指传输给后端会使用的字段):
        type: 7
        message: 需要到界面显示的某一些信息, 会以弹窗的形式显示出来

        :param message: 需要到界面显示的某一些信息, 会以弹窗的形式显示出来
        :return:
        """
        self._add_event(etype=event_type.show_message_event, message=message)

    def add_status_alarm_event(self, *, alarm_status: str, start: datetime = None, message: str = ''):
        """
        添加状态报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 8
        name: 指定的需要报警的状态名称
        start: 事件起始时间
        message: 备注

        :param alarm_status: 报警事件所代表的状态名称, **必填**
        :param start: 事件起始时间, 可以不存在
        :param message: 事件信息
        """
        self._add_event(name=alarm_status, etype=event_type.status_alarm, start=start, message=message)

    def add_integrate_alarm_event(self, *, start: datetime = None, stop: datetime = None, message: str = ''):
        """
        添加综合报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 9
        start: 事件起始时间
        message: 备注

        :param start: 事件起始时间, 可以不存在
        :param stop: 事件结束时间，可以不存在
        :param message: 事件信息
        """
        self._add_event(etype=event_type.integrate_alarm, start=start, stop=stop, message=message)

    def add_features_alarm_event(self, *, start: datetime = None, stop: datetime = None,
                                 message: str = ''):
        """
        添加特征值报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 10
        start: 事件起始时间
        message: 备注

        :param start: 事件起始时间, 可以不存在
        :param stop: 事件结束时间，可以不存在
        :param message: 事件信息
        """
        self._add_event(etype=event_type.features_alarm, start=start, stop=stop, message=message)
