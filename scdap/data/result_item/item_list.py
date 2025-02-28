"""

@create on: 2020.12.29
"""
import warnings
from datetime import datetime
from typing import List, Optional, Dict

from scdap.flag import event_type

from .event import Event
from .stat_item import StatItem
from .item import ResultItem, __result_key__

from ..base import ItemList, ICollection


class IResult(ICollection):
    __slots__ = __result_key__

    status: List[int]
    # 时间戳
    time: List[datetime]
    # 健康度, 可以是多个健康度
    score: List[List[int]]
    health_define: List[List[str]]
    # 事件
    # {
    #   type(int): [...],
    #   type(int): [...],
    #   ...
    # }
    event: List[Dict[int, List[Event]]]
    # 统计结果
    stat_item: List[StatItem]


class ResultList(ItemList[ResultItem]):
    """
    结果数据结构集合
    """
    __slots__ = ['algorithm_id', 'node_id']

    def __init__(self, algorithm_id: str = '', node_id: int = 0, maxlen: int = None):
        self.algorithm_id = algorithm_id
        self.node_id = node_id
        ItemList.__init__(self, ResultItem, IResult(IResult.__slots__), maxlen, IResult.__slots__)

    def __str__(self) -> str:
        return f'{type(self).__name__}: algorithm_id={self.algorithm_id}, node_id={self.node_id}, size={len(self)}'

    def sub_itemlist(self, start: int = None, stop: int = None):
        start = start if start is not None else 0
        stop = stop if stop is not None else self.size() - 1
        sub_itemlist = type(self)(self.algorithm_id, self.node_id, self._maxlen)
        cache = dict()
        for key in self._select_keys:
            cache[key] = getattr(self._item_list, key)[start:stop]
        sub_itemlist.extend_ldict(**cache)
        return sub_itemlist

    def get_algorithm_id(self) -> str:
        return self.algorithm_id

    def get_device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        return self.algorithm_id

    def get_node_id(self) -> int:
        return self.node_id

    def set_value(self, name: str, val, index: int = None):
        index = self._position if index is None else index
        getattr(self._item_list, name)[index] = val

    def get_time(self, index: int = None) -> datetime:
        """
        获取数据时间

        :param index:
        :return:
        """
        return self.get_value('time', index)

    def set_time(self, value: datetime, index: int = None):
        """
        设置数据时间

        :param value:
        :param index:
        """
        self.set_value('time', value, index)

    def get_all_time(self, start: int = None, stop: int = None) -> List[datetime]:
        """
        获取所有数据时间

        :param start:
        :param stop:
        :return:
        """
        return self.get_range('time', start, stop)

    def get_simple_score(self, score_index: int, index: int = None) -> int:
        """
        获取指定位置的健康度数值

        :param score_index:
        :param index:
        :return:
        """
        return self.get_score(index)[score_index]

    def get_score(self, index: int = None) -> List[int]:
        """
        获取健康度数据列表

        :param index:
        :return:
        """
        return self.get_value('score', index)

    def set_score(self, value: List[int], index: int = None):
        """
        设置健康度

        :param value:
        :param index:
        :return:
        """
        self.set_value('score', value, index)

    def set_simple_score(self, score_index: int, val: int, index: int = None):
        """
        设置指定位置的健康度数值

        :param score_index:
        :param val:
        :param index:
        :return:
        """
        self.get_score(index)[score_index] = val

    def get_all_score(self, start: int = None, stop: int = None) -> List[List[int]]:
        """
        获取所有健康度数据

        :param start:
        :param stop:
        :return:
        """
        return self.get_range('score', start, stop)

    def get_health_define(self, index: int = None) -> List[int]:
        """
        获取健康度定义名称列表

        :param index:
        :return:
        """
        return self.get_value('health_define', index)

    def set_health_define(self, value: List[int], index: int = None):
        """
        设置健康度定义列表

        :param value:
        :param index:
        :return:
        """
        self.set_value('health_define', value, index)

    def set_simple_health_define(self, score_index: int, val: int, index: int = None):
        """
        设置指定位置的健康度定义

        :param score_index:
        :param val:
        :param index:
        :return:
        """
        self.get_health_define(index)[score_index] = val

    def get_all_health_define(self, start: int = None, stop: int = None) -> List[List[str]]:
        """
        获取所有健康度定义列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_range('health_define', start, stop)

    def get_status(self, index: int = None) -> int:
        """
        获取状态

        :param index:
        :return:
        """
        return self.get_value('status', index)

    def set_status(self, value: int, index: int = None):
        """
        设置状态

        :param value:
        :param index:
        :return:
        """
        self.set_value('status', value, index)

    def get_all_status(self, start: int = None, stop: int = None) -> List[int]:
        """
        获取所有状态数据

        :param start:
        :param stop:
        :return:
        """
        return self.get_range('status', start, stop)

    def get_stat_item(self, index: int = None) -> Optional[StatItem]:
        """
        获取统计数据

        :param index:
        :return:
        """
        return self.get_value('stat_item', index)

    def set_stat_item(self, stat_item: StatItem, index: int = None):
        """
        设置统计数据

        :param stat_item:
        :param index:
        :return:
        """
        self.set_value('stat_item', stat_item, index)

    def get_all_stat_item(self, start: int = None, stop: int = None) -> List[Optional[StatItem]]:
        """
        获取所有统计数据

        :param start:
        :param stop:
        :return:
        """
        return self.get_range('stat_item', start, stop)

    def get_event(self, index: int = None) -> List[Event]:
        """
        获取事件

        :param index:
        :return:
        """
        return self.get_value('event', index)

    def get_dist_event(self, etype: int, index: int = None) -> List[Event]:
        """
        获取指定类型的事件

        :param etype:
        :param index:
        :return:
        """
        return [event for event in self.get_value('event', index) if event.etype == etype]

    def set_event(self, event: Event, index: int = None):
        """
        新增事件

        :param event:
        :param index:
        :return:
        """
        return self.get_value('event', index).append(event)

    def get_all_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有事件

        :param start:
        :param stop:
        :return:
        """
        return self.get_range('event', start, stop)

    def get_all_dist_event(self, etype: int, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有指定类型的事件

        :param etype:
        :param start:
        :param stop:
        :return:
        """
        return [[event for event in events if event.etype == etype] for events in self.get_all_event(start, stop)]

    def get_alarm_event(self, index: int = None) -> List[Event]:
        """
        获取报警事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.alarm, index)

    def get_all_alarm_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有报警事件列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.alarm, start, stop)

    def get_period_event(self, index: int = None) -> List[Event]:
        """
        获取周期事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.period, index)

    def get_all_period_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有周期事件列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.period, start, stop)

    def get_part_event(self, index: int = None) -> List[Event]:
        """
        获取配件加工事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.part, index)

    def get_all_part_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有配件加工事件列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.part, start, stop)

    def get_operation_start_event(self, index: int = None) -> List[Event]:
        """
        获取操作起始事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.operation_start, index)

    def get_all_operation_start_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有操作起始事件

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.operation_start, start, stop)

    def get_operation_stop_event(self, index: int = None) -> List[Event]:
        """
        获取操作结束事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.operation_stop, index)

    def get_all_operation_stop_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有操作结束事件

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.operation_stop, start, stop)

    def get_extend_event(self, index: int = None) -> List[Event]:
        """
        获取扩展信息配置事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.extend_event, index)

    def get_all_extend_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有扩展信息配置事件

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.extend_event, start, stop)

    def get_show_message_event(self, index: int = None) -> List[Event]:
        """
        获取信息展示事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.show_message_event, index)

    def get_all_show_message_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有信息展示事件

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.show_message_event, start, stop)

    def get_status_alarm_event(self, index: int = None) -> List[Event]:
        """
        获取状态异常事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.status_alarm, index)

    def get_all_status_alarm_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有状态报警事件列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.status_alarm, start, stop)

    def get_integrate_alarm_event(self, index: int = None) -> List[Event]:
        """
        获取综合异常事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.integrate_alarm, index)

    def get_all_integrate_alarm_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有综合报警事件列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.integrate_alarm, start, stop)

    def get_features_alarm_event(self, index: int = None) -> List[Event]:
        """
        获取特征值异常事件

        :param index:
        :return:
        """
        return self.get_dist_event(event_type.features_alarm, index)

    def get_all_features_alarm_event(self, start: int = None, stop: int = None) -> List[List[Event]]:
        """
        获取所有特征值报警事件列表

        :param start:
        :param stop:
        :return:
        """
        return self.get_all_dist_event(event_type.features_alarm, start, stop)
