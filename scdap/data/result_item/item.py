"""

@create on: 2021.05.20
"""
from typing import List
from datetime import datetime

from .event import Event
from .stat_item import StatItem

from scdap.util.tc import DATETIME_MIN
from ..base import RefItem

__result_default__ = {
    'time': DATETIME_MIN,
    'score': list(),
    'health_define': list(),
    'event': list(),
    'status': 0,
    'stat_item': None
}

__result_key__ = tuple(__result_default__.keys())


class ResultItem(RefItem):
    __default__ = __result_default__.copy()
    __slots__ = __result_key__

    status: int
    # 时间戳
    time: datetime
    # 健康度, 可以是多个健康度
    score: List[int]
    health_define: List[str]
    # 事件
    event: List[Event]
    # 统计结果
    stat_item: StatItem
