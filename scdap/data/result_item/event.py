"""

@create on: 2020.12.29
"""
from datetime import datetime
from typing import Dict


class Event(object):
    """
    事件数据结构
    """
    __slots__ = [
        "algorithm_id", "node_id", "etype", "start", "stop", "time",
        "message", "name", "status", "score", "code", "check_result",
        "detail", 'extend'
    ]

    def __init__(self, etype: int = 0, algorithm_id: str = "", node_id: int = 0,
                 status: int = 0, score: Dict[str, int] = None,
                 name: str = "", time: datetime = None,
                 start: datetime = None, stop: datetime = None,
                 message: str = "", code: int = 0, check_result: int = 0, detail: str = '',
                 extend: dict = None):
        self.algorithm_id: str = algorithm_id
        self.node_id: int = node_id
        self.etype: int = etype
        self.status = status
        self.start: datetime = start
        self.stop: datetime = stop
        self.time: datetime = time
        self.message: str = message
        self.name: str = name
        self.score: Dict[str, int] = score or dict()
        self.code: int = code
        self.check_result: int = check_result
        self.detail = detail
        self.extend = extend or dict()

    def __str__(self) -> str:
        return f"[nid: {self.node_id}, aid: {self.algorithm_id}, time: {self.time}, " \
               f"type: {self.etype}, name: '{self.name}', " \
               f"status: {self.status}, score: {self.score}, " \
               f"start: {self.start}, stop: {self.stop}, message: '{self.message}', " \
               f"code: {self.code}, check_result: {self.check_result}, detail: {self.detail}," \
               f"extend: {self.extend}]"

    def __eq__(self, other):
        for slot in self.__slots__:
            if getattr(self, slot) != getattr(other, slot):
                return False
        return True
