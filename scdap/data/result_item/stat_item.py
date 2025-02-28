"""

@create on: 2021.05.20
"""
from datetime import datetime
from typing import Dict

from scdap.util.tc import DATETIME_MIN


class StatItem(object):
    __slots__ = ['time', 'status', 'score', 'size']

    def __init__(self, time: datetime = DATETIME_MIN, size: int = 0, status: dict = None, score: dict = None):
        super().__init__()
        self.time = time
        self.size = size
        self.status: Dict[str, int] = status or dict()
        self.score: Dict[str, int] = score or dict()

    def __str__(self) -> str:
        return f'time: {self.time} size: {self.size} status: {self.status} score: {self.score}'

    def __eq__(self, other):
        for slot in self.__slots__:
            if getattr(self, slot) != getattr(other, slot):
                return False
        return True
