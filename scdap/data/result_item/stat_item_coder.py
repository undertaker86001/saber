"""

@create on: 2021.05.20
"""

from .stat_item import StatItem
from ..coder import ItemEncoder, ItemDecoder, TYPE_JSON

from scdap.util.tc import datetime_to_long, long_to_datetime, DATETIME_MIN


class StatItemKV(object):
    status = 'status'
    time = 'time'
    score = 'score'
    size = 'size'


class StatItemEncoder(ItemEncoder[StatItem, StatItemKV]):

    def encode_status(self, obj: StatItem):
        return obj.status

    def encode_time(self, obj: StatItem):
        return datetime_to_long(obj.time)

    def encode_score(self, obj: StatItem):
        return obj.score

    def encode_size(self, obj: StatItem):
        return obj.size


class StatItemDecoder(ItemDecoder[StatItem, StatItemKV]):

    def decode_status(self, obj: TYPE_JSON):
        return obj.get(self.kv.status, dict())

    def decode_time(self, obj: TYPE_JSON):
        return long_to_datetime(obj.get(self.kv.time, DATETIME_MIN))

    def decode_score(self, obj: TYPE_JSON):
        return obj.get(self.kv.score, dict())

    def decode_size(self, obj: TYPE_JSON):
        return obj.get(self.kv.size, 0)
