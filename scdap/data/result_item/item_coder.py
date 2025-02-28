"""

@create on: 2021.05.20
"""
from .item import ResultItem
from .item_list import ResultList

from .event import Event
from .event_coder import EventEncoder, EventDecoder

from .stat_item import StatItem
from .stat_item_coder import StatItemDecoder, StatItemEncoder
from ..coder import RefItemDecoder, RefItemEncoder, TYPE_JSON

from scdap.util.tc import datetime_to_long, long_to_datetime, DATETIME_MIN_TIMESTAMP


class ResultItemKV(object):
    status = 'status'
    time = 'time'
    score = 'score'
    event = 'event'
    health_define = 'health_define'
    stat_item = 'stat_item'


class ResultItemEncoder(RefItemEncoder[ResultList, ResultItem, ResultItemKV]):
    def __init__(self, kv, event_encoder: EventEncoder,
                 stat_item_encoder: StatItemEncoder):
        super().__init__(kv)
        self.event_encoder = event_encoder
        self.stat_item_encoder = stat_item_encoder

    def encode_status(self, obj: ResultItem):
        return obj.status

    def encode_time(self, obj: ResultItem):
        return datetime_to_long(obj.time)

    def encode_score(self, obj: ResultItem):
        return obj.score

    def encode_health_define(self, obj: ResultItem):
        return obj.health_define

    def encode_event(self, obj: ResultItem):
        return list(map(self.event_encoder.encode, obj.event))

    def encode_stat_item(self, obj: ResultItem):
        if obj.stat_item:
            return self.stat_item_encoder.encode(obj.stat_item)
        return None


class ResultItemDecoder(RefItemDecoder[ResultList, ResultItem, ResultItemKV]):
    def __init__(self, kv, event_decoder: EventDecoder,
                 stat_item_decoder: StatItemDecoder):
        super().__init__(kv)
        self.event_encoder = event_decoder
        self.stat_item_decoder = stat_item_decoder

    def decode_status(self, obj: TYPE_JSON):
        return obj.get(self.kv.status, 0)

    def decode_time(self, obj: TYPE_JSON):
        return long_to_datetime(obj.get(self.kv.time, DATETIME_MIN_TIMESTAMP))

    def decode_score(self, obj: TYPE_JSON):
        return obj.get(self.kv.score, list())

    def decode_health_define(self, obj: TYPE_JSON):
        return obj.get(self.kv.health_define, list())

    def decode_event(self, obj: TYPE_JSON):
        return [self.event_encoder.decode(o, Event()) for o in obj.get(self.kv.event, list())]

    def decode_stat_item(self, obj: TYPE_JSON):
        stat_item = StatItem()
        self.stat_item_decoder.decode(obj[self.kv.stat_item], stat_item)
        return stat_item
