"""

@create on: 2020.12.29
"""
from scdap.util.tc import datetime_to_long, long_to_datetime

from .event import Event

from ..coder import ItemEncoder, ItemDecoder, TYPE_JSON


class EventKV(object):
    algorithm_id = "algorithm_id"
    node_id = "node_id"
    etype = "etype"
    start = "start"
    stop = "stop"
    status = "status"
    time = "time"
    message = "message"
    name = "name"
    score = "score"
    code = "code"
    check_result = "check_result"
    detail = 'detail'
    extend = 'extend'


class EventEncoder(ItemEncoder[Event, EventKV]):
    def encode_algorithm_id(self, obj: Event):
        return obj.algorithm_id

    def encode_node_id(self, obj: Event):
        return obj.node_id

    def encode_etype(self, obj: Event):
        return obj.etype

    def encode_status(self, obj: Event):
        return obj.status

    def encode_start(self, obj: Event):
        return None if obj.start is None else datetime_to_long(obj.start)

    def encode_stop(self, obj: Event):
        return None if obj.stop is None else datetime_to_long(obj.stop)

    def encode_time(self, obj: Event):
        return None if obj.time is None else datetime_to_long(obj.time)

    def encode_message(self, obj: Event):
        return obj.message

    def encode_name(self, obj: Event):
        return obj.name

    def encode_score(self, obj: Event):
        return obj.score

    def encode_code(self, obj: Event):
        return obj.code

    def encode_check_result(self, obj: Event):
        return obj.check_result

    def encode_detail(self, obj: Event):
        return obj.detail

    def encode_extend(self, obj: Event):
        return obj.extend


class EventDecoder(ItemDecoder[Event, EventKV]):
    def decode_algorithm_id(self, obj: TYPE_JSON):
        return obj.get(self.kv.algorithm_id, "")

    def decode_node_id(self, obj: TYPE_JSON):
        return obj.get(self.kv.node_id, 0)

    def decode_etype(self, obj: TYPE_JSON):
        return obj.get(self.kv.etype, 0)

    def decode_status(self, obj: TYPE_JSON):
        return obj.get(self.kv.status, 0)

    def decode_start(self, obj: TYPE_JSON):
        start = obj.get(self.kv.start, None)
        if start:
            start = long_to_datetime(start)
        return start

    def decode_stop(self, obj: TYPE_JSON):
        stop = obj.get(self.kv.stop, None)
        if stop:
            stop = long_to_datetime(stop)
        return stop

    def decode_time(self, obj: TYPE_JSON):
        time = obj.get(self.kv.time)
        if time:
            time = long_to_datetime(time)
        return time

    def decode_message(self, obj: TYPE_JSON):
        return obj.get(self.kv.message, "")

    def decode_name(self, obj: TYPE_JSON):
        return obj.get(self.kv.name, "")

    def decode_score(self, obj: TYPE_JSON):
        return obj.get(self.kv.score, 100)

    def decode_code(self, obj: TYPE_JSON):
        return obj.get(self.kv.code, 0)

    def decode_check_result(self, obj: TYPE_JSON):
        return obj.get(self.kv.check_result, 0)

    def decode_detail(self, obj: TYPE_JSON):
        return obj.get(self.kv.detail, '')

    def decode_extend(self, obj: TYPE_JSON):
        return obj.get(self.kv.extend, dict())
