"""

@create on: 2021.05.20
"""
from .item import ResultItem
from .item_coder import ResultItemEncoder, ResultItemDecoder, ResultItemKV

from .event import Event
from .event_coder import EventEncoder, EventDecoder, EventKV

from .stat_item import StatItem
from .stat_item_coder import StatItemEncoder, StatItemDecoder, StatItemKV

from .item_list import ResultList, IResult
from .item_list_coder import ResultListEncoder, ResultListDecoder, ResultListKV


def check():
    from .._check import check_function, check_value, check_slot, check_default
    check_slot(IResult, ResultItem)
    check_default(ResultItem)

    kv = ResultItemKV()
    for key in ResultItem.__slots__:
        check_function(ResultItemEncoder, f'encode_{key}')
        check_function(ResultItemDecoder, f'decode_{key}')
        check_function(ResultList, f'get_{key}')
        check_function(ResultList, f'get_all_{key}')
        check_function(ResultList, f'set_{key}')
        check_value(kv, key)

    kv = ResultListKV()
    for key in ResultList.__slots__:
        check_function(ResultList, f'get_{key}')
        check_function(ResultListEncoder, f'encode_{key}')
        check_function(ResultListDecoder, f'decode_{key}')
        check_value(kv, key)
