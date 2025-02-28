"""

@create on: 2021.01.02
"""
from datetime import datetime
from typing import Union, Generic, TypeVar

from .base import ItemList, RefItem

TYPE_JSON = Union[list, dict]
DEFAULT_DATETIME = datetime(2000, 1, 1)


class BaseItemListKV(object):
    data = 'data'


ITEM = TypeVar('ITEM')
ITEM_KV = TypeVar('ITEM_KV')

REFITEM = TypeVar('REFITEM', bound=RefItem)
REFITEM_KV = TypeVar('REFITEM_KV')

ITEM_LIST = TypeVar('ITEM_LIST', bound=ItemList)
ITEM_LIST_KV = TypeVar('ITEM_LIST_KV', bound=BaseItemListKV)


class ItemEncoder(Generic[ITEM, ITEM_KV]):
    def __init__(self, kv: ITEM_KV):
        self.kv = kv

    def encode(self, obj: ITEM) -> TYPE_JSON:
        result = dict()
        for key in obj.__slots__:
            result[getattr(self.kv, key)] = getattr(self, f'encode_{key}')(obj)
        return result

    __call__ = encode


class RefItemEncoder(Generic[ITEM_LIST, REFITEM, REFITEM_KV]):
    def __init__(self, kv: REFITEM_KV):
        self.kv = kv

    def encode(self, obj: REFITEM, itemlist: ITEM_LIST) -> TYPE_JSON:
        result = dict()
        for key in itemlist.select_keys():
            result[str(getattr(self.kv, key))] = getattr(self, f'encode_{key}')(obj)
        return result

    __call__ = encode


class ItemListEncoder(Generic[ITEM_LIST, REFITEM, ITEM_LIST_KV]):
    def __init__(self, kv: ITEM_LIST_KV, item_encoder: RefItemEncoder[ITEM_LIST, REFITEM, REFITEM_KV]):
        self.kv = kv
        self.item_encoder = item_encoder

    def encode(self, item_list: ITEM_LIST) -> TYPE_JSON:
        result = dict()
        for key in item_list.__slots__:
            result[getattr(self.kv, key)] = getattr(self, f'encode_{key}')(item_list)
        result[self.kv.data] = self.encode_data(item_list)
        return result

    def encode_data(self, item_list: ITEM_LIST) -> list:
        data = list()
        for item in item_list.generator():
            data.append(self.item_encoder.encode(item, item_list))
        return data

    __call__ = encode


class ItemDecoder(Generic[ITEM, ITEM_KV]):
    def __init__(self, kv: ITEM_KV):
        self.kv = kv

    def decode(self, obj: TYPE_JSON, to_obj: ITEM) -> ITEM:
        for key in to_obj.__slots__:
            setattr(to_obj, key, getattr(self, f'decode_{key}')(obj))
        return to_obj

    __call__ = decode


class RefItemDecoder(Generic[ITEM_LIST, REFITEM, REFITEM_KV]):
    def __init__(self, kv: REFITEM_KV):
        self.kv = kv

    def decode(self, obj: TYPE_JSON, itemlist: ITEM_LIST) -> REFITEM:
        temp = dict()
        for key in itemlist.select_keys():
            temp[key] = getattr(self, f'decode_{key}')(obj)
        itemlist.append_dict(**temp)
        return itemlist.get_last_ref()

    __call__ = decode


class ItemListDecoder(Generic[ITEM_LIST, REFITEM, ITEM_LIST_KV]):
    def __init__(self, kv: ITEM_LIST_KV, item_decoder: RefItemDecoder[ITEM_LIST, REFITEM, REFITEM_KV]):
        self.kv = kv
        self.item_decoder = item_decoder

    def decode(self, obj: TYPE_JSON, itemlist: ITEM_LIST) -> ITEM_LIST:
        for key in itemlist.__slots__:
            setattr(itemlist, key, getattr(self, f'decode_{key}')(obj))
        self.decode_data(obj, itemlist)
        return itemlist

    def decode_data(self, obj: TYPE_JSON, itemlist: ITEM_LIST):
        for o in obj.get(self.kv.data):
            self.item_decoder.decode(o, itemlist)

    __call__ = decode
