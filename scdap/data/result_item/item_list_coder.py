"""

@create on: 2021.05.20
"""
from .item import ResultItem
from .item_list import ResultList
from ..coder import ItemListDecoder, ItemListEncoder, BaseItemListKV, TYPE_JSON


class ResultListKV(BaseItemListKV):
    algorithm_id = 'algorithm_id'
    node_id = 'node_id'
    data = 'data'


class ResultListEncoder(ItemListEncoder[ResultList, ResultItem, ResultListKV]):
    def encode_algorithm_id(self, obj: ResultList):
        return obj.algorithm_id

    def encode_node_id(self, obj: ResultList):
        return obj.node_id


class ResultListDecoder(ItemListDecoder[ResultList, ResultItem, ResultListKV]):
    def decode_algorithm_id(self, obj: TYPE_JSON):
        return obj.get(self.kv.algorithm_id, '')

    def decode_node_id(self, obj: TYPE_JSON):
        return obj.get(self.kv.node_id, 0)
