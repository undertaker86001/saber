"""

@create on: 2021.05.20
"""
from .item import FeatureItem
from .item_list import FeatureList
from ..coder import ItemListDecoder, ItemListEncoder, TYPE_JSON, BaseItemListKV


class FeatureListKV(BaseItemListKV):
    algorithm_id = 'algorithm_id'
    node_id = 'node_id'
    data = 'data'


class FeatureListEncoder(ItemListEncoder[FeatureList, FeatureItem, FeatureListKV]):
    def encode_algorithm_id(self, obj: FeatureList):
        return obj.algorithm_id

    def encode_node_id(self, obj: FeatureList):
        return obj.node_id


class FeatureListDecoder(ItemListDecoder[FeatureList, FeatureItem, FeatureListKV]):
    def decode_algorithm_id(self, obj: TYPE_JSON):
        return obj.get(self.kv.algorithm_id, '')

    def decode_node_id(self, obj: TYPE_JSON):
        return obj.get(self.kv.node_id, 0)
