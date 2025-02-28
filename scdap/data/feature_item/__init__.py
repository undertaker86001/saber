"""

@create on: 2021.05.20
"""
from .item import FeatureItem
from .item_coder import DEFAULT_TEMPERATURE
from .item_coder import FeatureItemKV, FeatureItemDecoder, FeatureItemEncoder

from .item_list import FeatureList, IFeature
from .item_list_coder import FeatureListEncoder, FeatureListDecoder, FeatureListKV


def check():
    from .._check import check_function, check_value, check_slot, check_default
    check_slot(IFeature, FeatureItem)
    check_default(FeatureItem)

    kv = FeatureItemKV()
    for col in IFeature.__slots__:
        check_function(FeatureItemEncoder, f'encode_{col}')
        check_function(FeatureItemDecoder, f'decode_{col}')
        check_function(FeatureList, f'get_{col}')
        check_function(FeatureList, f'get_all_{col}')
        check_value(kv, col)

    kv = FeatureListKV()
    for key in FeatureList.__slots__:
        check_function(FeatureList, f'get_{key}')
        check_function(FeatureListEncoder, f'encode_{key}')
        check_function(FeatureListDecoder, f'decode_{key}')
        check_value(kv, key)
