"""

@create on: 2021.06.02
"""
from scdap.data.feature_item import *
from unittests import flist_utils


class TestFeatureListCoder(object):
    def test_feature_item_decoder(self):
        decoder = FeatureItemDecoder(FeatureItemKV())
        flist = FeatureList('0', 0, flist_utils.column())
        data = flist_utils.random_item_dict()
        src = flist_utils.item_to_decoder_src(data, False)
        decoder.decode(src, flist)
        assert flist.size() == 1

        flist_utils.assert_feature_item(flist.get_last_ref(), data)

    def test_feature_item_encoder(self):
        encoder = FeatureItemEncoder(FeatureItemKV())
        flist = FeatureList('0', 0, flist_utils.column())
        data = flist_utils.random_item_dict()
        flist.append_dict(**data)
        dist = encoder.encode(flist.get_last_ref(), flist)
        src = flist_utils.item_to_decoder_src(data, True)
        assert dist == src

    def test_feature_list_decoder(self):
        decoder = FeatureListDecoder(FeatureListKV(), FeatureItemDecoder(FeatureItemKV()))
        flist = FeatureList('0', 0, flist_utils.column())
        data = flist_utils.random_itemlist_dict()
        src = flist_utils.itemlist_to_decoder_src(data, False)
        decoder.decode(src, flist)
        assert flist.size() == len(data['data'])
        assert flist.algorithm_id == data['algorithm_id']
        assert flist.node_id == data['node_id']
        for i in range(flist.size()):
            flist_utils.assert_feature_item(flist.get_ref(i), data['data'][i])

    def test_feature_list_encdoer(self):
        encoder = FeatureListEncoder(FeatureListKV(), FeatureItemEncoder(FeatureItemKV()))
        flist = FeatureList('0', 0, flist_utils.column())
        data = flist_utils.random_itemlist_dict()
        flist.algorithm_id = data['algorithm_id']
        flist.node_id = data['node_id']
        for d in data['data']:
            flist.append_dict(**d)
        dist = encoder.encode(flist)
        src = flist_utils.itemlist_to_decoder_src(data, True)
        assert dist == src
