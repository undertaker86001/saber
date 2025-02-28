"""

@create on: 2021.06.02
"""
from scdap.data.result_item import *
from unittests import rlist_utils


class TestResultListCoder(object):
    def test_result_item_decoder(self):
        decoder = ResultItemDecoder(ResultItemKV(), EventDecoder(EventKV()), StatItemDecoder(StatItemKV()))
        rlist = ResultList('0', 0)
        data = rlist_utils.random_item_dict()
        src = rlist_utils.item_to_decoder_src(data)
        decoder.decode(src, rlist)
        assert rlist.size() == 1

        rlist_utils.assert_result_item(rlist.get_last_ref(), data)

    def test_result_item_encoder(self):
        encoder = ResultItemEncoder(ResultItemKV(), EventEncoder(EventKV()), StatItemEncoder(StatItemKV()))
        rlist = ResultList('0', 0)
        data = rlist_utils.random_item_dict()
        rlist.append_dict(**data)
        dist = encoder.encode(rlist.get_last_ref(), rlist)
        src = rlist_utils.item_to_decoder_src(data)
        assert dist == src

    def test_result_list_decoder(self):
        decoder = ResultListDecoder(ResultListKV(),
                                    ResultItemDecoder(ResultItemKV(), EventDecoder(EventKV()),
                                                      StatItemDecoder(StatItemKV())))
        rlist = ResultList('0', 0)
        data = rlist_utils.random_itemlist_dict()
        src = rlist_utils.itemlist_to_decoder_src(data)
        decoder.decode(src, rlist)
        assert rlist.size() == len(data['data'])
        assert rlist.algorithm_id == data['algorithm_id']
        assert rlist.node_id == data['node_id']
        for i in range(rlist.size()):
            rlist_utils.assert_result_item(rlist.get_ref(i), data['data'][i])

    def test_result_list_encdoer(self):
        encoder = ResultListEncoder(ResultListKV(), ResultItemEncoder(ResultItemKV(),
                                                                      EventEncoder(EventKV()),
                                                                      StatItemEncoder(StatItemKV())))
        rlist = ResultList('0', 0)
        data = rlist_utils.random_itemlist_dict()
        rlist.algorithm_id = data['algorithm_id']
        rlist.node_id = data['node_id']
        for d in data['data']:
            rlist.append_dict(**d)
        dist = encoder.encode(rlist)
        src = rlist_utils.itemlist_to_decoder_src(data)
        assert dist == src
