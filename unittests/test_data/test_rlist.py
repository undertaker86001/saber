"""

@create on: 2021.05.24
"""
import numpy as np
from unittests import rlist_utils
from scdap.data.result_item import ResultItem, ResultList, IResult, check


class TestFeatureItem(object):
    def column(self):
        return ['time', 'score', 'health_define', 'event', 'status', 'stat_item']

    def test_auto_check(self):
        """
        调用内置的检查方法自动检查

        :return:
        """
        check()

    def test_column(self):
        # 防止漏掉任何column
        assert set(self.column()) == set(ResultItem.__slots__) == set(IResult.__slots__)

    def assert_feature_range(self, item_list, src_list):
        assert len(item_list) == len(src_list)
        for i in range(len(item_list)):
            v1 = item_list[i]
            v2 = src_list[i]
            if isinstance(v1, np.ndarray):
                v1 = v1.tolist()
                v2 = v2.tolist()
            assert v1 == v2

    def test_create_result_list(self):
        # 检查ResultList创建
        
        rlist = ResultList('0', 0)
        assert rlist.algorithm_id == '0'
        assert rlist.node_id == 0
        assert rlist.select_keys() == tuple(self.column())
        assert rlist.size() == 0
        assert rlist.empty()

    def test_default(self):
        """
        测试默认数值

        :return:
        """
        rlist = ResultList('0', 0)
        rlist.append_dict()
        data = rlist.get_last_ref().__default__.copy()
        rlist_utils.assert_result_item(rlist.get_last_ref(), data)

    def test_append_result_list(self):
        # 检查ResultList.append接口
        data = rlist_utils.random_item_dict()
        rlist = ResultList('0', 0)
        rlist.append_dict(**data)
        assert rlist.size() == 1
        assert not rlist.empty()

        rlist_utils.assert_result_list(rlist, data, 0)
        rlist_utils.assert_result_item(rlist.get_ref(0), data)

    def test_append_result_list_with_maxlen(self):
        # append的时候检查maxlen是否生效
        
        data = rlist_utils.random_item_dict()
        rlist = ResultList('0', 0, 2)
        rlist.append_dict(**data)
        rlist.append_dict(**data)
        rlist.append_dict(**data)
        # 配一段不重复的数据
        # 测试是否在有maxlen情况下会不会把最晚进入的数据drop掉
        data = rlist_utils.random_item_dict()
        rlist.append_dict(**data)
        assert rlist.size() == 2

        rlist_utils.assert_result_list(rlist, data, rlist.size() - 1)
        rlist_utils.assert_result_item(rlist.get_last_ref(), data)

    def get_dict_from_dictlist(self, dictlist, index):
        return {key: val[index] for key, val in dictlist.items()}

    def test_extend_result_list(self):
        # append的时候检查maxlen是否生效
        
        data = rlist_utils.random_list_dict()

        rlist = ResultList('0', 0)
        rlist.extend_ldict(**data)
        assert rlist.size() == 3
        for i in range(3):
            temp = self.get_dict_from_dictlist(data, i)
            rlist_utils.assert_result_list(rlist, temp, i)
            rlist_utils.assert_result_item(rlist.get_ref(i), temp)

    def test_pop_result_list(self):

        data = rlist_utils.random_list_dict()

        rlist = ResultList('0', 0)
        rlist.extend_ldict(**data)
        # 抛掉最左边的数据
        rlist.pop()
        assert rlist.size() == 2
        rlist_utils.assert_result_item(rlist.get_last_ref(), self.get_dict_from_dictlist(data, -1))
        rlist_utils.assert_result_item(rlist.get_ref(0), self.get_dict_from_dictlist(data, 1))

        # 抛弃最右边的数据
        rlist.remove(-1)
        assert rlist.size() == 1
        rlist_utils.assert_result_item(rlist.get_last_ref(), self.get_dict_from_dictlist(data, 1))
        rlist_utils.assert_result_item(rlist.get_ref(0), self.get_dict_from_dictlist(data, 1))

    def test_range_result_list(self):

        data = rlist_utils.random_list_dict()

        rlist = ResultList('0', 0)
        rlist.extend_ldict(**data)
        for key, val in data.items():
            self.assert_feature_range(rlist.get_range(key, 0, rlist.size()), val)
            self.assert_feature_range(rlist.get_range(key, 0, 1), val[0:1])
            self.assert_feature_range(rlist.get_range(key, 1, 2), val[1:2])
            self.assert_feature_range(rlist.get_range(key, 1, 1), [])

    def test_position(self):
        rlist = ResultList('0', 0)
        assert rlist.get_position() == -1
        assert not rlist.next()
        assert rlist.position_at_the_start()
        assert rlist.position_at_the_end()
        assert rlist.get_position() == -1
        data1 = rlist_utils.random_item_dict()
        rlist.append_dict(**data1)
        data2 = rlist_utils.random_item_dict()
        rlist.append_dict(**data2)
        assert rlist.position_at_the_start()

        rlist.next()
        assert rlist.get_position() == 0
        assert rlist.get_status() == data1['status']

        rlist.next()
        assert rlist.get_position() == 1
        assert rlist.get_status() == data2['status']

        assert not rlist.next()
        assert rlist.get_position() == 1
        assert rlist.position_at_the_end()

        rlist.reset_position()
        assert rlist.get_position() == -1
        assert rlist.position_at_the_start()

        rlist.position_to_end()
        assert rlist.get_position() == 1
        assert rlist.position_at_the_end()

        rlist.set_position(1)
        assert rlist.get_position() == 1
