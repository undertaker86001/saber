"""

@create on: 2021.05.24
"""
import pytest

from scdap.data.feature_item import FeatureList, FeatureItem, IFeature, check

from unittests import flist_utils


class TestFeatureItem(object):
    def test_auto_check(self):
        """
        调用内置的检查方法自动检查

        :return:
        """
        check()

    def test_column(self):
        # 防止漏掉任何column
        assert set(flist_utils.column()) == set(FeatureItem.__slots__) == set(IFeature.__slots__)

    def test_create_feature_list(self):
        # 检查FeatureList创建
        column = flist_utils.column().copy()
        flist = FeatureList('0', 0, column)
        assert flist.algorithm_id == '0'
        assert flist.node_id == 0
        assert flist.select_keys() == tuple(column)
        assert flist.size() == 0
        assert flist.empty()

    def test_default(self):
        """
        测试默认数值

        :return:
        """
        column = flist_utils.column().copy()
        flist = FeatureList('0', 0, column)
        flist.append_dict()
        data = flist.get_last_ref().__default__.copy()
        flist_utils.assert_feature_item(flist.get_last_ref(), data)

    def test_column_check(self):
        # 校验column配置后的报错情况
        flist = FeatureList('0', 0, ['time'])
        data = flist_utils.random_item_dict()
        flist.append_dict(**data)
        flist.next()

        assert flist.get_time() == data['time']
        assert flist.get_all_time() == [data['time']]

        with pytest.raises(Exception):
            flist.get_meanhf()
        with pytest.raises(Exception):
            flist.get_meanlf()
        with pytest.raises(Exception):
            flist.get_mean()
        with pytest.raises(Exception):
            flist.get_std()
        with pytest.raises(Exception):
            flist.get_feature1()
        with pytest.raises(Exception):
            flist.get_feature2()
        with pytest.raises(Exception):
            flist.get_feature3()
        with pytest.raises(Exception):
            flist.get_feature4()
        with pytest.raises(Exception):
            flist.get_bandspectrum()
        with pytest.raises(Exception):
            flist.get_customfeature()
        with pytest.raises(Exception):
            flist.get_peakfreqs()
        with pytest.raises(Exception):
            flist.get_peakpowers()
        with pytest.raises(Exception):
            flist.get_temperature()
        with pytest.raises(Exception):
            flist.get_status()
        with pytest.raises(Exception):
            flist.get_hrtime()

        with pytest.raises(Exception):
            flist.get_all_meanhf()
        with pytest.raises(Exception):
            flist.get_all_meanlf()
        with pytest.raises(Exception):
            flist.get_all_mean()
        with pytest.raises(Exception):
            flist.get_all_std()
        with pytest.raises(Exception):
            flist.get_all_feature1()
        with pytest.raises(Exception):
            flist.get_all_feature2()
        with pytest.raises(Exception):
            flist.get_all_feature3()
        with pytest.raises(Exception):
            flist.get_all_feature4()
        with pytest.raises(Exception):
            flist.get_all_bandspectrum()
        with pytest.raises(Exception):
            flist.get_all_customfeature()
        with pytest.raises(Exception):
            flist.get_all_peakfreqs()
        with pytest.raises(Exception):
            flist.get_all_peakpowers()
        with pytest.raises(Exception):
            flist.get_all_temperature()
        with pytest.raises(Exception):
            flist.get_all_status()
        with pytest.raises(Exception):
            flist.get_all_hrdata()

        item = flist.get_last_ref()

        with pytest.raises(Exception):
            v = item.meanhf
        with pytest.raises(Exception):
            v = item.meanlf
        with pytest.raises(Exception):
            v = item.mean
        with pytest.raises(Exception):
            v = item.std
        with pytest.raises(Exception):
            v = item.feature1
        with pytest.raises(Exception):
            v = item.feature2
        with pytest.raises(Exception):
            v = item.feature3
        with pytest.raises(Exception):
            v = item.feature4
        with pytest.raises(Exception):
            v = item.bandspectrum
        with pytest.raises(Exception):
            v = item.customfeature
        with pytest.raises(Exception):
            v = item.peakfreqs
        with pytest.raises(Exception):
            v = item.peakpowers
        with pytest.raises(Exception):
            v = item.temperature
        with pytest.raises(Exception):
            v = item.status
        with pytest.raises(Exception):
            v = item.hrtime

    def test_append_feature_list(self):
        # 检查FeatureList.append接口
        column = flist_utils.column().copy()
        data = flist_utils.random_item_dict()
        flist = FeatureList('0', 0, column)
        flist.append_dict(**data)
        assert flist.size() == 1
        assert not flist.empty()

        flist_utils.assert_feature_list(flist, data, 0)
        flist_utils.assert_feature_item(flist.get_ref(0), data)

    def test_append_feature_list_with_maxlen(self):
        # append的时候检查maxlen是否生效
        column = flist_utils.column().copy()
        data = flist_utils.random_item_dict()
        flist = FeatureList('0', 0, column, 2)
        flist.append_dict(**data)
        flist.append_dict(**data)
        flist.append_dict(**data)
        # 配一段不重复的数据
        # 测试是否在有maxlen情况下会不会把最晚进入的数据drop掉
        data = flist_utils.random_item_dict()
        flist.append_dict(**data)
        assert flist.size() == 2

        flist_utils.assert_feature_list(flist, data, flist.size() - 1)
        flist_utils.assert_feature_item(flist.get_last_ref(), data)

    def get_dict_from_dictlist(self, dictlist, index):
        return {key: val[index] for key, val in dictlist.items()}

    def test_extend_feature_list(self):
        # append的时候检查maxlen是否生效
        column = flist_utils.column().copy()
        data = flist_utils.random_list_dict()

        flist = FeatureList('0', 0, column)
        flist.extend_ldict(**data)
        assert flist.size() == 3
        for i in range(3):
            temp = self.get_dict_from_dictlist(data, i)
            flist_utils.assert_feature_list(flist, temp, i)
            flist_utils.assert_feature_item(flist.get_ref(i), temp)

    def test_pop_feature_list(self):
        column = flist_utils.column().copy()
        data = flist_utils.random_list_dict()

        flist = FeatureList('0', 0, column)
        flist.extend_ldict(**data)
        # 抛掉最左边的数据
        flist.pop()
        assert flist.size() == 2
        flist_utils.assert_feature_item(flist.get_last_ref(), self.get_dict_from_dictlist(data, -1))
        flist_utils.assert_feature_item(flist.get_ref(0), self.get_dict_from_dictlist(data, 1))

        # 抛弃最右边的数据
        flist.remove(-1)
        assert flist.size() == 1
        flist_utils.assert_feature_item(flist.get_last_ref(), self.get_dict_from_dictlist(data, 1))
        flist_utils.assert_feature_item(flist.get_ref(0), self.get_dict_from_dictlist(data, 1))

    def test_range_feature_list(self):
        column = flist_utils.column().copy()
        data = flist_utils.random_list_dict()

        flist = FeatureList('0', 0, column)
        flist.extend_ldict(**data)
        for key, val in data.items():
            flist_utils.assert_feature_range(flist.get_range(key, 0, flist.size()), val)
            flist_utils.assert_feature_range(flist.get_range(key, 0, 1), val[0:1])
            flist_utils.assert_feature_range(flist.get_range(key, 1, 2), val[1:2])
            flist_utils.assert_feature_range(flist.get_range(key, 1, 1), [])

        flist.remove_range(1, 2)
        for key, val in data.items():
            del val[1:2]
            flist_utils.assert_feature_range(flist.get_range(key, 0, flist.size()), val)

    def test_position(self):
        flist = FeatureList('0', 0, ['meanhf'])
        assert flist.get_position() == -1
        assert not flist.next()
        assert flist.position_at_the_start()
        assert flist.position_at_the_end()
        assert flist.get_position() == -1

        flist.extend_ldict(meanhf=[1.1, 1.2])
        assert flist.position_at_the_start()

        flist.next()
        assert flist.get_position() == 0
        assert flist.get_meanhf() == 1.1

        flist.next()
        assert flist.get_position() == 1
        assert flist.get_meanhf() == 1.2

        assert not flist.next()
        assert flist.get_position() == 1
        assert flist.position_at_the_end()

        flist.reset_position()
        assert flist.get_position() == -1
        assert flist.position_at_the_start()

        flist.position_to_end()
        assert flist.get_position() == 1
        assert flist.position_at_the_end()

        flist.set_position(1)
        assert flist.get_position() == 1
