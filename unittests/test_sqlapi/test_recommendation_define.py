"""

@create on: 2021.01.27
"""
import pytest

from scdap.sqlapi import recommendation_define


class TestRecommendationDefine(object):
    def teardown_class(self):
        # 清理结果
        recommendation_define._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        recommendation_define._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                recommendation_define._api.create_table()
                try:
                    f(self)
                finally:
                    recommendation_define._api.drop_table()

    def __test_api_add_and_get(self, p):
        recommendation_define.add_recommendation_define(**p)
        p = p.copy()
        assert recommendation_define.get_recommendation_define(p['recommendation_name']) == p

    def _test_api_add_hid_self(self):
        # 校验id自增
        p = {
            'recommendation_name': 'test1',
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        recommendation_define.add_recommendation_define(**p)
        temp = p.copy()
        temp['recommendation_id'] = 0
        assert recommendation_define.get_recommendation_define(p['recommendation_name']) == temp

        p['recommendation_name'] = 'test2'
        recommendation_define.add_recommendation_define(**p)
        temp = p.copy()
        temp['recommendation_id'] = 1
        assert recommendation_define.get_recommendation_define(p['recommendation_name']) == temp

    def _test_api_add(self):
        total_hs = dict()
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        total_hs[p['recommendation_name']] = p.copy()
        self.__test_api_add_and_get(p)

        p['recommendation_name'] = 'test2'
        p['recommendation_id'] = 1
        p['cn_name'] = 'rt_cn_name2'
        p['en_name'] = 'rt_cn_name2'
        total_hs[p['recommendation_name']] = p.copy()
        self.__test_api_add_and_get(p)

        assert recommendation_define.get_recommendation_define(list(total_hs.keys())) == total_hs

    def _test_api_add_with_error(self):
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        recommendation_define.add_recommendation_define(**p)

        # 不允许反复添加数据
        with pytest.raises(Exception):
            recommendation_define.add_recommendation_define(**p)

        p['recommendation_name'] = 'test2'

        with pytest.raises(Exception):
            temp = p.copy()
            temp['recommendation_id'] = '1'
            recommendation_define.add_recommendation_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['cn_name'] = 1
            recommendation_define.add_recommendation_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['extra'] = '11'
            recommendation_define.add_recommendation_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['enabled'] = '11'
            recommendation_define.add_recommendation_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['description'] = 111
            recommendation_define.add_recommendation_define(
                **temp
            )

    def _test_api_update(self):
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        recommendation_define.add_recommendation_define(**p)
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name1',
            'en_name': 'en_name1',
            'extra': {'test': 11},
            'enabled': False,
            'description': 'desc1'
        }
        recommendation_define.update_recommendation_define(**p)
        assert recommendation_define.get_recommendation_define(p['recommendation_name']) == p
        assert recommendation_define.get_recommendation_define(recommendation_id=p['recommendation_id']) == p

    def _test_api_update_with_error(self):
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        recommendation_define.add_recommendation_define(**p)

        with pytest.raises(Exception):
            temp = {
                'recommendation_name': 'test1',
                'cn_name': 1
            }
            recommendation_define.update_recommendation_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'recommendation_name': 'test1',
                'en_name': 1
            }
            recommendation_define.update_recommendation_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'recommendation_name': 'test1',
                'extra': '11'
            }
            recommendation_define.update_recommendation_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'recommendation_name': 'test1',
                'enabled': '11'
            }
            recommendation_define.update_recommendation_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'recommendation_name': 'test1',
                'description': 11
            }
            recommendation_define.update_recommendation_define(**temp)

        assert recommendation_define.get_recommendation_define(p['recommendation_name']) == p
        assert recommendation_define.get_recommendation_define(recommendation_id=p['recommendation_id']) == p

    def _test_api_function_define_exist(self):
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        recommendation_define.add_recommendation_define(**p)

        assert recommendation_define.recommendation_define_exist(p['recommendation_name'])
        assert not recommendation_define.recommendation_define_exist(p['recommendation_name'] + 'sss')
        assert recommendation_define.recommendation_define_exist(recommendation_id=p['recommendation_id'])
        assert not recommendation_define.recommendation_define_exist(recommendation_id=p['recommendation_id'] + 100)

    def _test_api_get_functions(self):
        size = 10
        enable_ids = []
        enable_names = []
        disenable_ids = []
        disenable_names = []
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        for i in range(1, size):
            fname = f'test{i}'
            if (i % 2) == 0:
                enable_ids.append(i)
                enable_names.append(fname)
            else:
                disenable_ids.append(i)
                disenable_names.append(fname)
            temp = p.copy()
            temp['recommendation_name'] = fname
            temp['recommendation_id'] = i
            temp['enabled'] = (i % 2) == 0
            recommendation_define.add_recommendation_define(**temp)

        assert recommendation_define.get_recommendation_ids(None) == list(sorted(enable_ids + disenable_ids))
        assert recommendation_define.get_recommendation_names(None) == list(sorted(enable_names + disenable_names))

        assert recommendation_define.get_recommendation_ids(True) == enable_ids
        assert recommendation_define.get_recommendation_names(True) == enable_names

        assert recommendation_define.get_recommendation_ids(False) == disenable_ids
        assert recommendation_define.get_recommendation_names(False) == disenable_names

    def _test_api_delete_function_define(self):
        p = {
            'recommendation_name': 'test1',
            'recommendation_id': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        recommendation_define.add_recommendation_define(**p)
        assert recommendation_define.get_recommendation_define('test1')
        assert recommendation_define.get_recommendation_define(recommendation_id=0)
        recommendation_define.delete_recommendation_define('test1')
        assert not recommendation_define.get_recommendation_define('test1')
        assert not recommendation_define.get_recommendation_define(recommendation_id=0)

        recommendation_define.add_recommendation_define(**p)
        assert recommendation_define.get_recommendation_define('test1')
        assert recommendation_define.get_recommendation_define(recommendation_id=0)
        recommendation_define.delete_recommendation_define(recommendation_id=0)
        assert not recommendation_define.get_recommendation_define('test1')
        assert not recommendation_define.get_recommendation_define(recommendation_id=0)
