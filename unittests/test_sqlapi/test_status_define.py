"""

@create on: 2021.01.27
"""
import pytest

from scdap.sqlapi import status_define


class TestStatusDefine(object):
    def teardown_class(self):
        # 清理结果
        status_define._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        status_define._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                status_define._api.create_table()
                try:
                    f(self)
                finally:
                    status_define._api.drop_table()

    def __test_api_add_and_get(self, p):
        status_define.add_status_define(**p)
        p = p.copy()
        assert status_define.get_status_define(p['status_name']) == p

    def _test_api_add_hid_self(self):
        # 校验id自增
        p = {
            'status_name': 'test1',
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        status_define.add_status_define(**p)
        temp = p.copy()
        temp['status_code'] = 0
        assert status_define.get_status_define(p['status_name']) == temp

        p['status_name'] = 'test2'
        status_define.add_status_define(**p)
        temp = p.copy()
        temp['status_code'] = 1
        assert status_define.get_status_define(p['status_name']) == temp

    def _test_api_add(self):
        total_hs = dict()
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        total_hs[p['status_name']] = p.copy()
        self.__test_api_add_and_get(p)

        p['status_name'] = 'test2'
        p['status_code'] = 1
        p['cn_name'] = 'rt_cn_name2'
        p['en_name'] = 'rt_cn_name2'
        total_hs[p['status_name']] = p.copy()
        self.__test_api_add_and_get(p)

        assert status_define.get_status_define(list(total_hs.keys())) == total_hs

    def _test_api_add_with_error(self):
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        status_define.add_status_define(**p)

        # 不允许反复添加数据
        with pytest.raises(Exception):
            status_define.add_status_define(**p)

        p['status_name'] = 'test2'

        with pytest.raises(Exception):
            temp = p.copy()
            temp['status_code'] = '1'
            status_define.add_status_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['cn_name'] = 1
            status_define.add_status_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['extra'] = '11'
            status_define.add_status_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['enabled'] = '11'
            status_define.add_status_define(
                **temp
            )

        with pytest.raises(Exception):
            temp = p.copy()
            temp['description'] = 111
            status_define.add_status_define(
                **temp
            )

    def _test_api_update(self):
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        status_define.add_status_define(**p)
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name1',
            'en_name': 'en_name1',
            'extra': {'test': 11},
            'enabled': False,
            'description': 'desc1'
        }
        status_define.update_status_define(**p)
        assert status_define.get_status_define(p['status_name']) == p
        assert status_define.get_status_define(status_code=p['status_code']) == p

    def _test_api_update_with_error(self):
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        status_define.add_status_define(**p)

        with pytest.raises(Exception):
            temp = {
                'status_name': 'test1',
                'cn_name': 1
            }
            status_define.update_status_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'status_name': 'test1',
                'en_name': 1
            }
            status_define.update_status_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'status_name': 'test1',
                'extra': '11'
            }
            status_define.update_status_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'status_name': 'test1',
                'enabled': '11'
            }
            status_define.update_status_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'status_name': 'test1',
                'description': 11
            }
            status_define.update_status_define(**temp)

        assert status_define.get_status_define(p['status_name']) == p
        assert status_define.get_status_define(status_code=p['status_code']) == p

    def _test_api_function_define_exist(self):
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        status_define.add_status_define(**p)

        assert status_define.status_define_exist(p['status_name'])
        assert not status_define.status_define_exist(p['status_name'] + 'sss')
        assert status_define.status_define_exist(status_code=p['status_code'])
        assert not status_define.status_define_exist(status_code=p['status_code'] + 100)

    def _test_api_get_functions(self):
        size = 10
        enable_ids = []
        enable_names = []
        disenable_ids = []
        disenable_names = []
        p = {
            'status_name': 'test1',
            'status_code': 0,
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
            temp['status_name'] = fname
            temp['status_code'] = i
            temp['enabled'] = (i % 2) == 0
            status_define.add_status_define(**temp)

        assert status_define.get_status_codes(None) == list(sorted(enable_ids + disenable_ids))
        assert status_define.get_status_names(None) == list(sorted(enable_names + disenable_names))

        assert status_define.get_status_codes(True) == enable_ids
        assert status_define.get_status_names(True) == enable_names

        assert status_define.get_status_codes(False) == disenable_ids
        assert status_define.get_status_names(False) == disenable_names

    def _test_api_delete_function_define(self):
        p = {
            'status_name': 'test1',
            'status_code': 0,
            'cn_name': 'cn_name',
            'en_name': 'en_name',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        status_define.add_status_define(**p)
        assert status_define.get_status_define('test1')
        assert status_define.get_status_define(status_code=0)
        status_define.delete_status_define('test1')
        assert not status_define.get_status_define('test1')
        assert not status_define.get_status_define(status_code=0)

        status_define.add_status_define(**p)
        assert status_define.get_status_define('test1')
        assert status_define.get_status_define(status_code=0)
        status_define.delete_status_define(status_code=0)
        assert not status_define.get_status_define('test1')
        assert not status_define.get_status_define(status_code=0)
