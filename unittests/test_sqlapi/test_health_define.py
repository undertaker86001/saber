"""

@create on: 2021.02.02
"""
import pytest

from scdap.sqlapi import health_define


class TestHealthDefine(object):
    def setup_class(self):
        from scdap import config
        config.FUNCTION_LIB = 'unittests.function'
        from scdap.frame.function import fset
        fset.reload()

    def teardown_class(self):
        # 清理结果
        health_define._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        health_define._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                health_define._api.create_table()
                try:
                    f(self)
                finally:
                    health_define._api.drop_table()

    def __test_health_define_api_add_and_get(self, p):
        health_define.add_health_define(**p)
        p = p.copy()
        assert health_define.get_health_define(p['health_name']) == p

    def _test_health_define_api_add_hid_self(self):
        # 校验health_id自增
        p = {
            'health_name': 'test1',
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        health_define.add_health_define(**p)
        temp = p.copy()
        temp['health_id'] = 0
        assert health_define.get_health_define(p['health_name']) == temp

        p['health_name'] = 'test2'
        health_define.add_health_define(**p)
        temp = p.copy()
        temp['health_id'] = 1
        assert health_define.get_health_define(p['health_name']) == temp

    def _test_health_define_api_add(self):
        total_hs = dict()
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        total_hs[p['health_name']] = p.copy()
        self.__test_health_define_api_add_and_get(p)

        p['health_name'] = 'test2'
        p['health_id'] = 1
        p['rt_cn_name'] = 'rt_cn_name2'
        p['rt_en_name'] = 'rt_cn_name2'
        total_hs[p['health_name']] = p.copy()
        self.__test_health_define_api_add_and_get(p)

        p['health_name'] = 'test3'
        p['health_id'] = 2
        p['secular_cn_name'] = 'secular_cn_name3'
        p['secular_en_name'] = 'secular_cn_name3'
        total_hs[p['health_name']] = p.copy()
        self.__test_health_define_api_add_and_get(p)

        p['health_name'] = 'test4'
        p['health_id'] = 3
        p['default_score'] = 10
        p['reverse'] = True
        total_hs[p['health_name']] = p.copy()
        self.__test_health_define_api_add_and_get(p)

        assert health_define.get_health_define(list(total_hs.keys())) == total_hs

    def _test_health_define_api_add_with_error(self):
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        health_define.add_health_define(**p)

        # 不允许反复添加数据
        with pytest.raises(Exception):
            health_define.add_health_define(**p)

        p['health_name'] = 'test2'

        with pytest.raises(Exception):
            temp = p.copy()
            temp['reverse'] = '0'
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['rt_cn_name'] = 1
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['rt_en_name'] = 1
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['secular_cn_name'] = 1
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['secular_en_name'] = 1
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['cn_alarm_topic'] = 1
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['en_alarm_topic'] = 1
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['default_score'] = '11'
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['extra'] = '11'
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['enabled'] = '11'
            health_define.add_health_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['description'] = 111
            health_define.add_health_define(**temp)

    def _test_health_define_api_update(self):
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        health_define.add_health_define(**p)
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': True,
            'rt_cn_name': 'rt_cn_name1',
            'rt_en_name': 'rt_en_name1',
            'secular_cn_name': 'secular_cn_name1',
            'secular_en_name': 'secular_en_name1',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 80,
            'extra': {'test': 2},
            'enabled': False,
            'description': 'desc11'
        }
        health_define.update_health_define(**p)
        assert health_define.get_health_define(p['health_name']) == p
        assert health_define.get_health_define(health_id=p['health_id']) == p

    def _test_health_define_api_update_with_error(self):
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        health_define.add_health_define(**p)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'reverse': 'decision1'
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'rt_cn_name': 111
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'rt_en_name': 111
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'secular_cn_name': 111
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'secular_en_name': 111
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'default_score': '11'
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'extra': '11'
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'enabled': '11'
            }
            health_define.update_health_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'health_name': 'test1',
                'description': 11
            }
            health_define.update_health_define(**temp)

        assert health_define.get_health_define(p['health_name']) == p
        assert health_define.get_health_define(health_id=p['health_id']) == p

    def _test_health_define_api_health_define_exist(self):
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        health_define.add_health_define(**p)

        assert health_define.health_name_exist(p['health_name'])
        assert not health_define.health_name_exist(p['health_name'] + 'sss')
        assert health_define.health_name_exist(health_id=p['health_id'])
        assert not health_define.health_name_exist(health_id=p['health_id'] + 100)

    def _test_health_define_api_get_functions(self):
        size = 10
        enable_ids = []
        enable_names = []
        disenable_ids = []
        disenable_names = []
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
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
            temp['health_name'] = fname
            temp['health_id'] = i
            temp['enabled'] = (i % 2) == 0
            health_define.add_health_define(**temp)

        assert health_define.get_health_ids(None) == list(sorted(enable_ids + disenable_ids))
        assert health_define.get_health_names(None) == list(sorted(enable_names + disenable_names))

        assert health_define.get_health_ids(True) == enable_ids
        assert health_define.get_health_names(True) == enable_names

        assert health_define.get_health_ids(False) == disenable_ids
        assert health_define.get_health_names(False) == disenable_names

    def _test_health_define_api_delete_health_define(self):
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        health_define.add_health_define(**p)
        assert health_define.get_health_define('test1')
        assert health_define.get_health_define(health_id=0)
        health_define.delete_health_define('test1')
        assert not health_define.get_health_define('test1')
        assert not health_define.get_health_define(health_id=0)

        health_define.add_health_define(**p)
        assert health_define.get_health_define('test1')
        assert health_define.get_health_define(health_id=0)
        health_define.delete_health_define(health_id=0)
        assert not health_define.get_health_define('test1')
        assert not health_define.get_health_define(health_id=0)

    def _test_health_define_api_get_id_from_name(self):
        p = {
            'health_name': 'test1',
            'health_id': 0,
            'reverse': False,
            'rt_cn_name': 'rt_cn_name',
            'rt_en_name': 'rt_en_name',
            'secular_cn_name': 'secular_cn_name',
            'secular_en_name': 'secular_en_name',
            'cn_alarm_topic': 'cn_alarm_topic',
            'en_alarm_topic': 'en_alarm_topic',
            'default_score': 90,
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        size = 10
        for i in range(0, size):
            fname = f'test{i}'
            temp = p.copy()
            temp['health_name'] = fname
            temp['health_id'] = i
            health_define.add_health_define(**temp)
        assert health_define.get_health_id_from_name([f'test{i}' for i in range(size)]) == list(range(size))

        with pytest.raises(Exception):
            health_define.get_health_id_from_name([f'test{i}' for i in range(size + 1)])
