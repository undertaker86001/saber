"""

@create on: 2021.01.26
"""
import pytest

from scdap.sqlapi import function_define


class TestFunctionDefine(object):
    def teardown_class(self):
        # 清理结果
        function_define._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        function_define._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                function_define._api.create_table()
                try:
                    f(self)
                finally:
                    function_define._api.drop_table()

    def __test_api_add_and_get(self, p):
        function_define.add_function_define(**p)
        assert function_define.get_function_define(p['function_name']) == p
        assert function_define.get_function_define(function_id=p['function_id']) == p

    def _test_api_add(self):
        total_ids = dict()
        total_fnames = dict()
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc',
            'health_define_name': []
        }

        total_ids[p['function_id']] = p.copy()
        total_fnames[p['function_name']] = p.copy()
        self.__test_api_add_and_get(p)

        p['function_id'] = 2
        p['function_name'] = 'test2'
        p['function_type'] = 'evaluation'
        total_ids[p['function_id']] = p.copy()
        total_fnames[p['function_name']] = p.copy()
        self.__test_api_add_and_get(p)

        p['function_id'] = 3
        p['function_name'] = 'test3'
        p['function_type'] = 'other'
        total_ids[p['function_id']] = p.copy()
        total_fnames[p['function_name']] = p.copy()
        self.__test_api_add_and_get(p)

        p['function_id'] = 4
        p['function_name'] = 'test4'
        p['function_type'] = 'other'
        total_ids[p['function_id']] = p.copy()
        total_fnames[p['function_name']] = p.copy()
        self.__test_api_add_and_get(p)
        assert function_define.get_function_define(list(total_fnames.keys())) == total_fnames
        assert function_define.get_function_define(function_id=list(total_ids.keys())) == total_ids

        p['function_id'] = None
        p['function_name'] = 'test5'
        p['function_type'] = 'other'
        function_define.add_function_define(**p)
        p['function_id'] = 5
        assert function_define.get_function_define(p['function_name']) == p
        assert function_define.get_function_define(function_id=p['function_id']) == p

    def _test_api_add_with_error(self):
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }

        function_define.add_function_define(**p)

        # 不允许反复添加数据
        with pytest.raises(Exception):
            function_define.add_function_define(**p)
        p['function_name'] = 'test2'
        p['function_id'] = 2

        # 不允许添加错误编号
        with pytest.raises(Exception):
            temp = p.copy()
            temp['function_id'] = 3
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['function_type'] = '???'
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['function_type'] = 1
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['author'] = 1
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['md5_code'] = 1
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['version'] = 1
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['extra'] = 1
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['enabled'] = '1'
            function_define.add_function_define(**temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['description'] = 1
            function_define.add_function_define(**temp)

    def _test_api_update(self):
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc'
        }
        function_define.add_function_define(**p)
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'evaluation',
            'author': 'author2',
            'md5_code': 'test2',
            'version': '1.0.1',
            'extra': {'test': 2},
            'enabled': False,
            'description': 'desc111',
            'health_define_name': ['trend']
        }
        function_define.update_function_define(**p)
        assert function_define.get_function_define(p['function_name']) == p
        assert function_define.get_function_define(function_id=p['function_id']) == p

    def _test_api_update_with_error(self):
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc',
            'health_define_name': []
        }
        function_define.add_function_define(**p)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'function_type': 'decision1'
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'function_type': 111
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'author': 111
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'md5_code': 111
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'version': 111
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'extra': 11
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'enabled': '11'
            }
            function_define.update_function_define(**temp)

        with pytest.raises(Exception):
            temp = {
                'function_name': 'test1',
                'description': 11
            }
            function_define.update_function_define(**temp)

        assert function_define.get_function_define(p['function_name']) == p
        assert function_define.get_function_define(function_id=p['function_id']) == p

    def _test_api_function_define_exist(self):
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc',
            'health_define_name': []
        }
        function_define.add_function_define(**p)

        assert function_define.function_define_exist('test1')
        assert function_define.function_define_exist(function_id=1)

        assert not function_define.function_define_exist('test10')
        assert not function_define.function_define_exist(function_id=10)

    def _test_api_get_functions(self):
        size = 10
        enable_fids = []
        enable_fnames = []
        disenable_fids = []
        disenable_fnames = []

        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc',
            'health_define_name': []
        }

        for i in range(1, size):
            fname = f'test{i}'
            if (i % 2) == 0:
                enable_fids.append(i)
                enable_fnames.append(fname)
            else:
                disenable_fids.append(i)
                disenable_fnames.append(fname)
            temp = p.copy()
            temp['function_name'] = fname
            temp['function_id'] = i
            temp['enabled'] = (i % 2) == 0
            function_define.add_function_define(**temp)

        assert function_define.get_function_ids(None) == list(sorted(enable_fids + disenable_fids))
        assert function_define.get_function_names(None) == list(sorted(enable_fnames + disenable_fnames))

        assert function_define.get_function_ids(True) == enable_fids
        assert function_define.get_function_names(True) == enable_fnames

        assert function_define.get_function_ids(False) == disenable_fids
        assert function_define.get_function_names(False) == disenable_fnames

    def _test_api_delete_function_define(self):
        p = {
            'function_name': 'test1',
            'function_id': 1,
            'function_type': 'decision',
            'author': 'author',
            'md5_code': 'test',
            'version': '1.0.0',
            'extra': {'test': 1},
            'enabled': True,
            'description': 'desc',
            'health_define_name': []
        }

        function_define.add_function_define(**p)
        assert function_define.get_function_define('test1')
        assert function_define.get_function_define(function_id=1)
        function_define.delete_function_define('test1')
        assert not function_define.get_function_define('test1')
        assert not function_define.get_function_define(function_id=1)
