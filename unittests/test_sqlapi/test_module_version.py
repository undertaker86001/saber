"""

@create on: 2021.02.23
"""
import pytest

from scdap.sqlapi import module_version


class TestModuleVersion(object):
    def setup_class(self):
        from scdap import config
        config.FUNCTION_LIB = 'unittests.function'
        from scdap.frame.function import fset
        fset.reload()

    def teardown_class(self):
        # 清理结果
        module_version._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        module_version._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                module_version._api.create_table()
                try:
                    f(self)
                finally:
                    module_version._api.drop_table()

    def __test_module_version_api_add_and_get(self, p):
        module_version.add_module_version(**p)
        p = p.copy()
        assert module_version.get_module_version(p['module_name']) == p

    def _test_module_version_api_add(self):
        p = {
            'module_name': 'core',
            'version': '1',
            'extra': {},
            'enabled': True,
            'description': ''
        }

        self.__test_module_version_api_add_and_get(p)

    def _test_module_version_api_add_with_error(self):
        p = {
            'module_name': 'core',
            'version': '1',
            'extra': {},
            'enabled': True,
            'description': ''
        }

        with pytest.raises(Exception):
            temp = p.copy()
            temp['module_name'] = 1
            self.__test_module_version_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['version'] = 1
            self.__test_module_version_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['extra'] = [{'hello': 1}]
            self.__test_module_version_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['enabled'] = [{'hello': 1}]
            self.__test_module_version_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['description'] = [{'hello': 1}]
            self.__test_module_version_api_add_and_get(temp)

    def _test_module_version_api_update(self):
        p = {
            'module_name': 'core',
            'version': '1',
            'extra': {},
            'enabled': True,
            'description': ''
        }
        module_version.add_module_version(**p)

        p = {
            'module_name': 'core',
            'version': '10',
            'extra': {"module_name": 111},
            'enabled': False,
            'description': '1111'
        }

        module_version.update_module_version(**p)
        assert module_version.get_module_version(p['module_name']) == p

    def _test_module_version_api_update_with_error(self):
        p = {
            'module_name': 'core',
            'version': '10',
            'extra': {"module_name": 111},
            'enabled': True,
            'description': '1111'
        }
        module_version.add_module_version(**p)

        with pytest.raises(Exception):
            temp = {
                'module_name': 'core',
                'version': 1,
            }
            module_version.update_module_version(**temp)

        with pytest.raises(Exception):
            temp = {
                'module_name': 'core',
                'extra': '',
            }
            module_version.update_module_version(**temp)

        with pytest.raises(Exception):
            temp = {
                'module_name': 'core',
                'enabled': '',
            }
            module_version.update_module_version(**temp)

        with pytest.raises(Exception):
            temp = {
                'module_name': 'core',
                'description': 1,
            }
            module_version.update_module_version(**temp)

    def _test_module_version_api_exists(self):
        assert not module_version.module_version_exist('module_name')
        p = {
            'module_name': 'core',
            'version': '10',
            'extra': {"module_name": 111},
            'enabled': True,
            'description': '1111'
        }
        module_version.add_module_version(**p)
        assert module_version.module_version_exist('core')

    def _test_module_version_api_get(self):
        p = {
            'module_name': 'core',
            'version': '10',
            'extra': {"module_name": 111},
            'enabled': True,
            'description': '1111'
        }
        size = 10
        enables = []
        disenables = []

        for i in range(1, size):
            module_name = f'core.{i}'
            if (i % 2) == 0:
                enables.append(module_name)
            else:
                disenables.append(module_name)
            temp = p.copy()
            temp['module_name'] = module_name
            temp['enabled'] = (i % 2) == 0
            module_version.add_module_version(**temp)

        assert module_version.get_modules(None) == list(sorted(enables + disenables))

        assert module_version.get_modules(True) == enables
        assert module_version.get_modules(False) == disenables

    def _test_module_version_api_delete_define(self):
        p = {
            'module_name': 'core',
            'version': '10',
            'extra': {"module_name": 111},
            'enabled': True,
            'description': '1111'
        }

        module_version.add_module_version(**p)
        assert module_version.module_version_exist(p['module_name'])
        module_version.delete_module_version(p['module_name'])
        assert not module_version.module_version_exist(p['module_name'])
