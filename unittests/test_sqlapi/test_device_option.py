"""

@create on: 2021.01.27
"""
import pytest

from scdap.sqlapi import device_option


class TestDeviceOptionAndHealthDefine(object):
    def setup_class(self):
        from scdap import config
        config.FUNCTION_LIB = 'unittests.function'
        from scdap.frame.function import fset
        fset.reload()

    def teardown_class(self):
        # 清理结果
        device_option._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        device_option._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                device_option._api.create_table()
                try:
                    f(self)
                finally:
                    device_option._api.drop_table()

    def get_functions(self, p):
        return [f['function'] for f in p['decision'] + p['evaluation'] + p['other']]

    def __test_device_option_api_add_and_get(self, p):
        device_option.add_option(**p)
        p = p.copy()
        p['functions'] = self.get_functions(p)
        assert device_option.get_option(p['tag']) == p

    def _test_device_option_api_add(self):
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {},
            'enabled': True,
            'description': ''
        }

        self.__test_device_option_api_add_and_get(p)

    def _test_device_option_api_add_with_error(self):
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [],
            'extra': {},
            'enabled': True,
            'description': ''
        }
        # worker 配置错误
        with pytest.raises(Exception):
            temp = p.copy()
            temp['worker'] = '???'
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['decision'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['evaluation'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['tag'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['devices'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['clock_time'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['extra'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['enabled'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

        with pytest.raises(Exception):
            temp = p.copy()
            temp['description'] = [{'hello': 1}]
            self.__test_device_option_api_add_and_get(temp)

    def _test_device_option_api_update(self):
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {},
            'enabled': True,
            'description': ''
        }
        device_option.add_option(**p)

        p = {
            'tag': 200,
            'devices': [100, 201, 202],
            'worker': 'normal_stack',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {"test": 1},
            'enabled': False,
            'description': '...'
        }

        device_option.update_option(**p)
        p['functions'] = self.get_functions(p)
        assert device_option.get_option(p['tag']) == p

    def _test_device_option_api_update_with_error(self):
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {},
            'enabled': True,
            'description': ''
        }
        device_option.add_option(**p)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'devices': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'worker': 100,
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'clock_time': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'decision': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'evaluation': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'other': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'extra': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'enabled': '',
            }
            device_option.update_option(**temp)

        with pytest.raises(Exception):
            temp = {
                'tag': 200,
                'description': 1,
            }
            device_option.update_option(**temp)

    def _test_device_option_api_exists(self):
        assert not device_option.tag_exist(200)
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {},
            'enabled': True,
            'description': ''
        }
        device_option.add_option(**p)
        assert device_option.tag_exist(200)

    def _test_device_option_api_get(self):
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {},
            'enabled': True,
            'description': ''
        }
        size = 10
        enables = []
        disenables = []

        for i in range(1, size):
            if (i % 2) == 0:
                enables.append(i)
            else:
                disenables.append(i)
            temp = p.copy()
            temp['tag'] = i
            temp['enabled'] = (i % 2) == 0
            device_option.add_option(**temp)

        assert device_option.get_tags(enabled=None) == list(sorted(enables + disenables))

        assert device_option.get_tags(enabled=True) == enables
        assert device_option.get_tags(enabled=False) == disenables

    def _test_device_option_api_delete_define(self):
        p = {
            'tag': 200,
            'devices': [100, 201],
            'worker': 'normal_realtime',
            'clock_time': 2,
            'decision': [{'function': 'decision1'}],
            'evaluation': [{'function': 'evaluation2'}],
            'other': [{'function': 'other4'}],
            'extra': {},
            'enabled': True,
            'description': ''
        }

        device_option.add_option(**p)
        assert device_option.tag_exist(p['tag'])
        device_option.delete_option(p['tag'])
        assert not device_option.tag_exist(p['tag'])
