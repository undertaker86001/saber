"""

@create on: 2021.01.28
"""
from datetime import datetime

from scdap.sqlapi import device_parameter


class TestDeviceParameter(object):
    def teardown_class(self):
        # 清理结果
        device_parameter._api.drop_table()

    def test_api(self):
        # 数据库操作需要同步运行
        # 需要测试的数据库的名称标记为
        device_parameter._api.initial()
        for fname, f in type(self).__dict__.items():
            if fname.startswith('_test_'):
                device_parameter._api.create_table()
                try:
                    f(self)
                finally:
                    device_parameter._api.drop_table()

    def _test_api_add(self):
        all_p = list()
        p = {
            'algorithm_id': 1,
            'function_id': 1,
            'parameter': {'test': 1},
            'effective_start': datetime(2020, 1, 1),
            'effective_stop': datetime(2020, 1, 1),
            'enabled': True,
            'reference': {'test': 1},
            'description': '...'
        }
        device_parameter.add_parameter(**p)
        assert device_parameter.get_last_parameter(1, 1) == p
        all_p.append(p)

        p = p.copy()
        p['parameter'] = {'test': 1, 'val': [1, 2, 3, 4]}
        device_parameter.add_parameter(**p)
        assert device_parameter.get_last_parameter(1, 1) == p

        all_p.append(p)
        assert device_parameter.get_parameter(1, 1) == list(reversed(all_p))
