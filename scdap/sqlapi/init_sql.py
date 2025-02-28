"""

@create on: 2021.03.15
初始化所有数据库
"""
__all__ = ['initial_tables', 'reinitial_tables']


from importlib import import_module

__initial_modules__ = [
    'device_health',
    'device_option',
    'device_parameter',
    'function_define',
    'health_define',
    'module_version',
    'recommendation_define',
    'status_define',
    'worker_define',
    'event_define',
    # 'summary_parameter',
    'summary_option'
]


def initial_tables():
    for module in __initial_modules__:
        module = import_module(f'{__package__}.{module}')
        module._api.initial()
        module._api.create_table()


def reinitial_tables():
    for module in __initial_modules__:
        module = import_module(f'{__package__}.{module}')
        module._api.initial()
        module._api.drop_table()
        module._api.create_table()
