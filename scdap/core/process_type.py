"""

@create on: 2021.05.07
"""
__process_type__ = {'program', 'summary'}


def get_process_type():
    return __process_type__


def allow_process_type(process_type: str) -> bool:
    return process_type in __process_type__


def check_process_type(process_type: str):
    if not allow_process_type(process_type):
        raise ValueError(f'错误的进程类型(process_type), 类型必须为: %s' % '/'.join(__process_type__))
