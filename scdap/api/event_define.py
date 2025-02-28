"""

@create on: 2021.05.10
"""
__all__ = ['get_event_define']

from typing import Optional


def get_event_define(event_type: int, event_code: int, load_mode: str = 'http') -> Optional[dict]:
    """
    获取事件定义

    :param event_type:
    :param event_code:
    :param load_mode: 读取的模式, http/sql/local
    :return: 健康度定义配置
    """
    if load_mode == 'http':
        from scdap import config
        from scdap.util.session import parse_router, do_request
        url = parse_router(config.SQLAPI_SERVER_URL, f'/event-define/{event_type}:{event_code}/')
        return do_request('get', url)
    elif load_mode == 'sql':
        from scdap.sqlapi import event_define
        return event_define.get_event_define(event_type, event_code)
    else:
        from scdap_algorithm.flag_detail.event_define import get_event_info
        return get_event_info(event_type, event_code)
