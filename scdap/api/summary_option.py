"""

@create on: 2021.04.25
"""
__all__ = ['get_option']

from typing import Optional


def get_option(tag: str, load_mode: str = 'http') -> Optional[dict]:
    """
    获取算法进程配置

    :param tag: 进程标识/算法点位编号/设备组编号
    :param load_mode: 读取的模式, http/sql/local
    :return: 获取的算法配置
    """
    if load_mode == 'http':
        from scdap import config
        from scdap.util.session import parse_router, do_request
        url = parse_router(config.SQLAPI_SERVER_URL, f'/summary-option/{tag}/')
        return do_request('get', url)
    elif load_mode == 'sql':
        from scdap.sqlapi import summary_option
        return summary_option.get_option(tag)
    else:
        from scdap.gop.func import get_summary_option
        return get_summary_option(tag, gnet=False, net_load_mode='local')


def http_post_option(tag: str, token: str = '', **kwargs):
    from scdap import config
    from scdap.util.session import parse_router, do_request
    url = parse_router(config.SQLAPI_SERVER_URL, f'/summary-option/{tag}/')
    do_request('post', url, json={'tag': tag, **kwargs}, headers={'token': token})
