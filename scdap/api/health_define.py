"""

@create on: 2021.04.19
获取健康度相关的信息
基本上在scdap_algorithm.flag_detail.health_define中配置后，取出来的就一样
"""
__all__ = ['get_health_define']

from typing import Optional
from ._cache import cache_function


def get_health_define(health_name: str, load_mode: str = 'http') -> Optional[dict]:
    """
    获取健康度定义配置

    :param health_name: 健康度定义名称
    :param load_mode: 读取的模式, http/sql/local
    :return: 健康度定义配置
    """
    if load_mode == 'http':
        from scdap import config
        from scdap.util.session import parse_router, do_request
        url = parse_router(config.SQLAPI_SERVER_URL, f'/health-define/{health_name}/')
        return do_request('get', url)
    elif load_mode == 'sql':
        from scdap.sqlapi import health_define
        return health_define.get_health_define(health_name)
    else:
        from scdap_algorithm.flag_detail.health_define import health_define_kv
        return health_define_kv.get(health_name)


__cache__ = dict()


def from_cache(health_name: str, load_mode: str = 'http') -> Optional[dict]:
    return cache_function(__cache__, get_health_define, health_name, health_name, load_mode)
