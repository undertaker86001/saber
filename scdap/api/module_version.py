"""

@create on: 2021.04.20
"""
__all__ = ['get_module_version']

from typing import Optional


def get_module_version(module_name: str, load_mode: str = 'http') -> Optional[dict]:
    """
    获取模块版本信息

    :param module_name: 健康度定义名称
    :param load_mode: 读取的模式, http/sql/local
    :return: 健康度定义配置
    """
    if load_mode == 'http':
        from scdap import config
        from scdap.util.session import parse_router, do_request
        url = parse_router(config.SQLAPI_SERVER_URL, f'/module-version/{module_name}/')
        return do_request('get', url)
    elif load_mode == 'sql':
        from scdap.sqlapi import module_version
        return module_version.get_module_version(module_name)
    else:
        return None
