"""

@create on: 2020.12.08
"""
__all__ = [
    'add_status_define', 'update_status_define', 'upload_status_define', 'status_define_exist',
    'remove_status_define', 'delete_status_define', 'get_status_define', 'get_status_names'
]

from typing import Union, List

from sqlalchemy import Column, JSON
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class StatusDefineKey(CommonFlag):
    status_name = 'status_name'
    status_code = 'status_code'
    cn_name = 'cn_name'
    en_name = 'en_name'
    extra = 'extra'


flag = StatusDefineKey


class StatusDefine(Base, DapBaseItem):
    __tablename__ = 'status_define'
    status_name = Column(flag.status_name, VARCHAR(32), nullable=False, unique=True, comment='状态标识, 不作为展示的依据')
    status_code = Column(flag.status_code, INTEGER(10), nullable=False, unique=True, comment='状态编号')
    cn_name = Column(flag.cn_name, VARCHAR(32), nullable=False, comment='中文状态名称')
    en_name = Column(flag.en_name, VARCHAR(32), nullable=False, comment='英文状态名称')
    extra = Column(flag.extra, JSON, nullable=False, comment='其他新增字段')


_api = TableApi(StatusDefine)


def get_max_status_code() -> int:
    return _api.get_max_unique_id(StatusDefine.status_code, -1)


def add_status_define(status_name: str, cn_name: str, en_name: str, status_code: int = None,
                      extra: dict = None, enabled: Union[bool, int] = True, description: str = '', session=None) -> int:
    """
    添加新的状态

    :param status_name: 状态名称标识
    :param cn_name: 中文状态名称
    :param en_name: 英文状态名称
    :param status_code: 状态编号
    :param extra: 其他新增字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    :return: 状态编号
    """
    extra = extra or dict()

    check_type(status_name, 'status_name', str)
    check_type(cn_name, 'cn_name', str)
    check_type(en_name, 'en_name', str)
    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)

    status_code = status_code or (get_max_status_code() + 1)
    check_type(status_code, 'status_code', int)
    _api.add({
        flag.status_name: status_name, flag.cn_name: cn_name, flag.en_name: en_name, flag.status_code: status_code,
        flag.extra: extra, flag.enabled: enabled, flag.description: description
    }, session=session)
    return status_code


upload_status_define = add_status_define


def status_define_exist(status_name: str = None, status_code: int = None, enabled: Union[bool, int] = None, session=None) -> bool:
    """
    确认状态是否存在

    :param status_name: 状态标签
    :param status_code: 状态编号
    :param enabled:
    :param session:
    :return: 是否存在
    """
    check_type(status_name, 'status_name', str, True)
    check_type(status_code, 'status_code', int, True)
    check_type(enabled, 'enabled', (bool, int), True)

    if status_name is not None:
        return _api.check_exist_with_unique(StatusDefine.status_name, status_name, session=session)
    elif status_code is not None:
        return _api.check_exist_with_unique(StatusDefine.status_code, status_code, session=session)
    else:
        raise ValueError(f'参数 status_name 与 status_code 必须有一个不为None.')


def get_status_codes(enabled: Union[bool, int] = None, session=None) -> List[int]:
    """
    获取所有存在的status_code

    :param enabled: 是否获取启用的配置
    :return: status_code列表
    :param session:
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(StatusDefine.status_code, enabled, session=session)


def get_status_names(enabled: Union[bool, int] = None, session=None) -> List[str]:
    """
    获取所有存在的status_name

    :param enabled: 是否获取启用的配置
    :return: status_name列表
    :param session:
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(StatusDefine.status_name, enabled, session=session)


def get_status_define(status_name: Union[List[str], str, None] = None,
                      status_code: Union[List[int], int, None] = None,
                      enabled: Union[bool, int] = None, session=None) -> dict:
    """
    获取所有存在的状态

    :param status_name: 状态标识
    :param status_code: 状态编号
    :param enabled: 是否只获取启动的定义
    :param session:
    :return: 状态定义列表
    """
    check_type(status_name, 'status_name', (str, list, tuple), True)
    check_type(status_code, 'status_code', (int, list, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    if status_code is not None:
        return _api.query_with_unique(StatusDefine.status_code, status_code, enabled, session=session)
    elif status_name is not None:
        return _api.query_with_unique(StatusDefine.status_name, status_name, enabled, session=session)
    else:
        raise ValueError(f'参数 status_name 与 status_code 必须有一个不为None.')


def update_status_define(status_name: str = None, status_code: int = None,
                         cn_name: str = None, en_name: str = None, extra: dict = None,
                         enabled: Union[bool, int] = None, description: str = None, session=None):
    """
    更新状态

    :param status_name: 状态名称标识
    :param status_code: 状态编号
    :param cn_name: 中文状态名称
    :param en_name: 英文状态名称
    :param extra: 其他新增字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """
    check_type(status_name, 'status_name', str, True)
    check_type(status_code, 'status_code', int, True)
    check_type(cn_name, 'cn_name', str, True)
    check_type(en_name, 'en_name', str, True)
    check_type(extra, 'extra', dict, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(description, 'description', str, True)
    kv = dict()
    if cn_name is not None:
        kv[flag.cn_name] = cn_name
    if en_name is not None:
        kv[flag.en_name] = en_name
    if extra is not None:
        kv[flag.extra] = extra
    if enabled is not None:
        kv[flag.enabled] = enabled
    if description is not None:
        kv[flag.description] = description

    if status_name is not None:
        return _api.update_with_unique(StatusDefine.status_name, status_name, kv, session=session)
    elif status_code is not None:
        return _api.update_with_unique(StatusDefine.status_code, status_code, kv, session=session)
    else:
        raise ValueError(f'参数 status_name 与 status_code 必须有一个不为None.')


def delete_status_define(status_name: Union[List[str], str] = None, status_code: Union[List[int], int] = None
                         , session=None):
    """
    删除状态

    :param status_name: 状态标识
    :param status_code: 状态编号
    :param session:
    """
    check_type(status_name, 'status_name', str, True)
    check_type(status_code, 'status_code', int, True)

    if status_name is not None:
        return _api.delete_with_unique(StatusDefine.status_name, status_name, session=session)
    elif status_code is not None:
        return _api.delete_with_unique(StatusDefine.status_code, status_code, session=session)
    else:
        raise ValueError(f'参数 status_name 与 status_code 必须有一个不为None.')


remove_status_define = delete_status_define
