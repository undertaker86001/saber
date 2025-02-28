"""

@create on: 2021.02.23
"""
__all__ = [
    'get_module_version', 'get_modules',
    'update_module_version', 'upload_module_version',
    'delete_module_version', 'add_module_version',
    'remove_module_version'
]

from typing import List, Union, Optional

from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR, JSON

from .common_flag import CommonFlag
from .sqlbase import Base, TableApi, check_type, DapBaseItem


class ModuleVersionKey(CommonFlag):
    module_name = 'module_name'
    version = 'version'
    extra = 'extra'


flag = ModuleVersionKey


class ModuleVersion(Base, DapBaseItem):
    __tablename__ = 'module_version'

    module_name = Column(
        flag.module_name, VARCHAR(32), nullable=False, unique=True,
        comment='模块名称'
    )
    version = Column(flag.version, VARCHAR(20), nullable=False, comment="模块版本号")
    extra = Column(flag.extra, JSON, nullable=False, comment='额外扩展字段')


_api = TableApi(ModuleVersion)


def add_module_version(module_name: str, version: str,
                       extra: dict = None, enabled: Union[bool, int] = True, description: str = '',
                       session=None):
    """
    添加模块版本号信息


    :param module_name: 模块名称
    :param version: 模块版本号
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """
    extra = extra or dict()
    check_type(module_name, 'module_name', str)
    check_type(version, 'version', str)
    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)
    _api.add({
        flag.module_name: module_name, flag.version: version,
        flag.enabled: enabled, flag.extra: extra, flag.description: description
    }, session)


upload_module_version = add_module_version


def get_modules(enabled: Optional[bool] = None, session=None) -> List[int]:
    """
    获取所有存在的模块版本号

    :param session:
    :param enabled:
    :return: 存在的健康度标识字段
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(ModuleVersion.module_name, enabled, session=session)


def get_module_version(module_name: Union[List[str], str, None] = None, enabled: Union[bool, int] = None, session=None) \
        -> dict:
    """
    获取模块版本号信息

    :param module_name: 模块名称
    :param enabled:
    :param session:
    :return: 健康度定义列表
    """
    check_type(module_name, 'module_name', (list, str))
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.query_with_unique(ModuleVersion.module_name, module_name, enabled, session=session)


def module_version_exist(module_name: str, enabled: Union[bool, int] = None, session=None) -> bool:
    """
    模块版本号信息是否存在

    :param module_name:
    :param enabled:
    :param session:
    :return:
    """
    check_type(module_name, 'module_name', str)
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.check_exist_with_unique(ModuleVersion.module_name, module_name, session=session)


def update_module_version(module_name: str, version: str = None, extra: dict = None,
                          enabled: Union[bool, int] = None, description: str = None,
                          session=None):
    """
    更新模块版本号信息

    :param module_name: 模块名称
    :param version: 模块版本号
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """
    kv = dict()
    check_type(module_name, 'module_name', str)
    check_type(version, 'version', str, True)
    check_type(extra, 'extra', dict, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(description, 'description', str, True)

    if version is not None:
        kv[flag.version] = version
    if extra is not None:
        kv[flag.extra] = extra
    if enabled is not None:
        kv[flag.enabled] = enabled
    if description is not None:
        kv[flag.description] = description

    _api.update_with_unique(ModuleVersion.module_name, module_name, kv, session=session)


def delete_module_version(module_name: Union[List[str], str], session=None):
    """
    删除模块版本号信息

    :param module_name: 模块名称
    :param session:
    """
    _api.delete_with_unique(ModuleVersion.module_name, module_name, session=session)


remove_module_version = delete_module_version
