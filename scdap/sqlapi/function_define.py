"""

@create on: 2020.12.08
"""
__all__ = [
    'get_function_ids', 'get_function_names', 'get_function_define',
    'add_function_define', 'update_function_define', 'upload_function_define',
    'remove_function_define', 'delete_function_define', 'function_define_exist',
    'get_max_function_id'
]

from typing import Union, List

from sqlalchemy import Column, JSON
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT

from scdap.frame.function import fset
from scdap.util.parser import parser_id

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class FunctionDefineKey(CommonFlag):
    function_name = 'function_name'
    function_id = 'function_id'
    function_type = 'function_type'
    health_define_name = 'health_define_name'
    md5_code = 'md5_code'
    author = 'author'
    version = 'version'
    extra = 'extra'


flag = FunctionDefineKey


class FunctionDefine(Base, DapBaseItem):
    __tablename__ = 'function_define'
    __table_args__ = {'comment': 'function_name命名规则: 英文名称+function_id'}

    function_name = Column(flag.function_name, VARCHAR(32), nullable=False, unique=True, comment='算法唯一标识/名称')
    function_id = Column(flag.function_id, BIGINT(20), nullable=False, unique=True, comment='算法编号')
    function_type = Column(flag.function_type, VARCHAR(32), nullable=False, comment='算法类型')
    md5_code = Column(flag.md5_code, VARCHAR(32), nullable=False, comment='算法源码md5码用于比对算法是否修改')
    health_define_name = Column(flag.health_define_name, JSON, nullable=False, comment="健康编号列表")
    author = Column(flag.author, VARCHAR(32), nullable=False, comment='作者,多人以","隔开')
    version = Column(flag.version, VARCHAR(32), nullable=False, comment='版本号')
    extra = Column(flag.extra, JSON, nullable=False, comment='额外扩展字段')


_api = TableApi(FunctionDefine)


def get_max_function_id() -> int:
    return _api.get_max_unique_id(FunctionDefine.function_id)


def add_function_define(function_name: str, function_type: str, md5_code: str,
                        author: str, version: str, function_id: int = None, health_define_name: list = None,
                        extra: dict = None, enabled: Union[bool, int] = True, description: str = '',
                        force: bool = False, session=None) -> int:
    """
    新增算法

    :param function_name: 算法唯一标识/名称
    :param function_type: 算法类型
    :param md5_code: 算法源码的md5码
    :param author: 作者,多人以","隔开
    :param version: 版本号
    :param function_id: 算法编号, 默认由function_name解析而来
    :param health_define_name: 算法健康度列表
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param force: 是否强制新增算法而忽视编号顺序
    :param session:
    :return: 算法编号
    """

    extra = extra or dict()
    health_define_name = health_define_name or list()
    check_type(function_name, 'function_name', str)
    check_type(function_type, 'function_type', str)
    check_type(md5_code, 'md5_code', str)
    check_type(author, 'author', str)
    check_type(version, 'version', str)
    check_type(health_define_name, 'health_define_name', list)
    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)

    pid = parser_id(function_name)
    if function_id is None:
        function_id = pid
    elif function_id != pid:
        raise Exception(f'算法编号: {function_id} 配置错误, 根据 {function_name} 数值必须为: {pid}')

    check_type(function_id, 'function_id', int)

    max_id = get_max_function_id()
    if not force and function_id <= max_id:
        raise ValueError(f'算法名称: {function_name} 中配置的算法编号必须大于: {max_id}.')

    if function_type not in fset.get_function_types():
        raise ValueError(f'function_type 只允许配置如下内容: {list(fset.get_function_types())}')

    _api.add({
        flag.function_name: function_name, flag.function_type: function_type, flag.function_id: function_id,
        flag.author: author, flag.version: version, flag.md5_code: md5_code,
        flag.health_define_name: health_define_name,
        flag.extra: extra, flag.enabled: enabled, flag.description: description,
    }, session=session)
    return function_id


upload_function_define = add_function_define


def function_define_exist(function_name: Union[List[str], str] = None, function_id: Union[List[int], int] = None,
                          last_one: bool = False, enabled: Union[bool, int] = None, session=None) -> bool:
    """
    是否存在指定的算法标识或者编号, 两个参数必须选择一个输入

    :param function_name: 算法唯一标识/名称
    :param function_id: 算法编号
    :param enabled:
    :param last_one: True->只要一个存在就返回True, False->只有所有点位都存在才返回True
    :param session:
    :return: 是否存在
    """
    check_type(function_name, 'function_name', (list, str), True)
    check_type(function_id, 'function_id', (list, int), True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(last_one, 'last_one', bool)
    if function_name:
        return _api.check_exist_with_unique(FunctionDefine.function_name, function_name, enabled,
                                            last_one, session=session)
    elif function_id:
        return _api.check_exist_with_unique(FunctionDefine.function_id, function_id, enabled,
                                            last_one, session=session)
    else:
        raise ValueError(f'参数 function_name 与 function_id 必须有一个不为None.')


def get_function_ids(enabled: Union[bool, int] = None, session=None) -> List[int]:
    """
    获取所有存在的function_id

    :param enabled: 是否获取启用的配置
    :param session:
    :return: function_id列表
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(FunctionDefine.function_id, enabled=enabled,
                                           order_by_list=[FunctionDefine.function_id], session=session)


def get_function_names(enabled: Union[bool, int] = None, function_types: str = None, session=None) -> List[str]:
    """
    获取所有存在的function_name

    :param enabled: 是否获取启用的配置
    :param function_types: 算法类型
    :param session:
    :return: function_name列表
    """
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(function_types, 'function_types', str, True)

    extra_filter_list = {FunctionDefine.function_type == function_types} if function_types else None
    return _api.get_exist_keys_with_unique(FunctionDefine.function_name, enabled, extra_filter_list,
                                           order_by_list=[FunctionDefine.function_id], session=session)


def get_function_define(function_name: Union[List[str], str, None] = None,
                        function_id: Union[List[int], int, None] = None,
                        enabled: Union[bool, int] = None, session=None) -> dict:
    """
    获取指定的算法定义

    :param function_name: 算法唯一标识/名称
    :param function_id: 算法编号
    :param enabled: 是否只获取启动的定义
    :param session:
    :return: 算法定义列表
    """
    check_type(function_name, 'function_name', (str, list, tuple), True)
    check_type(function_id, 'function_id', (int, list, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    if function_id is not None:
        return _api.query_with_unique(FunctionDefine.function_id, function_id, enabled,
                                      order_by_list=[FunctionDefine.function_id], session=session)
    elif function_name is not None:
        return _api.query_with_unique(FunctionDefine.function_name, function_name, enabled,
                                      order_by_list=[FunctionDefine.function_id], session=session)
    else:
        return _api.query_with_unique(FunctionDefine.function_id, None, enabled,
                                      order_by_list=[FunctionDefine.function_id], session=session)


def update_function_define(function_name: str = None, function_id: int = None, function_type: str = None,
                           md5_code: str = None, author: str = None, version: str = None,
                           health_define_name: list = None, extra: dict = None,
                           enabled: Union[int, bool] = None, description: str = None, force: bool = False,
                           session=None):
    """
    新增算法

    :param function_name: 算法唯一标识/名称
    :param function_id: 算法编号
    :param function_type: 算法类型
    :param md5_code: 算法源码的md5码
    :param author: 作者,多人以","隔开
    :param version: 版本号
    :param extra: 额外扩展字段
    :param health_define_name: 算法健康度编号列表
    :param enabled: 是否启用
    :param description: 备注与描述
    :param force: 无用
    :param session:
    """
    check_type(function_name, 'function_name', str, True)
    check_type(function_id, 'function_id', int, True)
    check_type(function_type, 'function_type', str, True)
    check_type(md5_code, 'md5_code', str, True)
    check_type(author, 'author', str, True)
    check_type(version, 'version', str, True)
    check_type(health_define_name, 'health_define_name', list, True)
    check_type(extra, 'extra', dict, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(description, 'description', str, True)

    kv = dict()
    if function_type is not None:
        kv[flag.function_type] = function_type
    if md5_code is not None:
        kv[flag.md5_code] = md5_code
    if author is not None:
        kv[flag.author] = author
    if version is not None:
        kv[flag.version] = version
    if extra is not None:
        kv[flag.extra] = extra
    if health_define_name is not None:
        kv[flag.health_define_name] = health_define_name
    if enabled is not None:
        kv[flag.enabled] = enabled
    if description is not None:
        kv[flag.description] = description

    if function_name is not None:
        return _api.update_with_unique(FunctionDefine.function_name, function_name, kv, session=session)
    elif function_id is not None:
        return _api.update_with_unique(FunctionDefine.function_id, function_id, kv, session=session)
    else:
        raise ValueError(f'参数 function_name 与 function_id 必须有一个不为None.')


def delete_function_define(function_name: str = None, function_id: int = None, session=None):
    """
    移除指定算法

    :param function_name: 算法唯一标识/名称
    :param function_id: 算法编号
    :param session:
    :return: 是否存在
    """
    check_type(function_name, 'function_name', str, True)
    check_type(function_id, 'function_id', int, True)

    if function_name is not None:
        return _api.delete_with_unique(FunctionDefine.function_name, function_name, session=session)
    elif function_id is not None:
        return _api.delete_with_unique(FunctionDefine.function_id, function_id, session=session)
    else:
        raise ValueError(f'参数 function_name 与 function_id 必须有一个不为None.')


remove_function_define = delete_function_define
