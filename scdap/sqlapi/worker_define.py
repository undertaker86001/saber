"""

@create on: 2021.03.01
"""
__all__ = [
    'get_worker_names', 'get_worker_define',
    'add_worker_define', 'update_worker_define', 'upload_worker_define',
    'remove_worker_define', 'delete_worker_define', 'worker_define_exist',
]

from typing import Union, List

from sqlalchemy import Column, JSON
from sqlalchemy.dialects.mysql import VARCHAR

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class WorkerDefineKey(CommonFlag):
    worker_name = 'worker_name'
    extra = 'extra'


flag = WorkerDefineKey


class WorkerDefine(Base, DapBaseItem):
    __tablename__ = 'worker_define'
    __table_args__ = {'comment': '算法工作组工作组类型.'}

    worker_name = Column(flag.worker_name, VARCHAR(64), nullable=False, unique=True, comment='算法工作组唯一标识/名称')
    extra = Column(flag.extra, JSON, nullable=False, comment='额外扩展字段')


_api = TableApi(WorkerDefine)


def add_worker_define(worker_name: str, extra: dict = None,
                      enabled: Union[bool, int] = True, description: str = '', session=None):
    """
    新增算法工作组

    :param worker_name: 算法工作组唯一标识/名称
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """

    extra = extra or dict()

    check_type(worker_name, 'worker_name', str)
    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)
    _api.add({
        flag.worker_name: worker_name,
        flag.extra: extra,
        flag.enabled: enabled,
        flag.description: description
    }, session=session)


upload_worker_define = add_worker_define


def worker_define_exist(worker_name: str, enabled: Union[bool, int] = None, session=None) -> bool:
    """
    是否存在指定的算法工作组标识或者编号, 两个参数必须选择一个输入

    :param worker_name: 算法工作组唯一标识/名称
    :param enabled:
    :param session:
    :return: 是否存在
    """
    check_type(worker_name, 'worker_name', str, False)
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.check_exist_with_unique(WorkerDefine.worker_name, worker_name, enabled, session=session)


def get_worker_names(enabled: Union[bool, int] = None, session=None) -> List[str]:
    """
    获取所有存在的worker_name

    :param enabled: 是否获取启用的配置
    :param session:
    :return: worker_name列表
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(WorkerDefine.worker_name, enabled, session=session)


def get_worker_define(worker_name: Union[List[str], str, None] = None,
                      enabled: Union[bool, int] = None, session=None) -> dict:
    """
    获取指定的算法工作组定义

    :param worker_name: 算法工作组唯一标识/名称
    :param enabled: 是否只获取启动的定义
    :param session:
    :return: 算法工作组定义列表
    """
    check_type(worker_name, 'worker_name', (str, list, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.query_with_unique(WorkerDefine.worker_name, worker_name, enabled, session=session)


def update_worker_define(worker_name: str, extra: dict = None,
                         enabled: Union[bool, int] = None, description: str = None, session=None):
    """
    新增算法工作组

    :param worker_name: 算法工作组唯一标识/名称
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """
    check_type(worker_name, 'worker_name', str, False)
    check_type(extra, 'extra', dict, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(description, 'description', str, True)

    kv = dict()
    if extra is not None:
        kv[flag.extra] = extra
    if enabled is not None:
        kv[flag.enabled] = enabled
    if description is not None:
        kv[flag.description] = description

    return _api.update_with_unique(WorkerDefine.worker_name, worker_name, kv, session=session)


def delete_worker_define(worker_name: str, session=None):
    """
    移除指定算法工作组

    :param worker_name: 算法工作组唯一标识/名称
    :param session:
    :return: 是否存在
    """
    check_type(worker_name, 'worker_name', str, False)

    return _api.delete_with_unique(WorkerDefine.worker_name, worker_name, session=session)


remove_worker_define = delete_worker_define
