"""

@create on: 2021.04.26
"""
from typing import Union, List

from sqlalchemy import Column, JSON, UniqueConstraint
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class EventDefineKey(CommonFlag):
    event_type = 'event_type'
    event_name = 'event_name'
    check_result = 'check_result'
    event_code = 'event_code'
    cn_name = 'cn_name'
    en_name = 'en_name'
    extra = 'extra'


flag = EventDefineKey


class EventDefine(Base, DapBaseItem):
    __tablename__ = 'event_define'

    event_type = Column(
        flag.event_type, INTEGER(10), nullable=False,
        comment='事件分类, 0->报警事件,1->周期性事件,2->加工事件,3->质检事件,其他待扩展'
    )
    event_name = Column(flag.event_name, VARCHAR(64), nullable=False,
                        comment='事件标识名称, 对算法有用但是对后端无用')
    cn_name = Column(flag.cn_name, VARCHAR(128), nullable=False,
                     comment='事件中文名称')
    en_name = Column(flag.en_name, VARCHAR(128), nullable=False,
                     comment='事件英文名称')
    check_result = Column(flag.check_result, INTEGER(10), nullable=False,
                          comment='根据event_type的不同有不同的功能')
    event_code = Column(flag.event_code, INTEGER(32), nullable=False,
                        comment='根据event_type的不同有不同的功能')
    extra = Column(flag.extra, JSON, nullable=False, comment='额外扩展字段')

    __table_args__ = (
        UniqueConstraint(event_type, event_code),
        UniqueConstraint(event_name)
    )


_api = TableApi(EventDefine)


def add_event_define(event_type: int, event_code: int, event_name: str,
                     cn_name: str, en_name: str, check_result: int = 0,
                     extra: dict = None, enabled: Union[bool, int] = True, description: str = '',
                     session=None):
    """

    :param en_name: 中文说明
    :param cn_name: 英文说明
    :param event_code: 事件编码
    :param check_result:
    :param event_type: 事件类型
    :param event_name: 事件名称
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """
    extra = extra or dict()
    check_type(event_type, 'event_type', int)
    check_type(event_name, 'event_name', str)
    check_type(event_code, 'event_code', int)

    check_type(cn_name, 'cn_name', str)
    check_type(en_name, 'en_name', str)

    check_type(check_result, 'check_result', int)

    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)
    _api.add({
        flag.event_type: event_type, flag.event_name: event_name, flag.event_code: event_code,
        flag.cn_name: cn_name, flag.en_name: en_name, flag.check_result: check_result,
        flag.enabled: enabled, flag.extra: extra, flag.description: description
    }, session)


upload_event_define = add_event_define


def get_event_defines(event_type: int, event_code: int, enabled: Union[bool, int] = None, session=None) -> List[int]:
    """

    :param event_type:
    :param event_code:
    :param session:
    :param enabled:
    :return:
    """
    check_type(event_type, 'event_type', int)
    check_type(event_code, 'event_code', int)
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(EventDefine.event_code, enabled,
                                           extra_filter_list=[EventDefine.event_type == event_type],
                                           session=session)


def get_event_define(event_type: int, event_code: Union[int, List[int]] = None,
                     enabled: Union[bool, int] = None, session=None) -> dict:
    """

    :param event_type:
    :param event_code:
    :param enabled:
    :param session:
    :return:
    """
    check_type(event_type, 'event_type', int)
    check_type(event_code, 'event_code', (list, int, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.query_with_unique(EventDefine.event_code, event_code, enabled,
                                  extra_filter_list=[EventDefine.event_type == event_type],
                                  session=session)


def event_define_exist(event_type: int, event_code: Union[List[int], int],
                       last_one: bool = False, enabled: Union[bool, int] = None, session=None) -> bool:
    """
    获取存在的

    :param event_type:
    :param event_code:
    :param enabled: 是否存在
    :param last_one: True->只要一个存在就返回True, False->只有所有点位都存在才返回True
    :param session:
    :return: 健康度定义列表
    """
    check_type(event_type, 'event_type', int)
    check_type(event_code, 'event_code', (list, int, tuple))
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.check_exist_with_unique(EventDefine.event_name, event_code, enabled,
                                        extra_filter_list=[EventDefine.event_type == event_type],
                                        session=session, last_one=last_one)


def update_event_define(event_type: int, event_code: int, event_name: str = None,
                        cn_name: str = None, en_name: str = None, check_result: int = None,
                        extra: dict = None, enabled: Union[bool, int] = None, description: str = None,
                        session=None):
    """

    :param en_name: 中文说明
    :param cn_name: 英文说明
    :param event_code: 事件编码
    :param check_result:
    :param event_type: 事件类型
    :param event_name: 事件名称
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    """

    kv = dict()
    check_type(event_type, 'event_type', int)
    check_type(event_code, 'event_code', int)
    check_type(event_name, 'event_name', str, True)

    check_type(cn_name, 'cn_name', str, True)
    check_type(en_name, 'en_name', str, True)

    check_type(check_result, 'check_result', int, True)

    check_type(extra, 'extra', dict, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(description, 'description', str, True)

    if event_name is not None:
        kv[flag.event_name] = event_name

    if cn_name is not None:
        kv[flag.cn_name] = cn_name

    if en_name is not None:
        kv[flag.en_name] = en_name

    if check_result is not None:
        kv[flag.check_result] = check_result

    if extra is not None:
        kv[flag.extra] = extra
    if enabled is not None:
        kv[flag.enabled] = enabled
    if description is not None:
        kv[flag.description] = description

    _api.update_with_unique(EventDefine.event_code, event_code, kv,
                            extra_filter_list=[EventDefine.event_type == event_type],
                            session=session)


def delete_event_define(event_type: int, event_code: Union[List[int], int], session=None):
    """

    :param event_type:
    :param event_code:
    :param session:
    """
    check_type(event_type, 'event_type', int)
    check_type(event_code, 'event_code', (tuple, list, int))
    _api.delete_with_unique(EventDefine.event_code, event_code,
                            extra_filter_list=[EventDefine.event_type == event_type],
                            session=session)


remove_event_define = delete_event_define
