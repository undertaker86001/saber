"""

@create on: 2020.12.08
健康度定义接口
"""
__all__ = [
    'get_health_define', 'health_name_exist', 'add_health_define',
    'update_health_define', 'get_health_ids', 'get_max_health_id',
    'remove_health_define', 'delete_health_define', 'upload_health_define',
    'get_health_names'
]

from typing import Union, List, Dict, Optional

from sqlalchemy import Column, JSON, text
from sqlalchemy.dialects.mysql import VARCHAR, TINYINT, INTEGER

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class HealthDefineKey(CommonFlag):
    health_name = 'health_name'
    health_id = 'health_id'
    reverse = 'reverse'
    rt_cn_name = 'rt_cn_name'
    rt_en_name = 'rt_en_name'
    limit = 'limit'
    secular_cn_name = 'secular_cn_name'
    secular_en_name = 'secular_en_name'
    default_score = 'default_score'
    cn_alarm_topic = 'cn_alarm_topic'
    en_alarm_topic = 'en_alarm_topic'
    extra = 'extra'


flag = HealthDefineKey


class HealthDefine(Base, DapBaseItem):
    __tablename__ = 'health_define'

    health_name = Column(
        flag.health_name, VARCHAR(32), nullable=False, unique=True, comment='唯一标识名称，不作为展示的依据'
    )
    health_id = Column(flag.health_id, INTEGER(10), nullable=False, unique=True, comment='唯一健康度定义编号')
    limit = Column(
        flag.limit, TINYINT(1), nullable=False, server_default=text("'1'"),
        comment='是否限制范围健康度的范围\\r\\'
                'n0->健康度的范围无限制\\r\\'
                'n1->健康度的范围呗限制在[0, 100], 其中0代表空健康度需要延续之前的健康度'
    )
    reverse = Column(
        flag.reverse, TINYINT(1), nullable=False, server_default=text("'0'"),
        comment='越低的数值是否代表越健康\\r\\n0->越高越健康\\r\\n1->越低越健康'
    )
    rt_cn_name = Column(flag.rt_cn_name, VARCHAR(64), nullable=False, comment='实时健康度中文名称')
    rt_en_name = Column(flag.rt_en_name, VARCHAR(64), nullable=False, comment='实时健康度英文名称')
    secular_cn_name = Column(flag.secular_cn_name, VARCHAR(64), nullable=False, comment='长期健康度的中文名称')
    secular_en_name = Column(flag.secular_en_name, VARCHAR(64), nullable=False, comment='长期健康度的英文名称')
    cn_alarm_topic = Column(flag.cn_alarm_topic, VARCHAR(255), nullable=False, comment='健康度报警主题中文说明, 前端用')
    en_alarm_topic = Column(flag.en_alarm_topic, VARCHAR(255), nullable=False, comment='健康度报警主题英文说明, 前端用')
    default_score = Column(flag.default_score, INTEGER(64), nullable=False, comment='默认健康度数值')
    extra = Column(flag.extra, JSON, nullable=False, comment='额外扩展字段')


_api = TableApi(HealthDefine)


def get_max_health_id() -> int:
    # 从0开始 所以需要配置默认为-1
    return _api.get_max_unique_id(HealthDefine.health_id, -1)


def add_health_define(health_name: str, rt_cn_name: str, rt_en_name: str,
                      secular_cn_name: str, secular_en_name: str,
                      cn_alarm_topic: str, en_alarm_topic: str,
                      default_score: int, reverse: Union[bool, int], limit: Union[bool, int] = 1,
                      health_id: int = None, extra: dict = None, enabled: Union[bool, int] = True,
                      description: str = '', session=None) -> int:
    """
    添加新的健康度

    :param cn_alarm_topic:健康度报警主题中文说明, 前端用
    :param en_alarm_topic:健康度报警主题英文说明, 前端用
    :param health_name: 健康度名称标识, 不作为展示的依据
    :param reverse: 健康度数值越高是否越健康, 如堵塞度为True, 趋势性为False
    :param limit: 健康度范围限制
    :param rt_cn_name: 实时健康度中文名称
    :param rt_en_name: 实时健康度英文名称
    :param secular_cn_name: 长期健康度的中文名称
    :param secular_en_name: 长期健康度的英文名称
    :param default_score: 默认健康度数值
    :param health_id: 健康度编号, 默认自增
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    :param session:
    :return: 健康度编号
    """
    extra = extra or dict()
    check_type(health_name, 'health_name', str)
    check_type(reverse, 'reverse', (int, bool))
    check_type(limit, 'limit', (int, bool))

    check_type(rt_cn_name, 'rt_cn_name', str)
    check_type(rt_en_name, 'rt_en_name', str)

    check_type(secular_cn_name, 'secular_cn_name', str)
    check_type(secular_en_name, 'secular_en_name', str)

    check_type(cn_alarm_topic, 'cn_alarm_topic', str)
    check_type(en_alarm_topic, 'en_alarm_topic', str)

    check_type(default_score, 'default_score', int)
    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)
    check_type(health_id, 'health_id', int, True)

    health_id = health_id or (get_max_health_id() + 1)
    check_type(health_id, 'health_id', int)

    _api.add({
        flag.health_name: health_name, flag.health_id: health_id,
        flag.rt_cn_name: rt_cn_name, flag.rt_en_name: rt_en_name,
        flag.secular_cn_name: secular_cn_name, flag.secular_en_name: secular_en_name,
        flag.cn_alarm_topic: cn_alarm_topic, flag.en_alarm_topic: en_alarm_topic,
        flag.default_score: default_score, flag.reverse: reverse, flag.limit: limit,
        flag.enabled: enabled, flag.extra: extra, flag.description: description
    }, session=session)
    return health_id


upload_health_define = add_health_define


def health_name_exist(health_name: str = None, health_id: int = None, enabled: Union[bool, int] = None, session=None) \
        -> bool:
    """
    确认健康度标识是否存在

    :param health_name:  健康度名称标识
    :param health_id: 健康度编号
    :param enabled:
    :param session:
    :return: 是否存在
    """
    check_type(health_name, 'health_name', str, True)
    check_type(health_id, 'health_id', int, True)
    check_type(enabled, 'enabled', (bool, int), True)

    if health_name is not None:
        return _api.check_exist_with_unique(HealthDefine.health_name, health_name, enabled, session=session)
    elif health_id is not None:
        return _api.check_exist_with_unique(HealthDefine.health_id, health_id, enabled, session=session)
    else:
        raise ValueError(f'参数 health_name 与 health_id 必须有一个不为None.')


def get_health_id_from_name(name: List[str], enabled: Union[bool, int] = None, session=None) -> List[int]:
    """
    通过health_name健康度名称查询健康度编号health_id

    :param name: 健康度名称列表
    :param enabled: 是否获取启用的配置
    :param session:
    :return: 健康度编号列表
    """
    check_type(name, 'name', (list, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    for n in name:
        if not isinstance(n, str):
            raise TypeError('name内存在的元素必须为类型str.')

    result = _api.get_exist_keys_with_unique(
        HealthDefine.health_id,
        enabled,
        [HealthDefine.health_name.in_(name)],
        session=session
    )
    if len(result) != len(name):
        raise Exception('待查询的字段与查询结果数量不一致, 请确认所有字段都已经在表中定义.')
    return result


def get_health_ids(enabled: Union[bool, int] = None) -> List[int]:
    """
    获取所有存在的health_id


    :param enabled: 是否获取启用的配置


    :return: health_id列表
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(HealthDefine.health_id, enabled)


def get_health_names(enabled: Union[bool, int] = None) -> List[str]:
    """
    获取所有存在的健康度标识字段


    :param enabled: 是否获取启用的配置


    :return: 存在的健康度标识字段
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(HealthDefine.health_name, enabled)


def update_health_define(health_name: str = None, health_id: int = None,
                         rt_cn_name: str = None, rt_en_name: str = None,
                         secular_cn_name: str = None, secular_en_name: str = None,
                         cn_alarm_topic: str = None, en_alarm_topic: str = None,
                         reverse: Union[bool, int] = None, limit: Union[bool, int] = None, default_score: int = None,
                         extra: dict = None, enabled: Union[bool, int] = None, description: str = None):
    """
    更新健康度内容

    :param health_name: 健康度名称标识
    :param health_id: 健康度编号
    :param reverse: 健康度数值越高是否越健康, 如堵塞度为True, 趋势性为False
    :param limit: 健康度数值是否现顶范围
    :param rt_cn_name: 实时健康度中文名称
    :param rt_en_name: 实时健康度英文名称
    :param secular_cn_name: 长期健康度的中文名称
    :param secular_en_name: 长期健康度的英文名称
    :param cn_alarm_topic:健康度报警主题中文说明, 前端用
    :param en_alarm_topic:健康度报警主题英文说明, 前端用
    :param default_score: 默认健康度数值
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    """
    check_type(health_name, 'health_name', str, True)
    check_type(health_id, 'health_id', int, True)
    check_type(reverse, 'reverse', (int, bool), True)
    check_type(limit, 'limit', (int, bool), True)

    check_type(cn_alarm_topic, 'cn_alarm_topic', str, True)
    check_type(en_alarm_topic, 'en_alarm_topic', str, True)

    check_type(rt_cn_name, 'rt_cn_name', str, True)
    check_type(rt_en_name, 'rt_en_name', str, True)

    check_type(secular_cn_name, 'secular_cn_name', str, True)
    check_type(secular_en_name, 'secular_en_name', str, True)

    check_type(default_score, 'default_score', int, True)
    check_type(extra, 'extra', dict, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(description, 'description', str, True)
    kv = dict()

    if reverse is not None:
        kv[flag.reverse] = reverse

    if limit is not None:
        kv[flag.limit] = limit

    if rt_cn_name is not None:
        kv[flag.rt_cn_name] = rt_cn_name
    if rt_en_name is not None:
        kv[flag.rt_en_name] = rt_en_name

    if secular_cn_name is not None:
        kv[flag.secular_cn_name] = secular_cn_name
    if secular_en_name is not None:
        kv[flag.secular_en_name] = secular_en_name

    if cn_alarm_topic is not None:
        kv[flag.cn_alarm_topic] = cn_alarm_topic
    if en_alarm_topic is not None:
        kv[flag.en_alarm_topic] = en_alarm_topic

    if default_score is not None:
        kv[flag.default_score] = default_score
    if extra is not None:
        kv[flag.extra] = extra
    if enabled is not None:
        kv[flag.enabled] = enabled
    if description is not None:
        kv[flag.description] = description

    if health_name:
        return _api.update_with_unique(HealthDefine.health_name, health_name, kv)
    elif health_id:
        return _api.update_with_unique(HealthDefine.health_id, health_id, kv)
    else:
        raise ValueError(f'参数 health_name 与 health_id 必须有一个不为None.')


def get_health_define(
        health_name: Union[List[str], str, None] = None,
        health_id: Union[List[int], int, None] = None,
        enabled: Optional[bool] = None) \
        -> Union[Dict[str, dict], dict]:
    """
    获取健康度定义


    :param health_name: 健康度标识
    :param health_id: 健康度编号
    :param enabled: 是否只获取启动的定义
    :return: 健康度定义列表
    """
    check_type(health_name, 'health_name', (str, list, tuple), True)
    check_type(health_id, 'health_id', (int, list, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    if health_id is not None:
        return _api.query_with_unique(HealthDefine.health_id, health_id, enabled)
    elif health_name is not None:
        return _api.query_with_unique(HealthDefine.health_name, health_name, enabled)
    else:
        raise ValueError(f'参数 health_name 与 health_id 必须有一个不为None.')


def delete_health_define(health_name: Union[List[str], str, None] = None,
                         health_id: Union[List[int], int, None] = None, session=None):
    """
    删除健康度定义


    :param health_name: 健康度定义名称


    :param health_id: 健康度编号


    :param session:
    """
    check_type(health_name, 'health_name', str, True)
    check_type(health_id, 'health_id', int, True)

    if health_name is not None:
        return _api.delete_with_unique(HealthDefine.health_name, health_name, session=session)
    elif health_id is not None:
        return _api.delete_with_unique(HealthDefine.health_id, health_id, session=session)
    else:
        raise ValueError(f'参数 health_name 与 health_id 必须有一个不为None.')


remove_health_define = delete_health_define
