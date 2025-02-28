"""

@create on: 2020.12.08
"""
__all__ = [
    'get_recommendation_define', 'get_recommendation_names', 'delete_recommendation_define',
    'remove_recommendation_define', 'recommendation_define_exist', 'update_recommendation_define',
    'upload_recommendation_define', 'add_recommendation_define'
]

from typing import Union, List, Dict, Optional

from sqlalchemy import Column, JSON
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class RecommendationDefineKey(CommonFlag):
    recommendation_name = 'recommendation_name'
    recommendation_id = 'recommendation_id'
    cn_name = 'cn_name'
    en_name = 'en_name'
    extra = 'extra'


flag = RecommendationDefineKey


class RecommendationDefine(Base, DapBaseItem):
    __tablename__ = 'recommendation_define'
    __table_args__ = {'comment': '算法维护建议'}

    recommendation_name = Column(
        flag.recommendation_name, VARCHAR(32), nullable=False, unique=True, comment='维护建议唯一标识, 不作为展示的依据'
    )
    recommendation_id = Column(
        flag.recommendation_id, BIGINT(20), nullable=False, unique=True, comment='维护建议编号'
    )
    cn_name = Column(flag.cn_name, VARCHAR(32), nullable=False, comment='中文名称')
    en_name = Column(flag.en_name, VARCHAR(32), nullable=False, comment='英文名称')
    extra = Column(flag.extra, JSON, nullable=False, comment='额外扩展字段')


_api = TableApi(RecommendationDefine)


def get_max_recommendation_id() -> int:
    return _api.get_max_unique_id(RecommendationDefine.recommendation_id, -1)


def add_recommendation_define(recommendation_name: str, cn_name: str, en_name: str, recommendation_id: int = None,
                              extra: dict = None, enabled: Union[bool, int] = True, description: str = ''):
    """
    添加新的维护建议

    :param recommendation_name: 维护建议标识, 不作为展示的依据
    :param recommendation_id: 维护建议编号, 默认自增
    :param cn_name: 中文名称
    :param en_name: 英文名称
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    """
    extra = extra or dict()

    check_type(recommendation_name, 'recommendation_name', str)
    check_type(cn_name, 'cn_name', str)
    check_type(en_name, 'en_name', str)

    check_type(extra, 'extra', dict)
    check_type(enabled, 'enabled', (bool, int))
    check_type(description, 'description', str)

    recommendation_id = recommendation_id or (get_max_recommendation_id() + 1)
    check_type(recommendation_id, 'recommendation_id', int)

    _api.add({
        flag.recommendation_name: recommendation_name, flag.recommendation_id: recommendation_id,
        flag.cn_name: cn_name, flag.en_name: en_name,
        flag.extra: extra, flag.enabled: enabled, flag.description: description
    })


upload_recommendation_define = add_recommendation_define


def recommendation_define_exist(recommendation_name: str = None, recommendation_id: int = None) -> bool:
    """
    是否存在指定的维护建议

    :param recommendation_name: 维护建议标识
    :param recommendation_id: 维护建议编号
    :return: 是否存在
    """
    check_type(recommendation_name, 'recommendation_name', str, True)
    check_type(recommendation_id, 'recommendation_id', int, True)

    if recommendation_name is not None:
        return _api.check_exist_with_unique(RecommendationDefine.recommendation_name, recommendation_name)
    elif recommendation_id is not None:
        return _api.check_exist_with_unique(RecommendationDefine.recommendation_id, recommendation_id)
    else:
        raise ValueError(f'参数 recommendation_name 与 recommendation_id 必须有一个不为None.')


def get_recommendation_ids(enabled: Optional[bool] = True) -> List[int]:
    """
    获取所有存在的recommendation_id


    :param enabled: 是否获取启用的配置
    :return: recommendation_id列表
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(RecommendationDefine.recommendation_id, enabled)


def get_recommendation_names(enabled: Optional[bool] = True) -> List[str]:
    """
    获取所有存在的recommendation_name

    :param enabled: 是否获取启用的配置
    :return: recommendation_name列表
    """
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.get_exist_keys_with_unique(RecommendationDefine.recommendation_name, enabled)


def get_recommendation_define(recommendation_name: Union[List[str], str, None] = None,
                              recommendation_id: Union[List[int], int, None] = None,
                              enabled: Optional[bool] = None) -> Union[Dict[str, dict], dict]:
    """
    获取指定的维护建议定义

    :param recommendation_name: 维护建议标识
    :param recommendation_id: 维护建议编号
    :param enabled: 是否获取启用的配置
    :return: 维护建议定义列表
    """
    check_type(recommendation_name, 'recommendation_name', (str, list, tuple), True)
    check_type(recommendation_id, 'recommendation_id', (int, list, tuple), True)
    check_type(enabled, 'enabled', (bool, int), True)
    if recommendation_id is not None:
        return _api.query_with_unique(RecommendationDefine.recommendation_id, recommendation_id, enabled)
    elif recommendation_name is not None:
        return _api.query_with_unique(RecommendationDefine.recommendation_name, recommendation_name, enabled)
    else:
        raise ValueError(f'参数 recommendation_name 与 recommendation_id 必须有一个不为None.')


def update_recommendation_define(recommendation_name: str = None, recommendation_id: int = None,
                                 cn_name: str = None, en_name: str = None,
                                 extra: dict = None, enabled: Union[bool, int] = None, description: str = None):
    """
    添加新的维护建议

    :param recommendation_name: 维护建议标识, 不作为展示的依据
    :param recommendation_id: 维护建议编号
    :param cn_name: 中文名称
    :param en_name: 英文名称
    :param extra: 额外扩展字段
    :param enabled: 是否启用
    :param description: 备注与描述
    """
    check_type(recommendation_name, 'recommendation_name', str, True)
    check_type(recommendation_id, 'recommendation_id', int, True)
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

    if recommendation_name is not None:
        return _api.update_with_unique(RecommendationDefine.recommendation_name, recommendation_name, kv)
    elif recommendation_id is not None:
        return _api.update_with_unique(RecommendationDefine.recommendation_id, recommendation_id, kv)
    else:
        raise ValueError(f'参数 recommendation_name 与 recommendation_id 必须有一个不为None.')


def delete_recommendation_define(recommendation_name: Union[List[str], str] = None,
                                 recommendation_id: Union[List[int], int, None] = None):
    """
    删除指定的维护建议定义

    :param recommendation_name: 维护建议标识
    :param recommendation_id: 维护建议编号
    """
    check_type(recommendation_name, 'recommendation_name', str, True)
    check_type(recommendation_id, 'recommendation_id', int, True)

    if recommendation_name is not None:
        return _api.delete_with_unique(RecommendationDefine.recommendation_name, recommendation_name)
    elif recommendation_id is not None:
        return _api.delete_with_unique(RecommendationDefine.recommendation_id, recommendation_id)
    else:
        raise ValueError(f'参数 recommendation_name 与 recommendation_id 必须有一个不为None.')


remove_recommendation_define = delete_recommendation_define
