"""

@create on: 2020.12.08
点位健康度配置信息
只要配置了进程配置, 进程配置中的所有点位(包括devices)中的所有点位都会创建一条数据
也可以用来确认某一个点位是否已经配置了算法(既这个点位可能是一个独立的进程也可能是作为某一个联动进程的子点位)
"""
__all__ = [
    'device_health_exist', 'add_device_health', 'delete_device_health', 'update_device_health',
    'upload_device_health', 'remove_device_health', 'get_device_health', 'get_device_health_tags'
]

from typing import List, Union

from sqlalchemy import Column
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER

from .sqlbase import Base, TableApi, check_type, DapBaseItem


class DeviceHealthKey(object):
    device_option_tag = 'device_option_tag'
    health_define_id = 'health_define_id'
    node_id = 'node_id'


flag = DeviceHealthKey


class DeviceHealth(Base, DapBaseItem):
    __tablename__ = 'device_health'

    device_option_tag = Column(
        flag.device_option_tag, VARCHAR(128), nullable=False, unique=True,
        comment='设备(节点)组主标识编号'
    )
    node_id = Column(
        flag.node_id, INTEGER(64), nullable=False, unique=True,
        comment='后端用设备点位编号'
    )
    health_define_id = Column(flag.health_define_id, VARCHAR(256), nullable=False, comment="健康编号,多个编号以','隔开")


_api = TableApi(DeviceHealth)


def _check_idlist(idlist: list):
    for hid in idlist:
        if not isinstance(hid, int):
            raise TypeError(f'health_define_id中配置的id必须为类型int.')


def add_device_health(device_option_tag: str, node_id: int, health_define_id: List[int], session=None):
    """
    添加节点健康度信息

    :param device_option_tag: 节点标签
    :param node_id: 后端用设备点位编号
    :param health_define_id: 健康度列表
    :param session:
    """
    check_type(device_option_tag, 'device_option_tag', str)
    check_type(node_id, 'node_id', int)
    check_type(health_define_id, 'health_define_id', (list, int))
    _check_idlist(health_define_id)
    _api.add({
        flag.device_option_tag: device_option_tag,
        flag.node_id: node_id,
        flag.health_define_id: ','.join(map(str, health_define_id)),
    }, session)


upload_device_health = add_device_health


def get_device_health_tags(session=None) -> List[int]:
    """
    获取所有存在的节点标签

    :param session:
    :return: 存在的健康度标识字段
    """
    return _api.get_exist_keys_with_unique(DeviceHealth.device_option_tag, session=session)


def get_device_health(device_option_tag: Union[List[str], str, None] = None, session=None)\
        -> dict:
    """
    获取所有存在的节点健康度信息


    :param device_option_tag: 接电脑标签
    :param session:
    :return: 健康度定义列表
    """
    check_type(device_option_tag, 'device_option_tag', (list, str))
    return _api.query_with_unique(DeviceHealth.device_option_tag, device_option_tag, session=session)


def device_health_exist(device_option_tag: Union[List[str], str],
                        last_one: bool = False, session=None) -> bool:
    """
    节点健康度信息是否存在

    :param device_option_tag:
    :param last_one: True->只要一个存在就返回True, False->只有所有点位都存在才返回True
    :param session:
    :return:
    """
    check_type(device_option_tag, 'device_option_tag', (list, str))
    check_type(last_one, 'last_one', bool)
    return _api.check_exist_with_unique(DeviceHealth.device_option_tag, device_option_tag,
                                        last_one=last_one, session=session)


def update_device_health(device_option_tag: str, node_id: int = None, health_define_id: List[int] = None, session=None):
    """
    更新节点健康度信息

    :param device_option_tag: 节点标签
    :param node_id: 后端用设备点位编号
    :param health_define_id: 健康度列表
    :param session:
    """
    kv = dict()
    check_type(device_option_tag, 'device_option_tag', str)
    check_type(node_id, 'node_id', int, True)
    check_type(health_define_id, 'health_define_id', (tuple, list), True)
    _check_idlist(health_define_id)

    if node_id is not None:
        kv[flag.node_id] = node_id

    if health_define_id is not None:
        kv[flag.health_define_id] = ','.join(map(str, health_define_id))

    _api.update_with_unique(DeviceHealth.device_option_tag, device_option_tag, kv, session=session)


def delete_device_health(device_option_tag: Union[List[str], str], session=None):
    """
    删除节点健康度信息

    :param device_option_tag: 节点标签
    :param session:
    """
    _api.delete_with_unique(DeviceHealth.device_option_tag, device_option_tag, session=session)


remove_device_health = delete_device_health
