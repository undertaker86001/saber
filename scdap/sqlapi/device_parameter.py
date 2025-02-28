"""

@create on: 2020.12.07
"""
__all__ = ['get_last_parameter', 'get_parameter', 'upload_parameter', 'add_parameter']

import pickle
from datetime import datetime
from typing import List, Optional, Union

from sqlalchemy import Column, JSON, text, DateTime
from sqlalchemy.dialects.mysql import VARCHAR, BIGINT, LONGBLOB

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class DeviceParameterKey(CommonFlag):
    tag = 'tag'
    function_id = 'function_id'
    effective_start = 'effective_start'
    effective_stop = 'effective_stop'
    reference = 'reference'
    parameter = 'parameter'


flag = DeviceParameterKey


class DeviceParameterItem(Base, DapBaseItem):
    __tablename__ = 'device_parameter'

    tag = Column(flag.tag, VARCHAR(128), nullable=False)
    function_id = Column(flag.function_id, BIGINT(20), nullable=False)
    effective_start = Column(
        flag.effective_start, DateTime, nullable=False,
        comment='参数的有效期起始时间', server_default=text("CURRENT_TIMESTAMP")
    )
    effective_stop = Column(
        flag.effective_stop, DateTime, nullable=False,
        comment='参数的有效期截至时间', server_default=text("CURRENT_TIMESTAMP")
    )
    parameter = Column(flag.parameter, LONGBLOB(0),
                       nullable=False, comment='参数, 由字典保存为保存为binary数据格式, 使用pickle可进行解压.')
    reference = Column(flag.reference, JSON)


_api = TableApi(DeviceParameterItem)


class ParameterTypeError(ValueError):
    def __init__(self):
        super().__init__(f'parameter_type 必须为: json/pkl.')


def add_parameter(tag: str, function_id: int, parameter: dict,
                  effective_start: datetime = None, effective_stop: datetime = None,
                  enabled: Union[bool, int] = True, reference: dict = None, description: str = ''):
    """
    上传阈值数据至数据库

    :param tag: 算法点位编号
    :param function_id: 算法编号
    :param effective_start: 阈值适用起始时间
    :param effective_stop: 阈值适用截止时间
    :param parameter: 阈值字典，算法服务器将读取该字段并用于算法计算
    :param enabled: 该段阈值数据是否启用，默认启用
    :param reference: 需要暂存的参数，算法服务器不使用
    :param description: 注释
    """
    reference = reference or dict()

    effective_start = effective_start or datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    effective_stop = effective_stop or datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    check_type(tag, 'tag', str)
    check_type(function_id, 'function_id', int)
    check_type(parameter, 'parameter', (str, dict, bytes))
    check_type(effective_start, 'effective_start', datetime)
    check_type(effective_start, 'effective_start', datetime)
    check_type(enabled, 'enabled', (bool, int))
    check_type(reference, 'reference', dict)
    check_type(description, 'description', str)
    
    parameter.pop('function', None)
    parameter.pop('global_parameter', None)

    parameter = encode_parameter(parameter)

    _api.add({
        flag.tag: tag,
        flag.function_id: function_id,
        flag.parameter: parameter,
        flag.effective_start: effective_start,
        flag.effective_stop: effective_stop,
        flag.enabled: enabled,
        flag.reference: reference,
        flag.description: description
    })


upload_parameter = add_parameter


def get_parameter(tag: str, function_id: int = None,
                  enabled: Optional[bool] = True, limit: int = 10) -> List[dict]:
    """
    获取算法参数

    :param tag: 算法点位编号
    :param function_id: 算法编号
    :param enabled: 该段阈值数据是否启用，默认启用
    :param limit: 限制获取的条数
    :return: 参数列表
    """

    check_type(tag, 'tag', str)
    check_type(function_id, 'function_id', int, True)
    check_type(limit, 'limit', int, True)
    check_type(enabled, 'enabled', (bool, int), True)

    filter_list = [
        DeviceParameterItem.tag == tag,
    ]
    if function_id is not None:
        filter_list.append(DeviceParameterItem.function_id == function_id)

    result = _api.query_with_none(filter_list=filter_list, order_by_list=[DeviceParameterItem.id.desc()], limit=limit)
    parameter = list()
    for r in result:
        r[flag.parameter] = decode_parameter(r[flag.parameter])
        parameter.append(r)
    return parameter


def get_last_parameter(tag: str, function_id: int, enabled: Optional[bool] = True) -> Optional[dict]:
    """
    获取最新的算法参数, 如果获取不到则返回None

    :param tag: 算法点位编号
    :param function_id: 算法编号
    :param enabled: 该段阈值数据是否启用，默认启用
    :return: 参数列表
    """
    parameter = get_parameter(tag, function_id, enabled, 1)
    if not parameter:
        raise Exception('无法获取任何算法参数.')
    return parameter[0]


def decode_parameter(parameter: bytes) -> dict:
    """
    解析参数

    :param parameter: 参数数据(是十六进制的字符串)
    :return: 解析后的参数
    """
    return pickle.loads(parameter)


def encode_parameter(parameter: dict) -> bytes:
    """
    编码参数

    :param parameter: 待编码的参数
    :return: 编码后的参数, 使用的编码类型
    """
    return pickle.dumps(parameter)
