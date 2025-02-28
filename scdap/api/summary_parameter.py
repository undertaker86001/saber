"""

@create on: 2021.04.25
"""
__all__ = [
    'get_parameter',
]

from typing import Optional, Union
from .device_parameter import get_parameter as _get_parameter


def get_parameter(algorithm_id: str, function: Union[int, str], load_mode: str = 'http') \
        -> Optional[dict]:
    """
    获取参数

    :param algorithm_id: 算法点位编号
    :param function: 算法名称或者算法编号
    :param load_mode: 读取的模式, http/sql/local
    :return: 参数
    """
    return _get_parameter(algorithm_id, function, load_mode)

