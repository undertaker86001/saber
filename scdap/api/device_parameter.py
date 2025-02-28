"""
Created on 2019.12.23 by 张豪斌

获取某一个点位的某一个算法的参数数据
"""
__all__ = [
    'get_parameter',
]

from typing import Optional, Union

from scdap.logger import logger
from scdap.util.parser import parser_id


def get_parameter(algorithm_id: str, function: Union[int, str], load_mode: str = 'http') \
        -> Optional[dict]:
    """
    获取参数

    :param algorithm_id: 算法点位编号
    :param function: 算法名称或者算法编号
    :param load_mode: 读取的模式, http/sql/local
    :return: 参数
    """
    if isinstance(function, str):
        function = parser_id(function)
    if load_mode == 'http':
        from scdap import config
        from scdap.util.session import parse_router, do_request, PickleError

        url = parse_router(config.SQLAPI_SERVER_URL, f'/device-parameter/{algorithm_id}:{function}/')
        try:
            parameter = do_request('get', url, json_type=True)
        except PickleError:
            logger.warning(f'aid={algorithm_id}, fid={function}的算法参数获取失败, '
                           f'无法解析接口: {url}回传的数据, 一般是没有把参数传递到线上服务中.')
            return None

        # 兼容新旧版本的sqlapi接口
        # 新版的会使用java设计的
        # 并且只返回parameter内的二进制数据
        if 'tag' in parameter and 'function_id' in parameter and 'parameter' in parameter:
            return parameter
        else:
            return {'parameter': parameter, 'tag': algorithm_id, 'function_id': function}

    elif load_mode == 'sql':
        from scdap.sqlapi import device_parameter
        parameter = device_parameter.get_last_parameter(algorithm_id, function)
    else:
        from scdap.gop.func import get_program_parameter
        parameter = get_program_parameter(algorithm_id, function, gnet=False, net_load_mode='local')
    return parameter
