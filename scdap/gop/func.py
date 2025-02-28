"""

@create on: 2020.01.24

get parameters or options
用于获得算法组参数与配置的方法库
"""

__all__ = [
    'get_parameter',
    'get_program_parameter',
    'get_program_option',
    'list_program_parameter',

    'get_option',
    'get_summary_option',
    'get_summary_parameter',
    'list_summary_parameter',

    'list_parameter',

    'register_option',
    'clear_register_option',
    'register_parameter',
    'clear_register_parameter'
]

from typing import Dict, Optional, List

from scdap.logger import logger
from scdap.flag import option_key
from scdap.core.process_type import check_process_type

from . import loc

# 缓存配置与参数
# 拥有最高的查询优先度
option_register: Dict[str, dict] = dict()
parameter_register: Dict[str, dict] = dict()


def _print_message(how: bool, ok: bool, env: str, process_type: str,
                   mode: str, tag: str, function_id: int,
                   message: str = '获取成功'):
    message = f'get_{how}(process_type={process_type}, algorithm_id={tag}, function_id={function_id}, ' \
              f'from={env}, mode={mode}) -> {message}'
    if ok:
        logger.seco(message)
    else:
        logger.warning(message)


def get_parameter(tag: str, function_id: int, process_type: str,
                  gnet: bool = True, gloc: bool = True, net_load_mode: str = 'http',
                  greg: bool = True) -> Optional[dict]:
    """
    根据算法组标签与算法编号获得对应的算法参数

    :param process_type:
    :param tag: 算法点位编号
    :param function_id: 算法编号
    :param gnet: 是否获取数据库参数
    :param gloc: 是否获取本地参数
    :param net_load_mode: 在gnet=True时选择的读取模式, http/sql
    :param greg: 是否从最上级的缓存中获取配置
    """
    check_process_type(process_type)

    print_kwargs = {'process_type': process_type, 'how': 'parameter',
                    'mode': net_load_mode, 'tag': tag, 'function_id': function_id}

    if greg:
        parameter = get_register_parameter(tag, function_id, process_type)
        if parameter:
            _print_message(**print_kwargs, ok=True, env='reg')
            return parameter

    if gnet:
        if process_type == 'summary':
            from scdap.api import summary_parameter
            do_function = summary_parameter.get_parameter
        else:
            from scdap.api import device_parameter
            do_function = device_parameter.get_parameter
        try:
            parameter = do_function(tag, function_id, net_load_mode)
            if parameter:
                parameter = parameter['parameter']
                _print_message(**print_kwargs, ok=True, env='net')
                return parameter
        except Exception as e:
            _print_message(**print_kwargs, ok=False, env='net', message=f'获取失败: {e}')

    if gloc:
        parameter = loc.get_parameter(tag, function_id, 'program')
        if parameter:
            _print_message(**print_kwargs, ok=False, env='loc')
            return parameter

    _print_message(**print_kwargs, ok=False, env='any', message=f'无法从任何位置获取参数.')
    return None


def list_parameter(tag: str, function_ids: List[int], process_type: str,
                   gnet: bool = True, gloc: bool = True, net_load_mode: str = 'http',
                   greg: bool = True) -> Dict[int, dict]:
    """
    批量获取参数

    :param process_type:
    :param tag: 算法点位编号
    :param function_ids: 算法编号
    :param gnet: 是否获取数据库参数
    :param gloc: 是否获取本地参数
    :param net_load_mode: 在gnet=True时选择的读取模式, http/sql
    :param greg: 是否从最上级的缓存中获取配置
    """
    parameter = dict()
    for fid in function_ids:
        parameter[fid] = get_parameter(tag, fid, process_type, gnet, gloc, net_load_mode, greg) or dict()
    return parameter


def get_option(tag: str, process_type: str, gnet: bool = True, gloc: bool = True,
               net_load_mode: str = 'http', greg: bool = True) -> Optional[dict]:
    """
    根据算法组标签与算法编号获得算法工作组配置

    :param process_type:
    :param tag: 算法点位编号
    :param gnet: 是否从数据库读取配置
    :param gloc: 是否从本地读取配置,假设gnet==True, 则优先从数据库获取配置
    :param net_load_mode: 在gnet=True时选择的读取模式, http/sql
    :param greg: 是否从最上级的缓存中获取配置
    """
    check_process_type(process_type)

    print_kwargs = {'process_type': process_type, 'how': 'option', 'mode': net_load_mode, 'tag': tag, 'function_id': -1}
    if greg:
        option = get_register_option(tag, process_type)
        if option:
            _print_message(**print_kwargs, ok=True, env='reg')
            return option

    if gnet:
        if process_type == 'summary':
            from scdap.api import summary_option
            do_function = summary_option.get_option
        else:
            from scdap.api import device_option
            do_function = device_option.get_option

        try:
            option = do_function(tag, load_mode=net_load_mode)
            if option:
                _print_message(**print_kwargs, ok=True, env='net')
                return option
        except Exception as e:
            _print_message(**print_kwargs, ok=False, env='net', message=f'获取失败: {e}')

    if gloc:
        option = loc.get_option(tag, process_type)
        if option:
            _print_message(**print_kwargs, ok=True, env='loc')
            return option

    _print_message(**print_kwargs, ok=False, env='any', message=f'无法从任何位置获取配置.')
    return None


def register_option(process_type: str, option: dict):
    """
    将参数缓存至本地
    注意，缓存的设备将拥有获取的最高优先级
    register > net > loc

    :param option: 设备配置
    :param process_type: 配置类型
    """
    option, param = loc.parse_program_option(option, save=False)
    tag = option[option_key.tag]
    option_register[loc.join_process_name(tag, process_type)] = option
    for fid, p in param.items():
        parameter_register[loc.get_parameter_tag(tag, fid, process_type)] = p


def clear_register_option():
    option_register.clear()


def register_parameter(tag: str, fid: int, process_type: str, parameter: dict):
    parameter_register[loc.get_parameter_tag(tag, fid, process_type)] = parameter


def clear_register_parameter():
    parameter_register.clear()


def get_register_option(tag: str, process_type: str) -> dict:
    """
    获取暂存的配置

    :param tag: 设备标签
    :param process_type: 进程类型
    :return: 暂存的配置
    """
    return option_register.get(loc.join_process_name(tag, process_type))


def get_register_parameter(tag: str, function_id: int, process_type: str) -> dict:
    """
    获取暂存的参数

    :param tag: 设备标签
    :param function_id: 算法编号
    :param process_type: 进程类型
    :return: 暂存的参数
    """
    return parameter_register.get(loc.get_parameter_tag(tag, function_id, process_type))


def get_program_option(tag: str, gnet: bool = True, gloc: bool = True,
                       net_load_mode: str = 'http', greg: bool = True):
    return get_option(tag, 'program', gnet, gloc, net_load_mode, greg)


def get_program_parameter(tag: str, function_id: int,
                          gnet: bool = True, gloc: bool = True, net_load_mode: str = 'http',
                          greg: bool = True) -> Optional[dict]:
    return get_parameter(tag, function_id, 'program', gnet, gloc, net_load_mode, greg)


def list_program_parameter(tag: str, function_ids: List[int],
                           gnet: bool = True, gloc: bool = True, net_load_mode: str = 'http',
                           greg: bool = True) -> Dict[int, dict]:
    return list_parameter(tag, function_ids, 'program', gnet, gloc, net_load_mode, greg)


def get_summary_option(tag: str, gnet: bool = True, gloc: bool = True,
                       net_load_mode: str = 'http', greg: bool = True):
    return get_option(tag, 'summary', gnet, gloc, net_load_mode, greg)


def get_summary_parameter(tag: str, function_id: int,
                          gnet: bool = True, gloc: bool = True, net_load_mode: str = 'http',
                          greg: bool = True) -> Optional[dict]:
    return get_parameter(tag, function_id, 'summary', gnet, gloc, net_load_mode, greg)


def list_summary_parameter(tag: str, function_ids: List[int],
                           gnet: bool = True, gloc: bool = True, net_load_mode: str = 'http',
                           greg: bool = True) -> Dict[int, dict]:
    return list_parameter(tag, function_ids, 'summary', gnet, gloc, net_load_mode, greg)
