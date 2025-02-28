"""

@create on: 2021.02.02
"""
import os
import json

from typing import Union, List, Tuple

from scdap.flag import option_key
from scdap.frame.function import BaseFunction
from scdap.frame.worker import get_worker_names


def dump_option(tag: str, worker: str, devices: List[str] = None,
                clock_time: int = None, decision: List[dict] = None,
                evaluation: List[dict] = None, other: List[dict] = None,
                enabled: bool = True, description: str = '', extra: dict = None,
                path: str = None):
    """
    根据配置的参数创建一个进程配置文件

    :param tag: 进程编号
    :param worker: 算法工作组类型
    :param devices: 设备组编号列表
    :param clock_time: 定时唤醒时间, 一般不需要配置
    :param decision: 识别算法配置列表
    :param evaluation: 评价算法配置列表
    :param other: 其他算法配置列表
    :param description: 描述与备注
    :param extra: 其他配置参数
    :param enabled: 是否弃用
    :param path: 保存的路径
    """
    option = {
        option_key.tag: tag,
        option_key.description: description,
        option_key.devices: devices or list(),
        option_key.worker: worker,
        option_key.decision: decision or list(),
        option_key.evaluation: evaluation or list(),
        option_key.other: other or list(),
        option_key.extra: extra or dict(),
        option_key.enabled: enabled
    }

    if clock_time:
        option[option_key.clock_time] = clock_time

    wnames = get_worker_names()
    if option[option_key.worker] not in wnames:
        raise Exception(f'配置了错误的worker, worker可用的配置有: {wnames}')

    if option_key.clock_time in option and option[option_key.clock_time] <= 0:
        raise Exception('配置的clock_time必须大于0.')
    flist = list(option[option_key.decision]) + list(option[option_key.evaluation]) + list(option[option_key.other])
    for f in flist:
        fname = f[option_key.function]
        if not isinstance(fname, str):
            f[option_key.function] = fname.get_function_name()

    if len(flist) == 0:
        raise Exception('请至少配置一个算法.')

    if path:
        path = os.path.abspath(path)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        with open(path, 'w+', encoding='utf-8') as file:
            json.dump(option, file, indent=2, ensure_ascii=False)

    return option


def _check_key(option: dict, key: str, type_list: Union[type, Tuple[type, ...]], must_has: bool = True,
               item_type: Union[type, Tuple[type, ...], None] = None) -> bool:
    val = option.get(key)
    if val is None:
        if must_has:
            raise ValueError(f'进程配置中不存在键: {key}.')
        return False

    if not isinstance(val, type_list):
        raise TypeError(f'进程配置中键: {key} 配置的数值类型错误, 类型必须为: {type_list}.')
    if item_type:
        for item in val:
            if not isinstance(item, item_type):
                raise TypeError(f'进程配置中键: {key} 配置的数值类型错误, 每一个元素的类型必须为: {item_type}.')
    return True


def check_option(option: dict, check_all: bool = True):
    """
    检查配置

    :param option: 待检查配置
    :param check_all: 是否强制检查所有必须存在的内容, 如果设置为False, 则只检查存在的内容
    """
    _check_key(option, option_key.tag, str, check_all)
    _check_key(option, option_key.worker, str, check_all)
    _check_key(option, option_key.devices, list, check_all, item_type=str)

    flist = list()
    if _check_key(option, option_key.decision, list, check_all, item_type=dict):
        _check_function_option(option, option_key.decision)
        flist += option[option_key.decision]

    if _check_key(option, option_key.evaluation, list, check_all, item_type=dict):
        _check_function_option(option, option_key.evaluation)
        flist += option[option_key.evaluation]

    if _check_key(option, option_key.other, list, check_all, item_type=dict):
        _check_function_option(option, option_key.other)
        flist += option[option_key.other]

    if check_all and not flist:
        raise ValueError(f'至少需要配置一个算法.')

    if _check_key(option, option_key.extra, dict, False):
        extra = option[option_key.extra]
        _check_key(extra, option_key.transfer_mode, str, False)
        _check_key(extra, option_key.email, str, False)

    _check_key(option, option_key.type, str, False)
    _check_key(option, option_key.clock_time, int, False)
    _check_key(option, option_key.description, str, False)


def _check_function_option(option: dict, key: str):
    for item in option[key]:
        f = item.get(option_key.function, None)
        if f is None:
            raise ValueError(f'进程配置的算法配置列表: {key} 中必须配置{option_key.function}.')
        if not isinstance(f, str) and not issubclass(f, BaseFunction):
            raise TypeError(f'进程配置的算法配置列表: {key} 中必须配置了错误的类型, 只允许配置如下类型: {[BaseFunction, str]}.')
        global_parameter = item.get(option_key.global_parameter, None)
        if global_parameter is not None and not isinstance(global_parameter, dict):
            raise TypeError(f'进程配置的算法配置列表: {key} 中配置了错误的类型, 只允许配置如下类型: dict.')
