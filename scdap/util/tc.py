"""

@create on 2020.02.24

类型转换
type convert
"""
import os
import json
import pickle
from time import time
from typing import Union
from functools import partial
from datetime import datetime, timedelta

import numpy as np

LOG = np.log
EXP = np.exp
ARANGE = np.arange
ROUND = partial(np.round, decimals=1)
FROMTIMESTAMP = datetime.fromtimestamp
FROMSTRING = partial(np.fromstring, dtype=np.float)

DATETIME_MIN = datetime(2000, 1, 1)
DATETIME_MIN_TIMESTAMP = int(DATETIME_MIN.timestamp() * 1000)


def time_ms() -> int:
    """
    获取时间戳单位为ms
    """
    return int(time() * 1000)


def time_s() -> float:
    """
    获取时间戳单位为s
    """
    return time()


def lrtime_to_hrtime(previous: datetime, current: datetime, reso: int) -> np.ndarray:
    """
    将低分时间戳转换为高分时间戳

    :param previous: 前一个数据的数据时间
    :param current: 当前数据的数据时间
    :param reso: 分辨率
    :return: 由reso个时间组成的时间数组
    """
    if previous is None or previous >= current:
        previous = current - timedelta(seconds=1)
    return ARANGE(previous, current, (current - previous) / reso, timedelta)[-reso:]


def datetime_to_long(t: datetime, ms: bool = True) -> int:
    """
    将类型datetime转换为整型的时间戳
    该方法用于转换datetime类型至java端可用的时间戳
    *注意*各个不同程序语言的时间戳格式略有不同

    :param t: datetime
    :param ms: 是否是微秒时间戳
    :return: 时间戳
    """
    if isinstance(t, int):
        return t
    t = t.timestamp()
    if ms:
        t *= 1000
    return int(t)


def long_to_datetime(t: int, ms: bool = True, timestamp_shift: int = 0) -> datetime:
    """
    将整型时间戳转换为datetime
    该方法用于转换java端发送的时间戳至datetime

    :param t: java时间戳
    :param ms: 是否是微秒时间戳
    :param timestamp_shift: 时间偏移
    :return: datetime
    """
    if isinstance(t, datetime):
        return t
    elif isinstance(t, str):
        t = int(t)
    t += timestamp_shift
    if ms:
        t = t / 1000
    return FROMTIMESTAMP(t)


def get_next_time(current_time: datetime, delta: int):
    """
    获得下一次保存音频的时间
    """
    delta = timedelta(seconds=delta)
    next_time = (int((current_time - datetime.min) / delta) + 1) * delta + datetime.min
    return next_time


def string_to_array(string: str, sep: str = ',', exp: bool = False) -> np.ndarray:
    """
    分割字符串并且转型分割后的结果至ndarray

    :param string: 需要分割与转型的字符串
    :param sep: 分割的依据
    :param exp: 是否需要转换自然指数
    :return: 返回ndarray类型的数组
    """
    result = FROMSTRING(string, sep=sep)
    if exp:
        result = EXP(result)
    return result


def array_to_string(array: np.ndarray, sep: str = ',', ln: bool = False) -> str:
    """
    将ndarray反向解析成字符串

    :param array: 需要解析的数组
    :param sep: 分割的依据
    :param ln: 是否需要取对数
    :return: 字符串
    """
    if ln:
        array = LOG(array)
    array = ROUND(array)
    return sep.join(map(str, array))


def dict_to_str(data: Union[dict, list]) -> str:
    return json.dumps(data)


def load_json(path: str, default=None) -> dict:
    """
    读取配置并转换成dict

    :param path: 路径
    :param default: 当不存在文件时返回的默认数值
    :return: 配置
    """
    if not os.path.exists(path):
        return default
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)


def load_pickle(path: str, default=None) -> dict:
    """
    从pkl文件中读取内容

    :param path: 路径
    :param default: 当不存在文件时返回的默认数值
    :return: 配置
    """
    if not os.path.exists(path):
        return default
    with open(path, 'rb') as file:
        return pickle.load(file)
