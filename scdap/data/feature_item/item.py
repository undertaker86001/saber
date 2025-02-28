"""

@create on: 2021.05.20
"""
from typing import List
from datetime import datetime

import numpy as np
from numpy import ndarray

from scdap.util.tc import DATETIME_MIN
from ..base import RefItem
from functools import partial

# 默认的温度数值, 为绝对零度 - 1
DEFAULT_TEMPERATURE = -274
# 默认的初始化用方法, 用于初始化np.ndarray类型的列表
DEFAULT_ARRAY = partial(np.zeros, 0, dtype=np.float)

__feature_default__ = {
    'meanhf': 0.0,
    'meanlf': 0.0,
    'mean': 0.0,
    'std': 0.0,
    'status': 0,
    'feature1': DEFAULT_ARRAY(),
    'feature2': DEFAULT_ARRAY(),
    'feature3': DEFAULT_ARRAY(),
    'feature4': DEFAULT_ARRAY(),
    'bandspectrum': DEFAULT_ARRAY(),
    'peakfreqs': DEFAULT_ARRAY(),
    'peakpowers': DEFAULT_ARRAY(),
    'hrtime': [],
    'customfeature': DEFAULT_ARRAY(),
    'temperature': DEFAULT_TEMPERATURE,
    'time': DATETIME_MIN,
    'extend': {}
}

__feature_key__ = tuple(__feature_default__.keys())


class FeatureItem(RefItem):
    __default__ = __feature_default__.copy()
    __slots__ = __feature_key__

    meanhf: float
    meanlf: float
    mean: float
    std: float
    # 数据时间戳
    time: datetime
    # 高分特征
    feature1: ndarray
    feature2: ndarray
    feature3: ndarray
    feature4: ndarray
    # 高分特征时间戳
    # 根据前后时间生成
    hrtime: List[datetime]
    bandspectrum: ndarray
    peakfreqs: ndarray
    peakpowers: ndarray
    # 由后端提供的设备状态
    # 一般情况下该字段无用, 在个别情况可能有用
    # 比如由厂商提供了设备状态的接口至后端, 则后端可以通过此获取
    status: int
    # 定制特征
    customfeature: ndarray
    # 温度传感器
    temperature: int
    # 其他传感器数据
    # 需要算法自行解析内容
    extend: dict
