"""

@create on: 2021.05.20
"""
import warnings
import ast
from datetime import datetime
from typing import List, Tuple

from numpy import ndarray

from ..base import ItemList, ICollection
from .item import FeatureItem, __feature_key__
from scdap.logger import logger


class IFeature(ICollection):
    __slots__ = __feature_key__
    meanhf: List[float]
    meanlf: List[float]
    mean: List[float]
    std: List[float]

    # 数据时间戳
    time: List[datetime]

    # 高分特征
    feature1: List[ndarray]
    feature2: List[ndarray]
    feature3: List[ndarray]
    feature4: List[ndarray]
    # 高分特征时间戳
    # 根据前后时间生成
    hrtime: List[List[datetime]]

    bandspectrum: List[ndarray]

    peakfreqs: List[ndarray]
    peakpowers: List[ndarray]

    # 由后端提供的设备状态
    # 一般情况下该字段无用, 在个别情况可能有用
    # 比如由厂商提供了设备状态的接口至后端, 则后端可以通过此获取
    status: List[int]
    customfeature: List[ndarray]
    temperature: List[int]
    # 其他传感器数据
    extend: List[str]


class FeatureList(ItemList[FeatureItem]):
    """
    特征数据结构集合
    """
    __slots__ = ['algorithm_id', 'node_id']

    def __init__(self, algorithm_id: str = '', node_id: int = 0, column: List[str] = None, maxlen: int = None):
        self.algorithm_id = algorithm_id
        self.node_id = node_id
        column = column or IFeature.__slots__
        ItemList.__init__(self, FeatureItem, IFeature(column), maxlen, column)

    def __str__(self) -> str:
        return f'{type(self).__name__}: ' \
               f'algorithm_id={self.algorithm_id}, ' \
               f'node_id={self.node_id}, ' \
               f'size={self.size()}, ' \
               f'column={self._select_keys}'

    def sub_itemlist(self, start: int = None, stop: int = None):
        start = start if start is not None else 0
        stop = stop if stop is not None else self.size() - 1
        sub_itemlist = type(self)(self.algorithm_id, self.node_id, self._select_keys, self._maxlen)
        cache = dict()
        for key in self._select_keys:
            cache[key] = getattr(self._item_list, key)[start:stop]
        sub_itemlist.extend_ldict(**cache)
        return sub_itemlist

    def get_algorithm_id(self) -> str:
        """
        获取算法点位编号

        :return: 算法点位编号
        """
        return self.algorithm_id

    def get_device_id(self) -> str:
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        return self.algorithm_id

    def get_node_id(self) -> int:
        """
        获取后端使用的nodeId编号

        :return: 算法点位编号
        """
        return self.node_id

    def get_time(self, index: int = None) -> datetime:
        """
        获得指定index时间

        :param index: 需要获取的index, 默认获取内置的index
        :return: 时间
        """
        return self.get_value('time', index)

    def get_all_time(self, start: int = None, stop: int = None) -> List[datetime]:
        """
        获得所有时间

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 所有时间列表
        """
        return self.get_range('time', start, stop)

    def get_lrdata(self, index: int = None) -> Tuple[float, float, float, float]:
        """
        获得指定index的低分数据

        :param index: 需要获取的index, 默认获取内置的index
        :return: 低分数据
        """
        return self.get_meanhf(index), self.get_meanlf(index), self.get_mean(index), self.get_std(index)

    def get_all_lrdata(self, start: int = None, stop: int = None) \
            -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        获得所有低分数据，形状为(4, x)，x为数据量

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 所有低分数据
        """
        return self.get_all_meanhf(start, stop), self.get_all_meanlf(start, stop), \
            self.get_all_mean(start, stop), self.get_all_std(start, stop)

    def get_hrdata(self, index: int = None) \
            -> Tuple[ndarray, ndarray, ndarray, ndarray]:
        """
        获得指定index的高分数据

        :param index: 需要获取的index, 默认获取内置的index
        :return: 高分数据
        """
        # 建议获得的时候暂存于方法本地，不再多次使用
        return self.get_feature1(index), self.get_feature2(index), self.get_feature3(index), self.get_feature4(index)

    def get_all_hrdata(self, start: int = None, stop: int = None) \
            -> Tuple[List[ndarray], List[ndarray], List[ndarray], List[ndarray]]:
        """
        获得所有高分数据，形状为(4, x, y)，x为高分维度(24)，y为数据量

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 所有低分数据
        """
        return self.get_all_feature1(start, stop), self.get_all_feature2(start, stop), \
            self.get_all_feature3(start, stop), self.get_all_feature4(start, stop)

    def get_hrtime(self, index: int = None) -> List[datetime]:
        """
        获得指定index的高分时间，为一个包含高分分辨率(24)的一维时间数组[datetime, datetime, datetime, ...]

        :param index: 需要获取的index, 默认获取内置的index
        :return: 高分时间
        """
        return self.get_value('hrtime', index)

    def get_all_hrtime(self, start: int = None, stop: int = None) -> List[List[datetime]]:
        """
        获得所有高分时间，形状为(x, y)，x为数据量，y为高分分辨率(24)

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 所有低分数据
        """
        return self.get_range('hrtime', start, stop)

    def get_meanhf(self, index: int = None) -> float:
        """
        获取指定index的摩擦特征数值

        :param index: 需要获取的index, 默认获取内置的index
        :return: 摩擦特征数值
        """
        return self.get_value('meanhf', index)

    def get_all_meanhf(self, start: int = None, stop: int = None) -> List[float]:
        """
        获取所有摩擦特征数值

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 摩擦特征列表
        """
        return self.get_range('meanhf', start, stop)

    def get_meanlf(self, index: int = None) -> float:
        """
        获取指定index的振动特征数值

        :param index: 需要获取的index, 默认获取内置的index
        :return: 振动特征数值
        """
        return self.get_value('meanlf', index)

    def get_all_meanlf(self, start: int = None, stop: int = None) -> List[float]:
        """
        获取所有振动特征数值

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 振动特征列表
        """
        return self.get_range('meanlf', start, stop)

    def get_mean(self, index: int = None) -> float:
        """
        获取指定index的功率特征数值

        :param index: 需要获取的index, 默认获取内置的index
        :return: 功率特征数值
        """
        return self.get_value('mean', index)

    def get_all_mean(self, start: int = None, stop: int = None) -> List[float]:
        """
        获取所有功率特征数值

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 功率特征列表
        """
        return self.get_range('mean', start, stop)

    def get_std(self, index: int = None) -> float:
        """
        获取指定index的质量特征数值

        :param index: 需要获取的index, 默认获取内置的index
        :return: 质量特征数值
        """
        return self.get_value('std', index)

    def get_all_std(self, start: int = None, stop: int = None) -> List[float]:
        """
        获取所有质量特征数值

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 质量特征列表
        """
        return self.get_range('std', start, stop)

    def get_feature1(self, index: int = None) -> ndarray:
        """
        获取指定index的高分摩擦特征数值，包含高分分辨率(24)个数据

        :param index: 需要获取的index, 默认获取内置的index
        :return: 高分摩擦特征数值
        """
        return self.get_value('feature1', index)

    def get_all_feature1(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有高分摩擦特征数值，形状为(x, y) x为数据量，y为高分分辨率(24)

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 高分摩擦特征数值列表
        """
        return self.get_range('feature1', start, stop)

    def get_feature2(self, index: int = None) -> ndarray:
        """
        获取指定index的高分振动特征数值，包含高分分辨率(24)个数据

        :param index: 需要获取的index, 默认获取内置的index
        :return: 高分振动特征数值
        """
        return self.get_value('feature2', index)

    def get_all_feature2(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有高分振动特征数值，形状为(x, y) x为数据量，y为高分分辨率(24)

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 高分振动特征数值列表
        """
        return self.get_range('feature2', start, stop)

    def get_feature3(self, index: int = None) -> ndarray:
        """
        获取指定index的高分功率特征数值，包含高分分辨率(24)个数据

        :param index: 需要获取的index, 默认获取内置的index
        :return: 高分功率特征数值
        """
        return self.get_value('feature3', index)

    def get_all_feature3(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有高分功率特征数值，形状为(x, y) x为数据量，y为高分分辨率(24)

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 高分功率特征数值列表
        """
        return self.get_range('feature3', start, stop)

    def get_feature4(self, index: int = None) -> ndarray:
        """
        获取指定index的高分质量特征数值，包含高分分辨率(24)个数据

        :param index: 需要获取的index, 默认获取内置的index
        :return: 高分质量特征数值
        """
        return self.get_value('feature4', index)

    def get_all_feature4(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有高分质量特征数值，形状为(x, y) x为数据量，y为高分分辨率(24)

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 高分质量特征数值列表
        """
        return self.get_range('feature4', start, stop)

    def get_bandspectrum(self, index: int = None) -> ndarray:
        """
        获取指定index的分频特征数据列表
        默认使用内置的index，同时亦可使用自定义index位置

        :param index: 需要获取的index, 默认获取内置的index
        :return: 分频特征数据
        """
        return self.get_value('bandspectrum', index)

    def get_all_bandspectrum(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有分频特征数据列表

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 分频特征数据列表
        """
        return self.get_range('bandspectrum', start, stop)

    def get_peakfreqs(self, index: int = None) -> ndarray:
        """
        获取指定index的peakfreqs

        :param index: 需要获取的index, 默认获取内置的index
        :return: peakfreqs
        """
        return self.get_value('peakfreqs', index)

    def get_all_peakfreqs(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有peakfreqs

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: peakfreqs列表
        """
        return self.get_range('peakfreqs', start, stop)

    def get_peakpowers(self, index: int = None) -> ndarray:
        """
        获取指定index的peakpowers

        :param index: 需要获取的index, 默认获取内置的index
        :return: peakpowers
        """
        return self.get_value('peakpowers', index)

    def get_all_peakpowers(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取peakpowers

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: peakpowers
        """
        return self.get_range('peakpowers', start, stop)

    def get_status(self, index: int = None) -> int:
        """
        获取指定index的状态，该状态由java端或橙盒端设置，非算法计算的状态

        :param index: 需要获取的index, 默认获取内置的index
        :return: 状态
        """
        return self.get_value('status', index)

    def get_all_status(self, start: int = None, stop: int = None) -> List[int]:
        """
        获取所有状态

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 状态数据列表
        """
        return self.get_range('status', start, stop)

    def get_customfeature(self, index: int = None) -> ndarray:
        """
        获取指定index的定制化特征数据列表
        默认使用内置的index，同时亦可使用自定义index位置

        :param index: 需要获取的index, 默认获取内置的index
        :return: 定制特征数据
        """
        return self.get_value('customfeature', index)

    def get_all_customfeature(self, start: int = None, stop: int = None) -> List[ndarray]:
        """
        获取所有定制化特征数据列表

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 分频特征数据列表
        """
        return self.get_range('customfeature', start, stop)

    def get_temperature(self, index: int = None) -> int:
        """
        获取指定index的温度数据

        默认使用内置的index，同时亦可使用自定义index位置

        :param index: 需要获取的index, 默认获取内置的index
        :return: 定制特征数据
        """
        return self.get_value('temperature', index)

    def get_all_temperature(self, start: int = None, stop: int = None) -> List[int]:
        """
        获取所有温度数据列表

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 温度特征数据列表
        """
        return self.get_range('temperature', start, stop)

    def get_extend(self, index: int = None) -> dict:
        """
        获取指定index的传感器数据

        默认使用内置的index，同时亦可使用自定义index位置

        :param index: 需要获取的index, 默认获取内置的index
        :return: 扩展特征数据
        """
        data = self.get_value('extend', index)
        if isinstance(data, str):
            # 如果是字符串类型的字典和列表，尝试还原成原来类型
            try:
                data = ast.literal_eval(data)
            except Exception as e:
                # 如果上面字符串转换失败，则保留原有的字符串
                logger.warning(f"获取的特征值本身({data})为不可还原的字符串，无法转为字典或列表，将保持原有的字符串类型.")
                data = data
        return data

    def get_all_extend(self, start: int = None, stop: int = None) -> List[dict]:
        """
        获取所有传感器数据列表

        :param start: 选择起始时间, 默认为最初位置
        :param stop: 选择结束时间，默认为最后位置
        :return: 扩展特征数据列表
        """
        return self.get_range('extend', start, stop)


