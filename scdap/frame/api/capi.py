"""

@create on: 2021.01.19
"""
import warnings
from datetime import datetime
from typing import Tuple, List
from functools import partial

import numpy as np

from scdap.flag import column
from scdap.data import Container


class ContainerAPIEntity(object):
    def __init__(self, container: Container, function, *, allow_feature: bool = True):
        from scdap.frame.function import BaseFunction
        function: BaseFunction
        # 因为__getattr__被禁用, 所以无法使用getattr方法缓存一些配置
        # 故在此声明一个变量用于解决这个情况
        self.option = dict()

        self.get_algorithm_id = container.flist.get_algorithm_id
        self.get_device_id = container.flist.get_algorithm_id
        self.get_node_id = container.get_node_id
        self.get_column = function.get_column

        if not allow_feature:
            return

        # 对于api内接口，设置任何结果数据的数值使用的index都是内置的index，不允许使用自定义的index
        # 故使用partial提前设置index
        for col in function.get_column():
            fname = f'get_{col}'
            setattr(self, fname, partial(getattr(container.flist, fname), index=None))

        if column.has_all_lrdata(function.get_column()):
            self.get_lrdata = partial(container.flist.get_lrdata, index=None)

        if column.has_hrdata(function.get_column()):
            self.get_hrtime = partial(container.flist.get_hrtime, index=None)

        if column.has_all_hrdata(function.get_column()):
            self.get_hrdata = partial(container.flist.get_hrdata, index=None)

    def __getattr__(self, item):
        raise NotImplementedError(f'{item}()是没有登记的接口,请确保算法类已经配置好需要使用的column.')

    def __setstate__(self, state):
        """
        pickle需要调用的接口
        如果不重载该方法的话pickle在调用时会进入到__getattr__中导致抛出pickle不可接收的错误最终报错
        """
        super().__setstate__(state)

    def __getstate__(self):
        """
        pickle需要调用的接口
        如果不重载该方法的话pickle在调用时会进入到__getattr__中导致抛出pickle不可接收的错误最终报错
        """
        return super().__getstate__()


class ContainerAPI(object):
    """
    数据容器接口实现类
    该类通过__new__进行初始化, 将ContainerAPI替换为ContainerAPIEntity
    ContainerAPI类的主要作用在于提供接口IDE补全(联想)功能以及提供一个统一的入口用于创建接口类
    实际上创建的接口类是 ContainerAPIEntity
    这么实现的原因在于,
    如果直接使用ContainerAPI创建接口类, 则无法实现在提供接口IDE补全(联想)功能的同时隔离不允许调用的接口
    当然实际上可以做到, 需要在每一个接口写入 raise NotImplementedError(...) 但是为了实现对接口名称的针对性异常信息抛出
        ps.方便算法设计人员调试与定位问题
    需要每一个接口中的异常初始化信息都针对性的填写
    比如:
        def get_meanhf(self): raise NotImplementedError('get_meanhf() 没有实现...')
        def get_meanlf(self): raise NotImplementedError('get_meanlf() 没有实现...')
        ...
    这样实现起来太过麻烦, 所以直接在ContainerAPIEntity中定义__getattr__(self, item)
    只要接口有在__new__中进行配置就不会进入到__getattr__
    进入到__getattr__的都是没有在__new__中配置的接口, 就可以通过item实现接口名称的获取以及针对性的异常信息抛出

    rapi也是同样的操作
    """
    def __new__(cls, *args, **kwargs):
        return ContainerAPIEntity(*args, **kwargs)

    def get_column(self) -> List[str]:
        """
        获取可获取的特征名称列表

        :return: 特征列表
        """
        pass

    def get_algorithm_id(self) -> str:
        """
        获得算法点位编号

        :return: 算法点位编号
        """
        pass

    def get_device_id(self) -> str:
        """
        获得算法点位编号, 旧版本接口, 不再使用, 请使用get_algorithm_id()获取编号

        :return: 算法点位编号
        """
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        pass

    def get_node_id(self) -> int:
        """
        获得点位的后端编号

        :return: 点位的后端编号
        """
        pass

    def get_time(self) -> datetime:
        """
        获得时间

        :return: 时间
        """
        pass

    def get_lrdata(self) -> Tuple[float, float, float, float]:
        """
        获得低分数据

        :return: 低分数据
        """
        # 建议获得的时候暂存于方法本地，不再多次使用
        pass

    def get_hrdata(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        获得高分数据

        :return: 高分数据
        """
        # 建议获得的时候暂存于方法本地，不再多次使用
        pass

    def get_hrtime(self) -> List[datetime]:
        """
        获得高分时间，为一个包含高分分辨率(24)的一维时间数组[datetime, datetime, datetime, ...]

        :return: 高分时间
        """
        pass

    def get_meanhf(self) -> float:
        """
        获取摩擦特征数值

        :return: 摩擦特征数值
        """
        pass

    def get_meanlf(self) -> float:
        """
        获取振动特征数值

        :return: 振动特征数值
        """
        pass

    def get_mean(self) -> float:
        """
        获取功率特征数值

        :return: 功率特征数值
        """
        pass

    def get_std(self) -> float:
        """
        获取质量特征数值

        :return: 质量特征数值
        """
        pass

    def get_feature1(self) -> np.ndarray:
        """
        获取高分摩擦特征数值，包含高分分辨率(24)个数据

        :return: 高分摩擦特征数值
        """
        pass

    def get_feature2(self) -> np.ndarray:
        """
        获取高分振动特征数值，包含高分分辨率(24)个数据

        :return: 高分振动特征数值
        """
        pass

    def get_feature3(self) -> np.ndarray:
        """
        获取高分功率特征数值，包含高分分辨率(24)个数据

        :return: 高分功率特征数值
        """
        pass

    def get_feature4(self) -> np.ndarray:
        """
        获取高分质量特征数值，包含高分分辨率(24)个数据

        :return: 高分质量特征数值
        """
        pass

    def get_bandspectrum(self) -> np.ndarray:
        """
        获取分频特征数据列表

        :return: 分频特征数据
        """
        pass

    def get_peakfreqs(self) -> np.ndarray:
        """
        获取peakfreqs

        :return: peakfreqs
        """
        pass

    def get_peakpowers(self) -> np.ndarray:
        """
        获取peakpowers

        :return: peakpowers
        """
        pass

    def get_status(self) -> int:
        """
        获取状态，该状态由java端或橙盒端设置，非算法计算的状态

        :return: 状态
        """
        pass

    def get_customfeature(self) -> np.ndarray:
        """
        获取定制化的特征列表

        :return: 定制化特征列表
        """
        pass

    def get_temperature(self) -> int:
        """
        获取温度, 默认数值为-274(绝对零度)
        **注意** 温度传感器可能会出现几分钟一次的数值爆表情况, 请自行过滤

        :return: 温度
        """
        pass

    def get_extend(self) -> dict:
        """
        获取传感器数据, 如扫码枪
        数据默认为字符串, 需要自行解析
        以后所有新增的特征都在这里取
        通过前后端约定好的key名称来获取新的特征
        比如扫码枪SerialData
        那么self.container.get_extend().get('SerialData')就可以获取到新增的特征

        :return: 传感器数据
        """
        pass
