"""

@create on 2020.02.28
"""
from typing import List, Union, Optional, Type

from scdap.data.result import Result
from scdap.data.container import Container

from .rapi import ResultAPI
from .capi import ContainerAPI


class APICreater(object):
    """
    算法需要调用数据接口并设置计算结果, 故需要提供相关的数据结构接口给算法类使用
    但是为了避免算法类在计算过程中误操作, 也就是做好数据结构结构的封装
    需要为算法提供其需要使用的接口, 不提供禁止使用的接口, 如识别算法只允许配置状态, 不允许配置健康度

    所以该类用于根据算法以及容器来创建对应的算法数据接口接口
    对于特征数据结构接口的具体实现在scdap.frame.api.capi中
    对于结果数据结构接口的具体实现在scdap.frame.api.rapi中

    数据容器接口基于一下两点进行设计

    1.在存在多个算法的情况下, 每一个算法可能需要配置不同细节的接口,
        比如对于评价算法, 通常的结果数据结构接口提供对所有健康度数值的配置,
        但对于A评价算法来说, 其应只被允许配置A算法关联的健康度, 而不应被允许配置B算法的健康度
        所以数据容器接口需要针对不同算法进行创建与生成, A算法拥有自己的容器接口, B算法拥有自己的容器接口, 两者不应互相影响与有所关联

    2.在存在多个设备的情况下, 不同设备间的数据接口同样也不应互相干扰, 不能发生设备A的数据容器接口能够修改设备B的数据的情况,
    所以每一个设备同样需要不同的数据容器接口, , 两者不应互相影响与有所关联

    根据上述两点, 于是就设计成如下情况：

    1.需要使用到两个变量用于标记[设备X-算法Y]的数据接口: BaseFunction.__findex__ / (Container.index / Result.index)
    2. 构建两个容器矩阵:

        # 特征数据容器接口
        _capi_list: [
                        [CAPI[fun1][dev1], CAPI[fun1][dev2], CAPI[fun1][dev3], ...],
                        [CAPI[fun2][dev1], CAPI[fun2][dev2], CAPI[fun2][dev3], ...],
                        [CAPI[fun3][dev1], CAPI[fun3][dev2], CAPI[fun3][dev3], ...],
                    ]

        # 结果数据容器接口
        _rapi_list: [
                        [RAPI[fun1][dev1], RAPI[fun1][dev2], RAPI[fun1][dev3], ...],
                        [RAPI[fun2][dev1], RAPI[fun2][dev2], RAPI[fun2][dev3], ...],
                        [RAPI[fun3][dev1], RAPI[fun3][dev2], RAPI[fun3][dev3], ...],
                    ]

        通过调用register_capi(function, container, ...)/register_rapi(function, result, ...)进行配置与注册
        通过调用get_capi(function, container)/getrapi(function, result)获取指定数据容器指定算法的数据容器接口

    """
    def __init__(self):
        self._capi_list: List[List[Optional[ContainerAPI]]] = list()
        self._rapi_list: List[List[Optional[ResultAPI]]] = list()

    def register_capi(self, function, container: Container,
                      kwargs: dict, api_class: Type[ContainerAPI] = None):
        """
        给定算法类与数据容器以及相关的配置以生成一个数据容器的API

        :param function: 算法类 BaseFunction
        :param container: 数据容器
        :param kwargs: API创建参数
        :param api_class: API实例类
        """
        self._registar_api(self._capi_list, function, container, api_class or ContainerAPI, kwargs)

    def register_rapi(self, function, result: Result,
                      kwargs: dict, api_class: Type[ResultAPI] = None):
        """
        给定算法类与数据容器以及相关的配置以生成一个数据容器的API

        :param function: 算法类 BaseFunction
        :param result: 数据容器
        :param kwargs: API创建参数
        :param api_class: API实例类
        """
        self._registar_api(self._rapi_list, function, result, api_class or ResultAPI, kwargs)

    def _registar_api(self, api_list: List[List[Union[ContainerAPI, ResultAPI, None]]], function,
                      obj: Union[Container, Result], api_class: Type[Union[ContainerAPI, ResultAPI]],
                      kwargs: dict):
        from ..function import BaseFunction
        function: BaseFunction

        while len(api_list) <= function.__findex__:
            api_list.append([])

        fapi = api_list[function.__findex__]

        while len(fapi) <= obj.index:
            fapi.append(None)

        fapi[obj.index] = api_class.__new__(api_class, obj, function, **kwargs)

    def get_capi(self, function, container: Container) -> ContainerAPI:
        """
        根据算法以及容器索引指定的container容器api

        :param function: 算法类 BaseFunction
        :param container: container数据容器类
        :return: 数据容器api
        """
        return self._capi_list[function.__findex__][container.index]

    def get_rapi(self, function, result: Result) -> ResultAPI:
        """
        根据算法以及容器索引指定的result容器api

        :param function: 算法类 BaseFunction
        :param result: result数据容器类
        :return: 数据容器api
        """
        return self._rapi_list[function.__findex__][result.index]
