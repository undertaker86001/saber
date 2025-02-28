"""

@create on: 2020.12.15
"""
__all__ = ['fset', 'BaseFunction']

import os
from re import match
from importlib import import_module
from typing import Dict, List, Type, Tuple, Set, Union

from scdap import config
from scdap.util import implib
from scdap.util.parser import parser_id

from .base import BaseFunction


class FunctionDetail(object):
    def __init__(self, name: str, function_id: int, path: str, function_type: str, module: str):
        self.name = name
        self.function_id = function_id
        self.path = path
        self.function_type = function_type
        self.module = module


class FunctionSet(object):
    _include_extension = {'so', 'pyd', 'py'}
    _exclude = ['__init__', '__pycache__', '.idea']
    _exlucde_p = r'^%s' % '|'.join(_exclude)

    _base_type = {'decision', 'evaluation', 'other', 'summary'}

    def __init__(self, package: str = None):
        self._base_dir = None
        self._package = None
        self._version = None
        # 算法编号至算法详情的映射
        self._fid2fdetail: Dict[int, FunctionDetail] = dict()
        # 算法名称至算法详情的映射
        self._fname2fdetail: Dict[str, FunctionDetail] = dict()
        # 按算法类型分类, 算法编号至算法详情的映射
        self._type_fid2fdetail: Dict[str, Dict[int, FunctionDetail]] = {t: dict() for t in self._base_type}
        # 按算法类型分类, 算法名称至算法详情的映射
        self._type_fname2fdetail: Dict[str, Dict[str, FunctionDetail]] = {t: dict() for t in self._base_type}
        # 暂存的算法
        self._register_fid: Dict[int, Type[BaseFunction]] = dict()
        self._register_fname: Dict[str, Type[BaseFunction]] = dict()
        self.load(package)

    def load(self, package: str = None):
        """
        导入算法
        从package中导入所有算法
        假设package = scdap_algorithm.function
        则scdap_algorithm.function目录结构为：
        scdap_algorithm.function
            .decision
            .evaluation
            .other
            .summary

        该方法会从四个子目录中导入所有算法，详细的逻辑请看self.search_function()

        :param package:
        :return:
        """
        package = package or config.FUNCTION_LIB
        try:
            module = import_module(package)
        except:
            print(f'算法模块: {package}读取失败.')
            return
        self._base_dir = os.path.dirname(module.__file__)
        self._package = package
        self.search_function()

    def reload(self, package: str = None):
        """
        重新导入算法
        """
        self.clear()
        self.load(package)

    def clear(self):
        self._fid2fdetail.clear()
        self._fname2fdetail.clear()
        self._type_fid2fdetail = {t: dict() for t in self._base_type}
        self._type_fname2fdetail = {t: dict() for t in self._base_type}
        self._register_fid.clear()
        self._register_fname.clear()

    def get_function_types(self) -> Set[str]:
        return self._base_type.copy()

    def register_function(self, function: Type[BaseFunction]):
        """
        临时注册算法至算法类框架中, 注意临时注册的算法只缓存在一个缓存的空间中

        :param function: 算法类
        """
        function_name = function.get_function_name()
        function_id = parser_id(function_name)
        self._register_fname[function_name] = function
        self._register_fid[function_id] = function

    def search_function(self):
        """
        历遍目录查找所有存在的算法
        查找的时候将会以function名称作为导入的模块名称进行算法的导入
        """
        if not os.path.exists(self._base_dir):
            return

        # print(function_dir, module)
        for ftype in self._base_type:
            fdir = os.path.join(self._base_dir, ftype)

            if not os.path.exists(fdir):
                continue

            fmodule = '.'.join([self._package, ftype])
            tfid2fdetail = self._type_fid2fdetail[ftype]
            tname2fdetail = self._type_fname2fdetail[ftype]
            for fpath in os.listdir(fdir):
                # 无关文件/目录

                if match(self._exlucde_p, fpath):
                    continue

                split = fpath.split('.')
                # 文件扩展名
                if not os.path.isdir(os.path.join(fdir, fpath)) and split[-1] not in self._include_extension:
                    continue
                function_name = split[0]
                function_id = parser_id(function_name)

                fdetail = FunctionDetail(
                    function_name, function_id,
                    os.path.join(fdir, fpath),
                    ftype, '.'.join([fmodule, function_name])
                )
                if function_id in self._fid2fdetail:
                    raise ValueError(f'同时存在两个算法:{function_name} | {self._fid2fdetail[function_id].name}'
                                     f'的算法编号为: {function_id}')
                tfid2fdetail[function_id] = self._fid2fdetail[function_id] = fdetail
                tname2fdetail[function_name] = self._fname2fdetail[function_name] = fdetail

    def check(self, names: List[str]) -> Tuple[List[str], List[Tuple[str, str]]]:
        """
        检查该库中所有代码是否规范的进行了编写与设计

        :return: 可导入的controller列表，导入失败的列表
        """
        names = names or self.get_function_names()
        return names, implib.check(names, self._import_module, BaseFunction)

    def get_function_names(self, function_type: str = None) -> List[str]:
        """
        获取存在的算法名称

        :param function_type: 算法类型, 如果为None则返回所有名称
        :return: 存在的算法名称列表
        """
        if function_type is None:
            return list(self._fname2fdetail.keys())
        if function_type not in self._base_type:
            raise Exception(f'参数function_type错误, 请确保function_type属于{list(self._base_type)}.')
        return list(self._type_fname2fdetail[function_type].keys())

    def get_function_ids(self, function_type: str = None) -> List[int]:
        """
        获取存在的算法编号

        :param function_type: 算法类型, 如果为None则返回所有编号
        :return: 存在的算法编号列表
        """
        if function_type is None:
            return list(self._fid2fdetail.keys())
        if function_type not in self._base_type:
            raise Exception(f'参数function_type错误, 请确保function_type属于{list(self._base_type)}.')
        return list(self._type_fid2fdetail[function_type].keys())

    def get_function_id(self, function_name: str) -> int:
        """
        根据算法名称解析算法编号

        :param function_name: 算法名称
        :return: 算法编号
        """
        return parser_id(function_name)

    def get_function_path(self, function_name: str = None, function_id: int = None) -> str:
        """
        获取算法的所在位置路径
        两个参数必须二选一, function_name优先度高

        :param function_name: 算法名称
        :param function_id: 算法编号
        :return: 路径
        """
        if function_name:
            function = self._fname2fdetail[function_name]
        elif function_id:
            function = self._fid2fdetail[function_id]
        else:
            raise Exception('请至少输入一个参数.')
        return function.path

    def _import_module(self, module: str) -> Type[BaseFunction]:
        """
        导入算法类并获得算法类

        :param module: 算法类的导入路径
        :return: 算法类
        """
        return implib.import_class(module, '', 'function')

    def get_function_class(self, function_name: str = None, function_id: int = None) -> Type[BaseFunction]:
        """
        根据算法名称算法编号或者获取算法类
        两个参数必须二选一, function_name优先度高

        :param function_name: 算法名称
        :param function_id: 算法编号
        :return: 算法类型
        """
        if function_name:
            function = self._register_fname.get(function_name)
            if function:
                return function
            function = self._fname2fdetail.get(function_name)
            if function is None:
                raise Exception(f'无法找到算法名称: [{function_name}], 请按照如下顺序排查: '
                                f'1).如果是自行设计的算法, 且为提交至scdap_algorithm算法库中, '
                                f'则调用scdap.frame.function.fset.register_function(YourFunction)进行算法类的注册; '
                                f'2).请检查"function": "your_function"配置的算法名字无误;'
                                f'3).请检查算法类中get_health_define()配置了正确的名字; '
                                f'4).请确保算法类放置的目录位置是否正确;'
                                f'5).请确保算法类放置的文件夹或者是文件名称与配置的get_health_define()一致;'
                                f'6).请联系管理员排查;')
            return self._import_module(function.module)

        elif function_id:
            function = self._register_fid.get(function_id)
            if function:
                return function
            function = self._fid2fdetail.get(function_id)
            if function is None:
                raise Exception(f'无法找到算法编号: [{function_id}], 请按照如下顺序排查: '
                                f'1).如果是自行设计的算法, 且为提交至scdap_algorithm算法库中, '
                                f'则调用scdap.frame.function.fset.register_function(YourFunction)进行算法类的注册; '
                                f'2).请检查"function": "your_function"配置的算法名字无误;'
                                f'3).请检查算法类中get_health_define()配置了正确的名字; '
                                f'4).请确保算法类放置的目录位置正确;'
                                f'5).请确保算法类放置的文件夹或者是文件名称与配置的get_health_define()一致;'
                                f'6).请联系管理员排查;')
            return self._import_module(function.module)

        else:
            raise Exception('请至少输入一个参数.')

    def check_function_type(self, function: Union[str, int], function_type: str):
        if function_type not in self._base_type:
            raise ValueError(f'function_type必须为{self._base_type}')

        if isinstance(function, int):
            if function not in self._fid2fdetail:
                raise ValueError(f'无法找到算法: {function}')
            return self._fid2fdetail[function].function_type == function_type
        elif isinstance(function, str):
            if function not in self._fname2fdetail:
                raise ValueError(f'无法找到算法: {function}')
            return self._fname2fdetail[function].function_type == function_type
        else:
            raise ValueError(f'function类型错误.')

    def get_decision_function_names(self) -> List[str]:
        return self.get_function_names('decision')

    def get_evaluation_function_names(self) -> List[str]:
        return self.get_function_names('evaluation')

    def get_other_function_names(self) -> List[str]:
        return self.get_function_names('other')

    def get_summary_function_names(self) -> List[str]:
        return self.get_function_names('summary')

    def get_decision_function_ids(self) -> List[int]:
        return self.get_function_ids('decision')

    def get_evaluation_function_ids(self) -> List[int]:
        return self.get_function_ids('evaluation')

    def get_other_function_ids(self) -> List[int]:
        return self.get_function_ids('other')

    def get_summary_function_ids(self) -> List[int]:
        return self.get_function_ids('summary')


fset = FunctionSet()
