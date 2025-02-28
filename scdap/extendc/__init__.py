"""

@create on 2021.01.08
扩展的控制器库
主要在进程配置中的: extra.before_controller/extra.after_controller中使用
"""
import os
from typing import Type, List, Tuple
from scdap.core.controller import BaseController
from scdap.util import implib


def get_controller_class(name: str) -> Type[BaseController]:
    """
    按需导入控制器

    :param name: 需要获取的控制器名称

    :return: 控制器类型
    """
    return implib.import_class(__package__, name, 'controller_class')


def get_controller_names() -> List[str]:
    """
    获取所有拥有的控制器名称

    :return: 控制器名称列表
    """
    return implib.get_lib_names(os.path.dirname(__file__), ['base.py'])


def check_controllers() -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    检查该库中所有controller代码是否规范的进行了编写与设计

    :return: 可导入的controller列表，导入失败的列表
    """
    names = get_controller_names()
    return names, implib.check(names, get_controller_class, BaseController)
