"""

@create on: 2021.01.02
"""
import os
from typing import Type, List

from scdap.util.implib import import_class, get_lib_names

from .base import *


def get_crimp_class(name: str) -> Type[CRImplementer]:
    """
    获取数据结构实例类

    :param name: 实例类名称
    :return:
    """
    return import_class(__package__, name, 'crimp_class')


def get_crimp_names() -> List[str]:
    """
    获取所有可使用的数据结构实例类

    :return:
    """
    return get_lib_names(os.path.dirname(__file__), ['base', 'util'])


def check() -> bool:
    """
    检查模块设计是否规范

    :return:
    """
    names = get_crimp_names()
    error_module = list()
    print(f'{__package__} 拥有如下模块: {names}')
    for name in names:
        package = '.'.join([__package__, name])
        print(f"检查模块: {package}")
        try:
            get_crimp_class(name)
        except Exception as e:
            print(f'模块 {package} 检查发生错误. {e}')
            error_module.append((package, name))
    for package, name in error_module:
        print(f'错误的模块: {package}')
    return len(error_module) == 0
