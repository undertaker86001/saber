"""

@create on: 2020.01.25
"""
__all__ = ['get_worker_names', 'get_worker_class', 'check_workers', 'BaseWorker']

import os
from typing import List, Type, Tuple

from scdap.util import implib

from .base import BaseWorker


def worker_exist(worker: str, get_all: bool = False) -> bool:
    """
    确认worker类型是否存在

    :param worker:
    :param get_all: 是否导入所有
    :return:
    """
    return worker in get_worker_names(get_all)


def get_worker_names(get_all: bool = False) -> List[str]:
    """
    获取所有算法工作组名称

    :param get_all: 是否导入所有

    """
    if get_all:
        return implib.get_lib_names(os.path.dirname(__file__), ['base', 'md_base'])
    return implib.get_lib_names_with_enabled(os.path.dirname(__file__), ['base', 'md_base'], lib_package=__package__)


def get_worker_class(name: str) -> Type[BaseWorker]:
    """
    按需导入算法工作组
    """
    return implib.import_class(__package__, name, 'worker_class')


def check_workers() -> Tuple[List[str], List[Tuple[str, str]]]:
    names = get_worker_names()
    return names, implib.check(names, get_worker_class, BaseWorker)
