"""

@create on: 2021.01.21
"""

from abc import ABCMeta
from typing import Optional, Dict, List

from .base import BaseFunction
from ..api import ContainerAPI, ResultAPI


TYPE_DC = Optional[Dict[str, ContainerAPI]]
TYPE_DR = Optional[Dict[str, ResultAPI]]


class MDBaseFunction(BaseFunction, metaclass=ABCMeta):
    container: TYPE_DC
    # 计算结果结果容器
    result: TYPE_DR
