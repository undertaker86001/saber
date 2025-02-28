"""

@create on: 2021.01.05
"""
from itertools import chain


def flatten(array):
    """
    拉平列表
    """
    return list(chain.from_iterable(array))


def flatten_generator(array):
    yield chain.from_iterable(array)
