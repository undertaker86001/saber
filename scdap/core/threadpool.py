"""

@create on: 2021.03.20
"""

from typing import Optional
from concurrent.futures import ThreadPoolExecutor, Future

from scdap import config


__pool__: Optional[ThreadPoolExecutor] = None


def submit(fun, *args, **kwargs) -> Future:
    global __pool__
    if __pool__ is None:
        __pool__ = ThreadPoolExecutor(config.API_THREADPOOL_SIZE)
    return __pool__.submit(fun, *args, **kwargs)
