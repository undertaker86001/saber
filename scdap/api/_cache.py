"""

@create on: 2021.05.28
缓存层装饰器接口
"""
from functools import wraps

__cache__ = dict()


def _make_key(fname: str, keys: list, args: tuple, kwargs: dict) -> str:
    args_size = len(args)
    keys_size = len(keys)
    if args_size == keys_size:
        key = [fname, *args]
    elif args_size < keys_size:
        key = [fname, *args] + [kwargs[key] for key in keys[args_size:]]
    else:
        key = []
    return '.'.join(map(str, key))


def cache_wrapper(keys_list: list):
    """
    缓存装饰器
    可用于缓存部分接口数据
    但是需要小心, 部分接口的数据会频繁变动
    :param keys_list:
    :return:
    """

    def _cache_wrapper(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            from_cache = kwargs.get('from_cache', False)
            if from_cache:
                key = _make_key(f.__name__, keys_list, args, kwargs)
                if key in __cache__:
                    return __cache__[key]
                result = f(*args, **kwargs)
                __cache__[key] = result
                return result
            else:
                return f(*args, **kwargs)

        return wrapper

    return _cache_wrapper


def cache_function(cache: dict, function, key, *args, **kwargs):
    if key in cache:
        return cache[key]
    result = function(*args, **kwargs)
    if result:
        cache[key] = result
    return result
