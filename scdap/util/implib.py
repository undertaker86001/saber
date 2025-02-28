"""

@create on: 2021.01.04
"""
__all__ = ['get_lib_names', 'import_class', 'check']

import os
from re import match
from typing import List, Callable, Type, Tuple, Union


def path_to_package(path: str) -> str:
    """
    将路径拆分为模块路径:
    ./aaa/bbb/ccc -> aaa.bbb.ccc

    :param path: 待解析的模块路径

    :return: 模块路径
    """
    # ./aaa/bbb/ccc -> /aaa/bbb/ccc
    if path.startswith('.'):
        path = path[2:]

    # aaa/bbb/ccc/ -> aaa/bbb/ccc
    if path.endswith('/') or path.endswith('\\'):
        path = path[:-1]

    sub_dir = list()
    while path:
        path, sub = os.path.split(path)
        sub_dir.insert(0, sub)

    if not sub_dir:
        raise Exception('不允许指定为当前根目录, 必须指定为根目录下的有一个路径.')
    return '.'.join(sub_dir)


def get_lib_names(lib_dir: str, exclude: List[str] = None, startwith: str = None, endswith: str = None) -> List[str]:
    """
    获取指定文件目录内所有可使用的库名称

    :return: 控制器名称列表
    """
    if not os.path.exists(lib_dir):
        raise Exception(f'库路径: {lib_dir}不存在.')

    cnames = list()
    exclude = set(exclude or list())
    exclude.update(('__pycache__', '.idea', '__init__'))
    f = r"^%s" % '|'.join(exclude)
    for fname in os.listdir(lib_dir):
        # 因为框架肯能会进行加密, 所以只通过匹配的方式
        if match(f, fname):
            continue
        if startwith and not fname.startswith(startwith):
            continue
        if endswith and not fname.endswith(endswith):
            continue
        cnames.append(fname.split('.')[0])
    return cnames


def get_lib_names_with_enabled(lib_dir: str, exclude: List[str] = None,
                               startwith: str = None, endswith: str = None,
                               lib_package: str = None) -> List[str]:
    names = get_lib_names(lib_dir, exclude, startwith, endswith)
    if lib_package is None:
        return names
    result = list()
    for name in names:
        enabled = import_class(lib_package, name, '__enabled__', True)
        if enabled:
            result.append(name)
    return result


def import_class(lib_dir: str, lib_name: str = None, val_name: str = None, default=None):
    """
    动态导入库

    :param default: 默认值
    :param lib_dir: 库路径, 必须为相对目录并且不允许指定为当前的根目录(./), 同样允许指定pacakge(scdap.function....)

    :param lib_name: 库路径下指定的某一个库名称, 可以为空

    :param val_name: 导入库内的指定变量, 如果为空则直接返回库模块

    :return: 库模块或者是库模块内指定变量
    """
    if lib_dir.find('/') == -1 or lib_dir.find('\\') == -1:
        package = lib_dir
    else:
        if lib_dir.startswith('/'):
            raise Exception('在动态导入库请配置相对路径而不是绝对路径.')

        if not os.path.exists(lib_dir):
            raise Exception(f'库路径: {lib_dir}不存在.')

        if not os.path.exists(os.path.join(lib_dir, '__init__.py')):
            raise Exception(f'库路径: {lib_dir}必须包含__init__.py文件.')

        package = path_to_package(lib_dir)

    from importlib import import_module
    if lib_name:
        package = '.'.join([package, lib_name])
    try:
        module = import_module(package)
        if val_name:
            module = getattr(module, val_name, default)

        if module is None:
            raise Exception(f'{package}.{val_name}是一个空变量.')
        return module
    except Exception as exce:
        raise Exception(f'模块: {package}.{val_name} 导入失败, {exce}')


def check(names: List[str], import_function: Callable[[str], Type],
          base_class: Union[List[Type], Type] = None) -> List[Tuple[str, str]]:
    """
    检查对应的类型能否导入
    一般是用来检查文件的编写规范是否正确

    :param names: 待检查的列表

    :param import_function: 导入方法, 由参数导入在方法内进行调用

    :param base_class: 必须继承的基类

    :return: 导入失败的名称列表
    """
    failed = list()
    for name in names:
        try:
            cls = import_function(name)
        except Exception as e:
            failed.append([name, str(e)])
            raise e
        if base_class and not issubclass(cls, base_class):
            failed.append((name, f'没有继承自基类: {base_class}'))
    return failed
