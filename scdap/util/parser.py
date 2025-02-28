"""

@create on: 2021.01.13
"""
__all__ = ['parser_id']


def parser_id(name: str) -> int:
    """
    解析名称, 通过算法名称获取编号

    :param name: 名称
    :return: 编号
    """
    size = len(name)
    function_id = None
    for i in range(size):
        if name[i].isdigit():
            function_id = int(name[i:])
            break
    if function_id is None:
        raise ValueError(
            f'name: [{name}] 命名规范错误,'
            f'请确保命名规范如下: <模块英文名称(str)>+<编号(int)>.'
        )
    return function_id

