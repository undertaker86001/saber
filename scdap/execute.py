"""

@create on 2020.09.25
运行process的相关方法
包括：
运行主入口方法
其他进程控制一个process的相关方法
"""
__all__ = ['execute_process']

import os
import argparse
from typing import Union

from scdap.core.process_type import check_process_type, get_process_type


def import_process(process_type: str):
    """
    导入算法进程, 目前可用的有两种，分别为summary/program
    summary -> scdap.summary.process_class
    program -> scdap.program.process_class

    classtype = import_process("summary")
    等价于
    classtype = scdap.summary.process_class

    :param process_type: summary/program
    :return:
    """
    from scdap.util.implib import import_class
    return import_class(__package__, process_type, 'process_class')


def parser_arg(*args) -> argparse.Namespace:
    """
    指令参数设置

    :return: 指令的解析结果
    """
    parser = argparse.ArgumentParser(
        description='硕橙-算法服务: 核心框架库/算法进程'
    )

    parser.add_argument(
        '-t', '--type', default='program', dest='process_type',
        help='进程类型.', choices=list(get_process_type())
    )

    parser.add_argument(
        '-n', '--name', default=None, dest='name',
        help='待启动的算法进程的算法ID(algorithm_id).'
    )

    parser.add_argument(
        '-c', '--command', type=str, default='start', dest='command',
        choices=['start', 'stop', 'restart'],
        help='算法进程控制指令, 不再使用, 为了向前兼容故未删除'
    )

    parser.add_argument(
        '--conf-path', default=None, type=str, dest='conf_path',
        help='配置文件路径.'
    )
    parser.add_argument(
        '--nacos', default=None, type=str, dest='nacos',
        help='配置nacos参数获取的地址, 格式为: [https/http]://{user}:{password}@{nacos-url}/{namespace}/{group}/{data_id}'
    )
    if len(args) > 0:
        return parser.parse_args(args)
    return parser.parse_args()


def _parse_nacos(nacos: str):
    exce = Exception('格式错误, 必须为[https/http]://{user}:{password}@{nacos-url}/{namespace}/{group}/{data_id}')
    nacos = nacos.rstrip('/')

    if not nacos.startswith('http://') and not nacos.startswith('https://'):
        raise exce

    try:
        protocol, remain = nacos.split(':', 1)
        *_, remain = remain.split('//', 1)
        remain, namespace, group, data_id = remain.rsplit("/", 3)
        remain, url = remain.rsplit('@', 1)
        user, password = remain.split(':')
    except:
        raise exce

    return {
        "data_id": data_id,
        "namespace": namespace,
        "group": group,
        "url": f"{protocol}://{url}",
        "username": user,
        "password": password
    }


def execute_process(process_type: str = None, name: Union[int, str] = None, command: str = 'start',
                    conf_path: str = None, nacos: str = None):
    """
    通过输入指令生成一个program，如果传入了方法参数，则优先使用方法参数

    :param nacos: 配置nacos参数获取的地址, 格式为: [https/http]://{user}:{password}@{nacos-url}/{namespace}/{group}/{data_id}
    :param process_type: 进程类型
    :param command: 控制指令, 不再使用
    :param name: 进程名称，一般是算法点位编号
    :param conf_path: 配置文件目录
    """
    # 解析指令参数
    parser = parser_arg()
    name = name or parser.name
    process_type = process_type or parser.process_type
    command = command or parser.command
    conf_path = conf_path or parser.conf_path
    nacos = nacos or parser.nacos
    check_process_type(process_type)
    # 读取配置
    from .configure import config
    config.load(True, conf_path)

    # 从nacos中获取配置
    if nacos:
        from .configure import config
        config.load_from_nacos(**_parse_nacos(nacos))

    # 配置日志
    from .logger import logger
    if config.SHOW_STDOUT_DETAIL:
        logger.set_stdout()
    # 打印一些日志
    if nacos:
        logger.info(f"nacos地址为：{nacos}")
    # 解析名称
    from .gop.loc import join_process_name
    pname = join_process_name(name, process_type)

    if config.SAVE_LOG:
        # 初始化日志
        logger.initial(pname, config.LOG_DIR, **config.PROGRAM_LOG_PARAM)

    process_class = import_process(process_type)

    try:
        server = process_class(name)
    except Exception as e:
        logger.error('初始化进程失败.')
        logger.exception(e)
        raise

    # 该变量主要是管防止重复回收资源
    is_closed = False

    if os.name == 'posix':
        # linux的信号才有效
        # window的没用
        # 当然主要是针对k8s的资源回收的
        def close(sig, frame):
            nonlocal is_closed
            if not is_closed:
                server.close()
                is_closed = True
            raise SystemExit(0)

        import signal
        signal.signal(signal.SIGTERM, close)

    try:
        server.serve_forever()
    finally:
        if not is_closed:
            server.close()
