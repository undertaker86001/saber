"""

@create on: 2021.04.25
"""
__all__ = [
    'get_tags', 'get_option', 'update_option', 'upload_option',
    'add_option', 'tag_exist', 'remove_option', 'delete_option'
]

from typing import Union, List, Optional, Tuple

from sqlalchemy import Column, JSON, text
from sqlalchemy.dialects.mysql import TINYINT, VARCHAR

from scdap import config
from scdap.flag import option_key
from scdap.gop.check import check_option
from scdap.frame.worker import get_worker_names

from .common_flag import CommonFlag
from .sqlbase import Base, DapBaseItem, TableApi, check_type


class SummaryOptionKey(CommonFlag):
    tag = 'tag'
    devices = 'devices'
    worker = 'worker'
    clock_time = 'clock_time'
    decision = 'decision'
    evaluation = 'evaluation'
    functions = 'functions'
    other = 'other'
    extra = 'extra'


flag = SummaryOptionKey


class SummaryOptionItem(Base, DapBaseItem):
    __tablename__ = 'summary_option'

    tag = Column(flag.tag, VARCHAR(128), nullable=False, unique=True, comment='设备(节点)组主标识编号')
    worker = Column(flag.worker, VARCHAR(64), nullable=False, comment='算法工作组类型')
    clock_time = Column(
        flag.clock_time, TINYINT(10), nullable=False, server_default=text("'2'"),
        comment='算法进程定时运行间隔'
    )
    devices = Column(flag.devices, JSON, nullable=False, comment='设备组其他算法点位编号, list(str)')
    decision = Column(flag.decision, JSON, nullable=False, comment='识别算法, list(json)')
    evaluation = Column(flag.evaluation, JSON, nullable=False, comment='评价算法, list(json)')
    other = Column(flag.other, JSON, nullable=False, comment='其他算法, list(json)')
    functions = Column(flag.functions, JSON, nullable=False, comment='使用的算法, 从decision/evaluation/other解析而来.')
    extra = Column(flag.extra, JSON, nullable=False, comment='其他配置, json')


_api = TableApi(SummaryOptionItem)


def get_functions(decision: list = None, evaluation: list = None, other: list = None) -> list:
    return [fs[option_key.function] for fs in (decision or list()) + (evaluation or list()) + (other or list())]


def list_options(page_size: int = 0, page_num: int = 1,
                 enabled: Optional[bool] = None, session=None) -> Tuple[int, int, List[dict]]:
    """
    列出配置信息

    :param page_size: 需要获取的单页配置数量
    :param page_num: 页码
    :param enabled:
    :param session:
    :return:
    """
    check_type(page_size, 'page_size', int, False)
    check_type(page_num, 'page_num', int, False)
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.list(page_size, page_num, enabled, order_by_list=[option_key.tag], session=session)


def get_option(tag: Union[str, List[str], None] = None,
               enabled: Optional[bool] = None, session=None) -> Optional[dict]:
    """
    获取算法进程配置

    :param tag: 进程标识/算法点位编号/设备组编号
    :param enabled: 是否获取启用的配置
    :param session:
    :return: 获取的算法配置, 如果tag为int则返回字典，如果为列表则获取由tag与对应的配置组成的字典
    """
    check_type(tag, 'tag', (str, tuple, list), True)
    check_type(enabled, 'enabled', (bool, int), True)

    return _api.query_with_unique(SummaryOptionItem.tag, tag, enabled, session=session)


def get_tags(enabled: Optional[bool] = None, session=None) -> List[int]:
    """
    获取所有存在的配置的tag/algorithm_id/group_tag/设备

    :param enabled: 是否获取启用的配置
    :param session:
    :return: 存在的标签(设备)列表
    """
    check_type(enabled, 'enabled', (bool, int), True)

    return _api.get_exist_keys_with_unique(SummaryOptionItem.tag, enabled, session=session)


def tag_exist(tag: Union[str, List[str]], enabled: Union[bool, int] = None, last_one: bool = False, session=None) \
        -> bool:
    """
    确认某一个设备组标签是否存在

    :param tag: 标签
    :param enabled: 是否存在
    :param last_one: True->只要一个存在就返回True, False->只有所有点位都存在才返回True
    :param session:
    :return: 是否存在
    """
    check_type(tag, 'tag', (str, list))
    check_type(enabled, 'enabled', (bool, int), True)
    return _api.check_exist_with_unique(SummaryOptionItem.tag, tag, enabled, last_one, session=session)


def add_option(tag: str, worker: str,
               clock_time: int = None, enabled: Union[bool, int] = True,
               devices: List[str] = None, decision: List[dict] = None,
               evaluation: List[dict] = None, other: List[dict] = None,
               extra: dict = None, description: str = '',
               check_worker: bool = True, session=None):
    """
    新增配置

    :param tag: 设备组编号
    :param worker: 算法工作组类型
    :param clock_time: 进程唤醒间隔
    :param enabled: 是否启用
    :param devices: 设备组其他算法点位编号列表
    :param decision: 识别算法
    :param evaluation: 评价算法
    :param other: 其他算法
    :param extra: 其他配置参数
    :param description: 描述内容
    :param check_worker: 是否检验worker
    :param session:
    """
    devices = devices or list()
    devices = list(devices)
    if tag in devices:
        devices.remove(tag)

    decision = decision or list()
    evaluation = evaluation or list()
    other = other or list()
    extra = extra or dict()

    if tag in devices:
        devices.remove(tag)

    check_type(tag, 'tag', str)
    check_type(worker, 'worker', str)
    check_type(clock_time, 'clock_time', int, True)
    check_type(enabled, 'enabled', (bool, int))
    check_type(devices, 'devices', (list, tuple))
    check_type(decision, 'decision', (list, tuple))
    check_type(evaluation, 'evaluation', (list, tuple))
    check_type(other, 'other', (list, tuple))
    check_type(extra, 'extra', dict)
    check_type(description, 'description', str)

    if check_worker and worker not in get_worker_names():
        raise ValueError(f'worker 只允许配置如下内容: {list(get_worker_names())}')

    functions = get_functions(decision, evaluation, other)

    kv = {
        flag.tag: tag, flag.worker: worker,
        flag.clock_time: clock_time or config.PROGRAM_CLOCK_TIME,
        flag.devices: devices, flag.enabled: enabled, flag.functions: functions,
        flag.decision: decision, flag.evaluation: evaluation, flag.other: other,
        flag.extra: extra, flag.description: description
    }
    check_option(kv)

    _api.add(kv, session)


upload_option = add_option


def update_option(tag: str, worker: str = None,
                  clock_time: int = None, enabled: Union[bool, int] = None,
                  devices: List[str] = None, decision: List[dict] = None,
                  evaluation: List[dict] = None, other: List[dict] = None,
                  extra: dict = None, description: str = '',
                  check_worker: bool = True, session=None):
    """
    新增配置

    :param tag: 设备组编号
    :param worker: 算法工作组类型
    :param clock_time: 进程唤醒间隔
    :param enabled: 是否启用
    :param devices: 设备组其他算法点位编号列表
    :param decision: 识别算法
    :param evaluation: 评价算法
    :param other: 其他算法
    :param extra: 其他配置参数
    :param description: 描述内容
    :param check_worker: 是否检验worker正确性
    :param session:
    """
    kv = dict()
    check_type(tag, 'tag', str)
    check_type(worker, 'worker', str, True)
    check_type(clock_time, 'clock_time', int, True)
    check_type(enabled, 'enabled', (bool, int), True)
    check_type(devices, 'devices', (list, tuple), True)
    check_type(decision, 'decision', (list, tuple), True)
    check_type(evaluation, 'evaluation', (list, tuple), True)
    check_type(other, 'other', (list, tuple), True)
    check_type(extra, 'extra', dict, True)
    check_type(description, 'description', str, True)

    kv['tag'] = tag

    if worker is not None:
        if check_worker and worker not in get_worker_names():
            raise ValueError(f'worker 只允许配置如下内容: {list(get_worker_names())}')
        kv[flag.worker] = worker

    if clock_time is not None:
        kv[flag.clock_time] = clock_time
    if enabled is not None:
        kv[flag.enabled] = enabled
    if devices is not None:
        kv[flag.devices] = devices

    count = 0
    for i in (decision, evaluation, other):
        if i is not None:
            count += 1
    if count != 3 and count != 0:
        raise ValueError(f'如果要更新算法配置的话必须传入所有算法参数配置并进行修改.')

    if count == 3:
        functions = get_functions(decision, evaluation, other)
        kv[flag.functions] = functions

    if decision is not None:
        kv[flag.decision] = decision
    if evaluation is not None:
        kv[flag.evaluation] = evaluation
    if other is not None:
        kv[flag.other] = other

    if extra is not None:
        kv[flag.extra] = extra
    if description is not None:
        kv[flag.description] = description

    check_option(kv, False)
    _api.update_with_unique(SummaryOptionItem.tag, tag, kv, session=session)


def delete_option(tag: Union[List[str], str], session=None):
    """
    删除设备组配置

    :param tag: 设备组编号
    :param session:
    """
    check_type(tag, 'tag', (str, list, tuple))
    _api.delete_with_unique(SummaryOptionItem.tag, tag, session=session)


remove_option = delete_option
