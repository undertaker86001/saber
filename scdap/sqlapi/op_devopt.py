"""

@create on: 2021.02.02
在device_health/device_option/health_define三个库的基础上进行操作
请尽量使用该库
该库将对多个表接口进行同步的操作
即在更新device_option的时候会同步更新device_health库
"""
__all__ = [
    'add_option', 'delete_option', 'update_option',
    'get_option', 'get_tags', 'tag_exist',
    'get_device_health_tags', 'get_device_health'
]

from typing import List, Union

from scdap.api import device_define
from scdap.flag import option_key
from scdap.sqlapi import device_option, health_define, worker_define, function_define, device_health

_api = device_option._api


def add_option(tag: str, worker: str, clock_time: int = None,
               enabled: Union[bool, int] = True, devices: List[str] = None,
               decision: List[dict] = None, evaluation: List[dict] = None,
               other: List[dict] = None, extra: dict = None,
               description: str = '', force: bool = True):
    """
    新增配置至option_define, 同时配置设备健康度至device_health
    """
    if not worker_define.worker_define_exist(worker):
        raise ValueError(f'worker: {worker}不存在.')
    if device_health.device_health_exist([tag] + (devices or list())):
        raise ValueError(f'新增点位已经配置.')

    with _api.get_session() as session:
        if devices:
            delete_option(devices, session=session)

        device_option.add_option(
            tag, worker, clock_time, enabled, devices,
            decision, evaluation, other, extra, description,
            session=session
        )

        hids = _get_health_defines(decision, evaluation, other, session=session)
        _update_device_health(_format_devices(tag, devices), hids, session)


def _format_devices(tag: str, devices: List[str] = None):
    devices = devices or list()
    return list(set(list(devices) + [tag]))


def _update_device_health(devices: List[str], hids: List[int], session, delete: bool = True):
    if delete:
        device_health.delete_device_health(devices, session=session)
    nodes = device_define.algorithm_id2node_id_batch(devices)
    if len(nodes) != len(devices):
        raise Exception(f'查询到的nodes: {nodes} 与devices: {devices}数量不一致.')
    for node, dev in zip(nodes, devices):
        device_health.add_device_health(dev, node, hids, session=session)


def _get_health_defines(decision: List[dict], evaluation: List[dict], other: List[dict], session):
    flist = list(decision) + list(evaluation) + list(other)
    fs = [fopt[option_key.function] for fopt in flist]
    # 部分健康度名称会配置在function.global_parameter.health_define中
    cps = [fopt.get(option_key.global_parameter, dict()).get('health_define') for fopt in flist]
    fds = function_define.get_function_define(fs, session=session)
    defines = list()
    for fn, cp in zip(fs, cps):
        cp = cp or fds[fn]['health_define_name']
        defines.extend(cp)
    return health_define.get_health_id_from_name(defines, session=session)


def update_option(tag: str, worker: str = None, clock_time: int = None,
                  enabled: Union[bool, int] = True, devices: List[str] = None,
                  decision: List[dict] = None,  evaluation: List[dict] = None,
                  other: List[dict] = None, extra: dict = None,
                  description: str = ''):
    """
    更新device_option的同时更新device_health
    """
    with _api.get_session() as session:
        if worker and not worker_define.worker_define_exist(worker):
            raise ValueError(f'worker: {worker}不存在.')
        old_devices = device_option.get_option(tag)[device_option.flag.devices]

        if devices:
            delete_option(devices, session=session)

        device_option.update_option(
            tag, worker, clock_time, enabled, devices,
            decision, evaluation, other, extra, description,
            session=session
        )
        # 需要更新device_health的情况:
        # 1.devices更新
        need_update_dh = False
        if need_update_dh:
            hids = device_health.get_device_health(tag, session=session)
        # 2.decision/evaluation/other更新:
        if decision is not None or evaluation is not None or other is not None:
            hids = _get_health_defines(decision, evaluation, other, session=session)
            need_update_dh = True
        if need_update_dh:
            # 删除旧的devices配置的device_health
            device_health.delete_device_health(_format_devices(tag, old_devices), session=session)
            if devices is None:
                devices = old_devices
            _update_device_health(_format_devices(tag, devices), hids, session, False)


def delete_option(tag: Union[List[str], str], session=None):
    """
    删除device_option的同时清空相对应的device_health
    """
    if isinstance(tag, str):
        tag = [tag]

    with _api.get_session(session=session) as session:
        options = device_option.get_option(tag, session=session)
        devices = set()
        for dev, option in options.items():
            devices.update(option[device_option.flag.devices] or list())
        devices.update(tag)

        device_option.delete_option(tag, session)
        device_health.delete_device_health(list(devices), session)


get_option = device_option.get_option
get_tags = device_option.get_tags
get_device_health = device_health.get_device_health
get_device_health_tags = device_health.get_device_health_tags
tag_exist = device_option.tag_exist
