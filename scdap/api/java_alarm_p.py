"""

@create on: 2021.05.25
获取java后端配置的报警相关配置
会在scdap.extendc.alarm中使用
"""
import json
from typing import Dict, List

from pymysql.cursors import DictCursor

from .mysql import get_cursor


def _parser(parameter) -> dict:
    parameter['manager_alarm'] = json.loads(parameter['manager_alarm'])
    parameter['pub_alarm'] = json.loads(parameter['pub_alarm'])
    return parameter


def _list_health_defines(parameter) -> list:
    return list(parameter['manager_alarm'].get('rules', dict()).keys()) + \
           list(parameter['pub_alarm'].get('threshold', dict()).keys())


class AlarmParameter(object):
    def __init__(self, health_define: str, reverse: bool, parameter: dict):
        # hd_info = get_health_define(health_define)
        #
        # if hd_info is None:
        #     raise Exception(f'配置了不存在的health_define:{health_define}')

        # 健康度是否是越低越好
        self.reverse = reverse
        self.health_define = health_define

        rules = parameter.get('manager_alarm', dict()).get('rules', dict()).get(health_define, dict())
        self.enabled = bool(rules.get('enable', False))
        self.unit = rules.get('intervalUnit', 'day') or 'day'
        self.interval = rules.get('time', 1)

        if self.unit == 'day':
            self.delta = 86400 * self.interval
        elif self.unit == 'hour':
            self.delta = 3600 * self.interval
        elif self.unit == 'minute':
            self.delta = 60 * self.interval
        else:
            self.delta = 86400 * self.interval

        self.use_mean = rules.get('moveAvgHealth', True)
        default_threshold = [50, 70] if self.reverse else [70, 50]
        self.threshold = parameter.get('pub_parameter', dict())\
            .get('threshold', dict())\
            .get(health_define, default_threshold)
        # reverse = 0 -> [70, 50]   倒序的阈值数值顺序为正确
        # reverse = 1 -> [50, 70]   顺序的阈值数值顺序为正确
        if list(sorted(self.threshold, reverse=not self.reverse)) != self.threshold:
            raise Exception(f'健康度:{health_define}的reverse={self.reverse}(true->数值越低越健康,false->数值越高越健康), '
                            f'配置的阈值数值列表:{self.threshold} 顺序不正确.')


def get_java_alarm_parameter(node_id: int, health_define: List[str], reverse: List[bool]) \
        -> Dict[str, AlarmParameter]:
    """
    {
        "trend": AlarmParameter(),
        "stability": AlarmParameter(),
        ...
    }

    :param node_id:
    :param reverse:
    :param health_define:
    :return:
    """
    with get_cursor('ddps_items', DictCursor) as cursor:
        cursor.execute(f'SELECT node_id, manager_alarm, pub_alarm FROM `sc_alarm_config` WHERE node_id = {node_id}')
        parameter = cursor.fetchall()

    if not parameter:
        return dict()

    parameter = _parser(parameter[0])
    return {hd: AlarmParameter(hd, r, parameter) for hd, r in zip(health_define, reverse)}
