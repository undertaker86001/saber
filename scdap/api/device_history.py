"""

@create on: 2021.04.30
上传历史结果数据相关的接口
这里只支持历史数据的覆盖，也就是数据库中的stat表
todo: 增加事件覆盖的接口
"""
__all__ = ['parse_stat_history', 'auto_update_stat']

import os
import json
import pickle
from typing import List, Dict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from scdap import config
from scdap.data import ResultList
from scdap.util.session import do_request
from scdap.util.tc import datetime_to_long, long_to_datetime

from .device_define import algorithm_id2node_id


def get_history_stat(node_id: int, start: datetime, stop: datetime, health_define: List[str]) \
        -> Dict[str, np.ndarray]:
    """
    获取历史统计数据结果

    :param node_id:
    :param start:
    :param stop:
    :param health_define:
    :return:
    """
    start = start.replace(second=0, microsecond=0)
    stop = stop.replace(second=0, microsecond=0)
    resp = do_request('get', config.DEVICE_HISTORY_GET_STAT_URL, decode_response=False,
                      params={'nodeId': node_id,
                              "startDate": datetime_to_long(start),
                              "endDate": datetime_to_long(stop)}
                      )
    return _parse_history_stat(resp['data'], start, stop, health_define)


def _parse_history_stat(data: list, start: datetime, stop: datetime, health_define: List[str]) \
        -> Dict[str, np.ndarray]:
    size = int((stop - start) / timedelta(minutes=1))
    result = {hd: np.full(size, np.nan, dtype=np.int32) for hd in health_define}
    for d in data:
        time = long_to_datetime(d['time'])
        delta = int((time - start) / timedelta(minutes=1))
        health_json = json.loads(d['healthJson'])
        # status_json = json.loads(d['statusJson'])

        # highest_status = status_json.get('highest')
        # if highest_status is None:
        #     # 以防万一highest不存在, 需要自己判断数量最大的状态
        #     # 如果状态0和任意一个非0状态的数量相同则取非0的
        #     highest_status = 0
        #     count = 0
        #     for key, val in status_json.items():
        #         if not key.startswith('s') or not str.isdigit(key[1:]):
        #             continue
        #         status = int(key[1:])
        #         if val > count:
        #             highest_status = status
        #             count = val
        #         elif val == count and highest_status == 0:
        #             highest_status = status

        for hd, stat in result.items():
            val = health_json.get(hd)
            # 最高状态是0的也不必统计
            # 因为不会计算这段时间的数值
            # if val and highest_status != 0:
            #     stat[delta] = val
            if val:
                stat[delta] = val

    return result


class StatItem(object):
    def __init__(self, algorithm_id: str, data: list,
                 start: datetime, stop: datetime, health_define, default_score, score_limit):
        self.algorithm_id = algorithm_id
        self.data = data
        self.start = start
        self.stop = stop
        self.health_define = health_define
        self.default_score = default_score
        self.score_limit = score_limit

    @property
    def node_id(self) -> int:
        return algorithm_id2node_id(self.algorithm_id)

    def update(self):
        node_id = self.node_id
        for d in self.data:
            d['nodeId'] = node_id


def parse_stat_history(algorithm_id: str, node_id: int,
                       start: datetime, stop: datetime,
                       health_define: list, default_score: list, score_limit: list,
                       final_result: ResultList) -> StatItem:
    """
    将结果数据解析为待上传stat结构数据
    **注意**
    因为环境的不通, 所以nodeId需要在上传的时候重新通过algorithmId查询

    :param health_define:
    :param default_score:
    :param score_limit:
    :param algorithm_id:
    :param node_id:
    :param start:
    :param stop:
    :param final_result:
    :return:
    """
    result = list()

    for ritem in final_result.generator():
        if ritem.stat_item is None:
            continue

        status_json = dict()

        max_status_id = 0
        max_status_count = 0

        for status_id, count in ritem.stat_item.status.items():
            status_json[f's{status_id}'] = count
            if max_status_count < count:
                max_status_id = status_id
                max_status_count = count

        # 后端需要使用到字段highest
        status_json['highest'] = max_status_id

        # 用于备注数据, 确认实际的数据来源是python端覆盖的, 方便排查
        status_json['py'] = 1

        # 数据量不足60补状态最多的数据到60
        if ritem.stat_item.size < 60:
            status_json[f's{max_status_id}'] += (60 - ritem.stat_item.size)

        obj = {
            'nodeId': node_id,
            'timeL': datetime_to_long(ritem.stat_item.time),
            'statusJson': json.dumps(status_json),
            'healthJson': json.dumps(ritem.stat_item.score)
        }
        result.append(obj)

    return StatItem(algorithm_id, result, start, stop, health_define, default_score, score_limit)


def _update_stat(data, info: str):
    timeout = len(data) * 0.05 + 10
    try:
        response = do_request('post', url=config.DEVICE_HISTORY_UPDATE_STAT_URL,
                              json=data, timeout=timeout, decode_response=False)
        print(f'上传结果: [{info}], response: {response}')
    except Exception as e:
        print(f'上传失败: [{info}], error: {e}')


def auto_update_stat(history_dir: str = 'history', pool_size: int = 5, batch_size: int = 600):
    """
    自动获取本地缓存的历史stat数据并且上传
    history_dir目录结构:
    history/
        -device1/
            -xxxx.pkl
            -xxxx.pkl
            ...
        -device2/
            -xxxx.pkl
            -xxxx.pkl
            ...
        ...

    :param history_dir: 待上传的历史stat数据目录
    :param pool_size: 多线程池的线程数量
    :param batch_size: 每一次上传的stat数据数量, 不应配置的过大, 防止后端堵塞
    """
    if not os.path.exists(history_dir):
        print(f'历史stat数据路径: {history_dir}不存在.')
        return

    pool = ThreadPoolExecutor(pool_size)

    for node_dir in os.listdir(history_dir):
        node_dir = os.path.join(history_dir, node_dir)
        print(f'载入历史文件目录: {node_dir}')

        for sub_file in sorted(os.listdir(node_dir)):
            sub_file = os.path.join(node_dir, sub_file)
            print(f'读取历史文件: {sub_file}')

            with open(sub_file, 'rb') as f:
                stat_data: StatItem = pickle.load(f)

            size = len(stat_data.data)
            stat_data.update()
            pos = 0
            while pos < size:
                start_pos = pos
                stop_pos = min(start_pos + batch_size, size)

                info = f"aid: {stat_data.algorithm_id}, nid: {stat_data.node_id}, " \
                       f"time: {stat_data.start} ~ {stat_data.stop} pos: [{start_pos}:{stop_pos}]"
                pool.submit(_update_stat, stat_data.data[start_pos:stop_pos], info)

                pos = stop_pos

    pool.shutdown()
