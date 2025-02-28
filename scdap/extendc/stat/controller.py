"""

@create on: 2021.01.22
统计层
用于规整化数据, 将数据的健康度最终规整为1分钟的颗粒度
"""
from collections import Counter
from typing import List, Optional
from datetime import datetime, timedelta

import numpy as np

from scdap import config
from scdap.util.tc import get_next_time
from scdap.core.controller import BaseController
from scdap.data import ResultItem, Result, StatItem


class DeviceSocreStack(object):

    def __init__(self, result: Result, score_size: int, stat_delta: int, stat_type: str, limit: List[bool], log):
        self.result = result
        self.stat_delta = stat_delta
        self.stat_type = stat_type
        self._score_size = score_size
        self.limit = np.array(limit, dtype=np.bool)

        self.log = log
        compute_kv = {
            'mean': [self.stat_mean, self.compute_mean],
            'min': [self.stat_min, self.compute_min],
            'max': [self.stat_max, self.compute_max],
        }

        if self.stat_type not in compute_kv:
            raise ValueError('DeviceSocreStack配置了错误的统计计算模式.')

        self.stat_method, self.compute_method = compute_kv[self.stat_type]

        self.status_temp: List[int] = list()
        self.score_temp: np.ndarray = np.zeros(self._score_size, dtype=np.int)
        self.stack_size: np.ndarray = np.zeros(self._score_size, dtype=np.int)
        self.next_stat_time: Optional[datetime] = None

    def reset(self):
        self.status_temp: List[int] = list()
        self.score_temp: np.ndarray = np.zeros(self._score_size, dtype=np.int)
        self.stack_size: np.ndarray = np.zeros(self._score_size, dtype=np.int)
        self.next_stat_time: Optional[datetime] = None

    def _get_post_need_stat(self, score):
        # 再某些情况下某些健康度需要统计
        # limit:  True -> 健康度有范围限制, (0, 100] 0不加入统计
        #        False -> 健康度没有范围限制, 任何数值都加入统计
        #             |  score=0  |  score!=0
        # -----------------------------------------
        # limit=True  | 不需要统计 |  需要统计
        # limit=False |  需要统计 | 需要统计
        return np.logical_not(np.logical_and(score == 0, self.limit))

    def stat_min(self, score: np.ndarray):
        pos = self._get_post_need_stat(score)
        self.score_temp[pos] = np.minimum(score[pos], self.score_temp[pos])

    def compute_min(self) -> np.ndarray:
        return self.score_temp

    def stat_max(self, score: np.ndarray):
        pos = self._get_post_need_stat(score)
        self.score_temp[pos] = np.maximum(score[pos], self.score_temp[pos])

    def compute_max(self) -> np.ndarray:
        return self.score_temp

    def stat_mean(self, score: np.ndarray):
        pos = self._get_post_need_stat(score)
        self.score_temp[pos] += score[pos]
        self.stack_size[pos] += 1
        # print(self.score_temp)
        # print(self.stack_size)

    def compute_mean(self) -> np.ndarray:
        # 防止除于0
        pos = self.stack_size != 0
        scores = np.zeros(self._score_size, dtype=np.int)
        scores[pos] = np.ceil((self.score_temp[pos] / self.stack_size[pos])).astype(np.int)
        return scores

    def run(self, result_item: ResultItem):
        self.status_temp.append(result_item.status)

        if self.next_stat_time is None:
            self.next_stat_time = get_next_time(result_item.time, self.stat_delta)

        if result_item.time >= self.next_stat_time:
            self.compute_stat(result_item)
            # 重置缓存
            self.status_temp.clear()
            self.score_temp = np.zeros(self._score_size, dtype=np.int)
            self.stack_size: np.ndarray = np.zeros(self._score_size, dtype=np.int)
            self.next_stat_time = get_next_time(result_item.time, self.stat_delta)

        score = np.array(result_item.score, np.int)
        self.stat_method(score)

    def compute_stat(self, result_item: ResultItem):
        if self.next_stat_time is None:
            if config.SHOW_STAT_CONTROLLER_LOG:
                self.log(f'[nid: {self.result.get_node_id()} aid: {self.result.get_algorithm_id()}] '
                         f'[stat: null]')
            return

        # 计算状态数量
        counter = dict(Counter(self.status_temp))
        counter = {str(key): counter[key] for key in sorted(counter)}
        # 将stat结果存入结果数据结构中
        result_item.stat_item = StatItem(
            self.next_stat_time - timedelta(seconds=self.stat_delta), len(self.status_temp), counter,
            dict(zip(self.result.get_health_define(), self.compute_method().tolist()))
        )
        if config.SHOW_STAT_CONTROLLER_LOG:
            self.log(f'[nid: {self.result.get_node_id()} aid: {self.result.get_algorithm_id()}] '
                     f'[stat: {result_item.stat_item}]')


class StatController(BaseController):
    """
    参数:
    delta: int 统计间隔
    mode: int 计算模式, max/mean/min
    """
    default_delta = 60
    default_mode = 'mean'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stat_delta = self._get_option('delta', self.default_delta)
        self._stat_mode = self._get_option('mode', self.default_mode)
        self._score_stats = list()
        health_define = self._context.worker.get_health_define()
        limit = self._context.worker.get_score_limit()
        for aid in self._context.devices:
            stat = DeviceSocreStack(
                self._context.crimp.get_result(aid), len(health_define[aid]),
                self._stat_delta, self._stat_mode, limit[aid], self.logger_info)
            self._score_stats.append(stat)

    @staticmethod
    def get_controller_name() -> str:
        return 'stat'

    def run(self):
        for stat in self._score_stats:
            for result in stat.result.rlist.generator():
                stat.run(result)

    def finish(self, last_item: dict, *args, **kwargs):
        """
        收尾动作
        主要是用于历史统计数据的数据收尾
        因为算法是数据驱动的运作模式
        所以如果不收尾的话则可能会漏掉最后一分钟的stat数据

        :return:
        """
        for stat in self._score_stats:
            item = last_item[stat.result.get_algorithm_id()]
            stat.compute_stat(item)

    def reset(self):
        for stat in self._score_stats:
            stat.reset()
