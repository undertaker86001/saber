"""

@create on: 2021.01.21
报警模块
主要将分三类,
1.不报警
2.报警, 均值报警, 需要先获取历史数据后, 根据这一堆数据进行移动均值计算
3.报警, 实时报警, 依据实时健康度数值进行报警
"""
from typing import List, Dict
from collections import Counter
from datetime import datetime, timedelta

import numpy as np
from scdap.api import java_alarm_p
from scdap.api import device_history
from scdap.logger import LoggerInterface
from scdap.core.controller import BaseController


class AlarmPackage(object):
    """
    报警数据结构

    """

    def __init__(self, need_alarm: bool, time: datetime, score: int, health_define: str):
        self.time = time
        self.need_alarm = need_alarm
        self.score = score
        self.health_define = health_define


class SingleHealthAlarm(LoggerInterface):
    """
    单一健康度报警模块
    只针对单个健康度进行报警

    """

    def interface_name(self):
        return f'alarm:aid={self.algorithm_id}:nid={self.node_id}:hd={self.health_define}'

    def __init__(self, algorithm_id: str, node_id: int, health_define: str, limit: bool,
                 alarm_parameter: java_alarm_p.AlarmParameter):
        self.algorithm_id = algorithm_id
        self.node_id = node_id
        self.health_define = health_define
        self.limit = limit
        # 计算均值时的需要筛选去掉的数值
        self.null = 0 if self.limit else np.nan

        self.alarm_parameter = alarm_parameter
        self.use_mean = alarm_parameter.use_mean
        # 移动平均计算长度
        self.mean_size = alarm_parameter.delta
        self.threshold = alarm_parameter.threshold
        self.reverse = alarm_parameter.reverse
        self.stat_history = np.zeros(0, dtype=np.int32)
        self.alarm_index = 0

    def set_stat_history(self, stat_history: np.ndarray):
        if self.use_mean:
            # 历史的stat表数据
            self.stat_history = stat_history

            # 在limit = True 的情况下
            # 只计算非0的数值
            # 所以如果即存在nan又存在0需要额外排除不计算则需要额外的计算操作
            # 所以当 limit=True 的时候直接把所有nan替换为0
            # 这样子在计算的时候只需要排除0就行了
            if self.limit:
                self.stat_history[self.stat_history == np.nan] = self.null

    def print_message(self):
        self.logger_info(f'enable: {self.alarm_parameter.enabled}, '
                         f'limit: {self.limit}, '
                         f'use_mean: {self.use_mean}, '
                         f'mean_size: {self.mean_size}')
        self.logger_info(f'reverse: {self.reverse}, '
                         f'threshold: {self.threshold}')

    def use_mean(self) -> bool:
        """
        是否使用均值报警
        另一种是实时健康度报警

        :return:
        """
        return self.use_mean()

    def _get_index(self, score: int) -> int:
        """
        获取当前健康度数值所处在的阈值挡位
        reverse = False, th = [70, 50]
        score = 100 -> 0
        score = 70 -> 1, score = 60 -> 1
        score = 50 -> 2, score = 10 -> 2
        -----------------------------
        reverse = True, th = [50, 70]
        score = 10 -> 0
        score = 50 -> 1, score = 60 -> 1
        score = 70 -> 2, score = 90 -> 2

        :param score:
        :return:
        """
        index = 0
        for index, t in enumerate(self.threshold):
            if self.reverse and score < t:
                return index
            elif not self.reverse and score > t:
                return index
        return index + 1

    def _trigger_alarm(self, time: datetime, score: int) -> AlarmPackage:
        """
        根据健康度判断是否需要触发报警

        :param time:
        :param score:
        :return:
        """
        # 未计算出健康度(或者是当健康度为配置的null的时候)无需报警
        # 并且在此时也未知健康度当前所处的阈值档位
        if score == self.null:
            return AlarmPackage(False, time, score, self.health_define)

        # 获取平均健康度所处的阈值区间
        index = self._get_index(score)
        # print(self.alarm_index, index, score, self.health_define)
        # 返回一个报警封包
        package = AlarmPackage(index > self.alarm_index, time, score, self.health_define)
        self.alarm_index = index
        return package

    def run_realtime_alarm(self, time: datetime, score: int) -> AlarmPackage:
        """
        实时健康度报警

        :param time:
        :param score:
        :return:
        """
        return self._trigger_alarm(time, score)

    def run_mean_alarm(self, stat_time: datetime, stat_score: int) -> AlarmPackage:
        """
        均值健康度报警

        :param stat_time:
        :param stat_score:
        :return:
        """
        if len(self.stat_history) == 0:
            return self._trigger_alarm(stat_time, self.null)

        # 向右压入数据
        # 最左边的数据抛弃
        self.stat_history = np.roll(self.stat_history, -1)
        self.stat_history[-1] = stat_score

        # 计算平均健康度
        # 不取np.nan或者0(依据limit)来进行计算
        temp = np.compress(self.stat_history != self.null, self.stat_history)

        if len(temp) == 0:
            score = self.null
        else:
            score = int(np.mean(temp))

        return self._trigger_alarm(stat_time, score)


class SingleDeviceAlarm(LoggerInterface):
    """
    单个点位的报警模块
    单个点位可能包含多个健康度需要报警
    _SingleDeviceAlarm  -> trend     : _SingleHealthAlarm
                        -> stability : _SingleHealthAlarm
                        -> ...
    模块会分两块进行计算
    1. realtime_alarm:  实时报警, 来源于实时健康度的报警
    2. mean_alarm:      均值报警, 来源于移动均值的报警

    """

    def interface_name(self):
        return f'alarm-dev:aid={self.algorithm_id}:nid={self.node_id}'

    def __init__(self, algorithm_id: str, node_id: int, health_defines: List[str], reverse: List[bool],
                 limit: List[bool], alarm_parameter: dict = None):
        self.algorithm_id = algorithm_id
        self.node_id = node_id
        self.health_defines = health_defines
        self.reverse = reverse
        self.limit = limit
        if alarm_parameter:
            alarm_parameter = {
                hd: java_alarm_p.AlarmParameter(hd, r, alarm_parameter)
                for hd, r in zip(health_defines, reverse)
            }
        else:
            alarm_parameter = java_alarm_p.get_java_alarm_parameter(node_id, health_defines, reverse)
        self.alarm_parameter = alarm_parameter

        self.history_stat_has_loaded = False

        # 移动平均报警模式的健康度
        self.mean_sh_alarm: Dict[str, SingleHealthAlarm] = dict()

        # 实时数值报警的健康度
        self.rt_sh_alarm: Dict[str, SingleHealthAlarm] = dict()

        for hd, l in zip(health_defines, limit):
            alarm_parameter = self.alarm_parameter[hd]
            # enabled = False 代表不需要推送报警
            # 则直接不加入
            if not alarm_parameter.enabled:
                continue

            alarm = SingleHealthAlarm(algorithm_id, node_id, hd, l, alarm_parameter)

            if alarm_parameter.use_mean:
                self.mean_sh_alarm[hd] = alarm
            else:
                self.rt_sh_alarm[hd] = alarm

    def reset(self):
        self.history_stat_has_loaded = False

    def load_history_stat(self, time: datetime):
        """
        载入历史状态数据
        按需载入, 如果一直没有调用到alarm则不触发
        只在第一次调用的时候触发载入(主要是方便配置时间

        :param time:
        :return:
        """
        if self.history_stat_has_loaded:
            return

        time = time.replace(second=0, microsecond=0)

        # 获取所有健康度的计算间隔
        delta_list = [p.delta for p in self.alarm_parameter.values()]

        # 说明获取的健康度的计算间隔都一致
        # 直接同意查询就可以了
        if len(Counter([p.delta for p in self.alarm_parameter.values()])) == 1:
            history_stat = device_history.get_history_stat(
                self.node_id, time - timedelta(seconds=delta_list[0]), time, self.health_defines
            )
        else:
            # 如果需要使用的移动均值长度不一致需要另外调用接口查询
            history_stat = dict()
            for hd in self.health_defines:
                history_stat.update(device_history.get_history_stat(
                    self.node_id, time - timedelta(seconds=delta_list[0]), time, [hd]
                ))
        for alarm in self.mean_sh_alarm.values():
            alarm.set_stat_history(history_stat[alarm.health_define])

        self.history_stat_has_loaded = True

    def print_message(self):
        for aid, val in self.mean_sh_alarm.items():
            val.print_message()
        for aid, val in self.rt_sh_alarm.items():
            val.print_message()

    def __repr__(self):
        return f'[<{type(self).__name__}> aid: {self.algorithm_id}, nid: {self.node_id}, ' \
               f'mean_alarm: {self.mean_sh_alarm}], realtime_alarm: {self.rt_sh_alarm}]'

    def has_realtime_alarm(self) -> bool:
        """
        该点位是否有需要做实时的报警推送的健康度

        :return:
        """
        return len(self.rt_sh_alarm) != 0

    def has_mean_alarm(self) -> bool:
        """
        该点位是否有需要做均值报警推送的健康度

        :return:
        """
        return len(self.mean_sh_alarm) != 0

    def run_realtime_alarm(self, time: datetime, scores: Dict[str, int]) -> List[AlarmPackage]:
        """
        实时报警

        :param time:
        :param scores:
        :return:
        """
        result = list()

        for hd, alarm in self.rt_sh_alarm.items():
            package = alarm.run_realtime_alarm(time, scores[hd])
            if package.need_alarm:
                result.append(package)
        return result

    def run_mean_alarm(self, stat_time: datetime, stat_scores: Dict[str, int]) -> List[AlarmPackage]:
        """
        均值报警

        :param stat_time:
        :param stat_scores:
        :return:
        """
        self.load_history_stat(stat_time)
        result = list()
        for hd, alarm in self.mean_sh_alarm.items():
            package = alarm.run_mean_alarm(stat_time, stat_scores[hd])
            if package.need_alarm:
                result.append(package)
        return result


class AlarmController(BaseController):
    """
    参数:
    下面的参数来源于数据库后端使用的报警配置: ddps_items.sc_alarm_config, 通过scdap.api.java_alarm_p调用获取
    同时也可以配置在参数配置extra.alarm.java_parameter中
    ps1.因为是临时从java后端迁移过来的, 所以没有强制修改参数的key名称以及相关的参数数据格式
    ps2.目前支持后端控制台配置以及算法控制台配置, 算法控制台如果点位是联动的, 可能会比较麻烦, 暂时需要到后端控制台取配置
    ps3.后续会由算法全权负责报警推送, 意味着不再使用该模块了
    ps4.因为考虑到需要和前端界面互动, 如前端显示的报警线等, 所以届时会提供一套在算法配置层面配置的内容

    java_parameter: {
        "algorithm_id": {
            "pub_parameter": {
                "threshold": {
                    "trend": [75, 50],      # trend健康度阈值
                    "stability": [75, 50],  # stability健康度阈值
                    "autoCompute": true     # 无用, 是否启用自动阈值计算
                },
            },

            "manager_alarm": {
                "rules": {
                    "trend": {
                        "time": 1,              # 与intervalUnit挂钩, 比如intervalUnit = day代表移动均值长度为1天
                        "enable": true,         # 是否启用报警
                        "intervalUnit": "day",  # 与time挂钩, 用于配置时间颗粒度, 如果是空字符串默认为day, 可配置day/hour/minute
                        "pushCustomer": true    # 是否推送报警至客户, 后端使用, 算法进程内无用
                        "moveAvgHealth": true   # 是否使用移动均值报警, 与intervalUnit/time关联, 如果为false则直接使用实时健康度数值进行报警
                    },
                    "stability": {
                        ...
                    }
                    # 注意可能部分健康度没有配置代表默认关闭
                }
            }
        }
    }
    """

    def __init__(self, context, **option):
        super().__init__(context, **option)
        health_defines = self._context.worker.get_health_define()
        reverse = self._context.worker.get_score_reverse()
        limits = self._context.worker.get_score_limit()

        self._realtime_sd_alarm: Dict[str, SingleDeviceAlarm] = dict()
        self._mean_sd_alarm: Dict[str, SingleDeviceAlarm] = dict()
        self._need_alarm = False

        self._java_parameter = self._get_option('java_parameter', dict())

        for aid in self._context.devices:
            alarm = SingleDeviceAlarm(aid, self._context.algorithm_id2node_id(aid),
                                      health_defines[aid], reverse[aid], limits[aid],
                                      self._java_parameter.get(aid))
            alarm.print_message()

            if alarm.has_mean_alarm():
                self._mean_sd_alarm[aid] = alarm
                self._need_alarm = True

            if alarm.has_realtime_alarm():
                self._realtime_sd_alarm[aid] = alarm
                self._need_alarm = True

    def reset(self):
        for alarm in self._realtime_sd_alarm.values():
            alarm.reset()

        for alarm in self._mean_sd_alarm.values():
            alarm.reset()

    @staticmethod
    def get_controller_name() -> str:
        return 'alarm'

    def need_run(self) -> bool:
        return self._need_alarm

    def run(self):
        for result in self._context.crimp.generator_result():

            alarm_packages = list()
            mean_alarm = self._mean_sd_alarm.get(result.get_algorithm_id())
            realtime_alarm = self._realtime_sd_alarm.get(result.get_algorithm_id())

            for r in result.rlist:

                # 实时报警
                if realtime_alarm:
                    alarm_packages.extend(
                        realtime_alarm.run_realtime_alarm(r.time, dict(zip(r.health_define, r.score)))
                    )

                # 均值报警
                # 需要extendc.stat先行进行计算后才会启用
                if mean_alarm and r.stat_item:
                    alarm_packages.extend(mean_alarm.run_mean_alarm(r.stat_item.time, r.stat_item.score))

            # 推送报警
            for p in alarm_packages:
                result.add_alarm_event(health_define=p.health_define, start=p.time, stop=p.time,
                                       message='[controller:alarm]触发自动报警.')
