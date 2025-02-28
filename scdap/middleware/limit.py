# -*- coding: utf-8 -*-
# @Time    : 2022/7/22 12:16
# @Author  : 高留柱
# @File    : frequency_limitation.py
# @Desc    :
from scdap.logger import LoggerInterface
from datetime import datetime


class KeyFrequencyLimitation(LoggerInterface):
    """
    关键字频率限制器，限制数据中某个key的value频率
    """

    def interface_name(self):
        return "事件频率限制器"

    def __init__(self, limit: int):
        self.limit = limit
        self.current_count = 0
        self.next_flag = None
        self.logger_warning(f"事件限制器已打开，限制条数为：{str(self.limit)}")

    def limit_event(self, data: dict, key: str = "event") -> dict:
        """
        限制事件中的数据频率
        """
        # 如果限制条数为0，则代表不限制数据
        if not self.limit:
            return data
        value = data.get(key)  # list
        if not value:
            # 如果取出来的数据为空，则返回原来的数据不变
            return data
        if isinstance(value, list):
            event_list = []
            # 如果是列表
            for event in value:
                alarm_time = event.get("alarmTime")
                minute_num = datetime.fromtimestamp(alarm_time / 1000).strftime("%Y-%m-%d %H:%M")
                # 如果发现上一笔事件与下一笔事件的时间不一致，清空计数，重置flag
                if self.next_flag != minute_num:
                    self.current_count = 1
                    self.next_flag = minute_num
                    event_list.append(event)  # 放行本次的事件
                    continue
                # 如果事件的时间处在同一分钟
                if self.next_flag == minute_num:
                    if self.current_count >= self.limit:
                        self.logger_warning(f"数据超出每分钟{self.limit}条, 该事件{str(event)}将被丢弃！")
                        self.current_count += 1
                        continue
                    self.current_count += 1
                    event_list.append(event)
                    continue
            data[key] = event_list
        return data

