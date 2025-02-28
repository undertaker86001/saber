"""

@create on: 2020.03.06

定时更新阈值任务
不使用了
"""
from datetime import datetime, timedelta

from scdap import config
from scdap.gop.loc import load_loc_program
from scdap.core.controller import BaseController


class CrontabController(BaseController):
    """
    根据配置的time参数来决定是否定时更新算法参数

    参数:
    time:[int, int, int, int]   更新时间，[天, 时, 分, 秒]
    """

    default_time = config.PROGRAM_CRONTAB_TIME

    @staticmethod
    def get_controller_name() -> str:
        return 'crontab'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._time = self._get_option('time', self.default_time)
        if len(self._time) != 4:
            raise self.wrap_exception(ValueError, '定时任务必须拥有四个参数: (day, hour, minute, second).')
        if self._time[0] < 0:
            raise self.wrap_exception(ValueError, 'time[0](day) 必须大于等于0.')
        if self._time[1] < 0 or self._time[1] >= 24:
            raise self.wrap_exception(ValueError, 'time[1](hour) 的范围必须处在[0, 24).')
        if self._time[2] < 0 or self._time[2] >= 60:
            raise self.wrap_exception(ValueError, 'time[2](minute) 的范围必须处在[0, 60).')
        if self._time[3] < 0 or self._time[3] >= 60:
            raise self.wrap_exception(ValueError, 'time[3](second) 的范围必须处在[0, 60).')

        # 以时间戳的方式进行时间的保存与计算
        # 提高运行效率
        now = self._context.systime()

        self._delta = timedelta(days=self._time[0]).total_seconds()
        self._next_time = now.replace(
            hour=self._time[1],
            minute=self._time[2],
            second=self._time[3],
            microsecond=0,
        ).timestamp()

        if self._next_time < now.timestamp():
            self._next_time += self._delta

        self.logger_info(f'下一次定时更新参数时间为: {datetime.fromtimestamp(self._next_time)}.')

    def run(self):
        if self._context.systimestamp_ms() >= self._next_time:
            self._next_time += self._delta
            if self._context.load_loc:
                load_loc_program()

            self._context.set_parameter()

            self.logger_info(f'定时更新参数成功, 下一次定时更新参数时间为: {datetime.fromtimestamp(self._next_time)}.')
