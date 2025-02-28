"""

@create on: 2020.03.15

计算控制器
"""
from datetime import datetime

from scdap.core.controller import BaseController


class WorkerController(BaseController):
    """
    算法工作组调用控制器
    参数：
    屏蔽时段设置
    block_time: [int, int, bool]
    block_time[0]   屏蔽启动时间
    block_time[1]   屏蔽停止时间
                    block_time[0] / block_time[1] = hour * 100 + minute
                    block_time[0] = block_time[1]: 不启用屏蔽时间
                    block_time[0] > block_time[1]: 从当日的block_time[0]~次日的block_time[1]为屏蔽时间，如 [2300 ,800]
                    block_time[0] < block_time[1]: 从次日的block_time[0]~次日的block_time[1]为屏蔽时间，如 [100, 800]
    block_time[2]  是否在屏蔽的时候重置算法工作组
    """

    # 屏蔽时段设置默认值
    default_break_time = [0, 0, True]

    @staticmethod
    def get_controller_name() -> str:
        return 'worker'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 屏蔽时段参数
        self._block_time = self._get_option('block_time', self.default_break_time)
        self._bt_start = self._block_time[0]
        self._bt_stop = self._block_time[1]
        self._reset_in_bt = self._block_time[2]

        self._in_bt = False
        if self._bt_start != self._bt_stop:
            self.run = self._run_with_breaktime

    def _run_normal(self):
        # worker计算
        self._context.worker()

    # 默认不使用屏蔽机制方法
    run = _run_normal

    def _run_with_breaktime(self):
        """
        添加屏蔽机制
        """
        if self._in_break_time(datetime.now()):
            if self._reset_in_bt and not self._in_bt:
                self._context.reset()
                self._context.set_parameter()
            self._in_bt = True
            return

        self._in_bt = False

        self._context.worker()

    def _in_break_time(self, time: datetime):
        """
        判断传入的时间是否处于屏蔽时间段

        :param time: 当前时间
        """
        time = time.hour * 100 + time.minute
        if self._bt_start > self._bt_stop:
            return time >= self._bt_stop or time < self._bt_start
        return self._bt_start <= time < self._bt_stop

    def exception(self, exception: Exception):
        self.logger_warning('调用reset()/clear()重置算法与进程.')
        self._context.clear()
        self._context.reset()
        self.logger_warning('调用set_parameter()重新获取参数.')
        self._context.set_parameter()
