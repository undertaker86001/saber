"""

@create on: 2020.04.10
"""
import time


class Delayer(object):
    """
    延迟器
    在触发延迟器的情况下，延迟期将启动一个计时器并进行累加直至计时器到超过一个指定数值
    超过数值后延迟期将停止累加并且关闭
    """

    def __init__(self, max_num: int):
        self._max_num = max_num
        self._wakeup_time = 0
        self._open = False

    def get_max_num(self) -> int:
        return self._max_num

    def need_delay(self) -> bool:
        """
        运行延迟器, 是否需要延迟
        :return : 如果需要延迟，即累加器没有到达指定数值，则返回True，反之返回False，并关闭延迟器
        """
        if not self._open:
            return False
        if time.time() >= self._wakeup_time:
            self.stop()
            return False
        return True

    def start(self, max_num: int = None):
        """
        启动延迟器
        """
        self._wakeup_time = time.time() + (max_num or self._max_num)
        self._open = True

    def stop(self):
        """
        关闭延迟器
        """
        self._wakeup_time = 0
        self._open = False
