"""

@create on: 2020.03.12

信号控制器
通过信号传递的方式控制算法进程刷新参数
"""
import os
import signal

from scdap.gop.loc import load_loc_program
from scdap.core.controller import BaseController


class SignalController(BaseController):
    # 更新阈值信号
    __need_update__ = False
    # 重置信号
    __need_reset__ = False

    @staticmethod
    def get_controller_name() -> str:
        return 'signal'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 使用signal.SIGUSR作为更新参数的指令信号
        # 只在linux系统下有用
        if os.name != 'posix':
            self.logger_warning('模块必须在posix系统中才能使用.')
        else:
            self._usr1 = signal.SIGUSR1
            signal.signal(self._usr1, self._signal_usr1)
            self._usr2 = signal.SIGUSR2
            signal.signal(self._usr2, self._signal_usr2)

    def can_use(self) -> bool:
        # 只在linux系统下有用
        return os.name == 'posix'

    def run(self):
        if SignalController.__need_update__:
            self._update_worker()
        if SignalController.__need_reset__:
            self._reset_worker()

    def _reset_worker(self):
        """
        重置worker
        """
        SignalController.__need_reset__ = False
        self.logger_info('worker重置成功.')

    def _update_worker(self):
        """
        更新算法参数
        """
        SignalController.__need_update__ = False
        if self._context.load_loc:
            load_loc_program()

        self._context.set_parameter()

        self.logger_info(f'worker参数更新成功')

    @staticmethod
    def _signal_usr1(signo, frame):
        SignalController.__need_update__ = True

    @staticmethod
    def _signal_usr2(signo, frame):
        SignalController.__need_reset__ = True

    def close(self):
        if self._usr1:
            signal.signal(self._usr1, signal.SIG_IGN)
        if self._usr2:
            signal.signal(self._usr2, signal.SIG_IGN)
