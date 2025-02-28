"""

@create on: 2020.06.28
"""
from datetime import datetime, timedelta

from scdap.core.controller import BaseController


class DisconnectController(BaseController):
    """
    参数：
    delta: int          断线通知间隔(s)
    """
    default_delta = 600

    @staticmethod
    def get_controller_name() -> str:
        return 'disconnect'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._delta = self._get_option('delta', self.default_delta) * 1000

        # [dev, container, result, [local_time]]
        self._time_cr = [
            [cont.node_id2algorithm_id(), cont, res, [self._context.systimestamp_ms()()]] for cont, res in self._context.cr
        ]

        if self._context.multi_dev:
            self.run = self.run_multi

    def run(self):
        now = self._context.systimestamp_ms()
        dev, cont, res, utime = self._time_cr[0]
        # 如果实时数据容器中有数据
        if not cont.emtpy():
            # 更新时间
            utime[0] = now
        # 上一次数据时间过久则断线通知
        elif utime[0] + self._delta < now:
            utime[0] = now
            self._context.worker.disconnect([dev])

    def run_multi(self):
        """
        多设备
        """
        now = self._context.systimestamp_ms()
        devices = list()
        for dev, cont, res, utime in self._time_cr:
            # 如果实时数据容器中有数据
            if not cont.emtpy():
                # 更新时间
                utime[0] = now
            # 上一次数据时间过久则断线通知
            elif utime[0] + self._delta < now:
                devices.append(dev)
                utime[0] = now

        if len(devices) > 0:
            self._context.worker.disconnect(devices)


class DebugDisconnectController(BaseController):
    """
    参数：
    delta: int          断线通知间隔(s)
    """
    default_delta = DisconnectController.default_delta

    @staticmethod
    def get_controller_name() -> str:
        return 'debug_disconnect'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._delta: int = self._get_option('delta', self.default_delta)
        self._delta = timedelta(seconds=self._delta)
        self._curr = datetime.now()

        # [dev, container, result, [data_time]]
        self._time_cr = [
            [cont.node_id2algorithm_id(), cont, res, [datetime.now()]]
            for cont, res in self._context.crimp.generator_cr()
        ]

        if len(self._context.devices) > 1:
            self.run = self.run_multi

    def run(self):
        dev, cont, res, utime = self._time_cr[0]
        # 如果实时数据容器中有数据
        if not cont.empty():
            now = cont.flist.get_time()
            # 当前一次最后数据时间与该次最初数据时间相差过大时则短线通知
            if utime[0] + self._delta < now:
                self._context.worker.disconnect([dev])
            utime[0] = now
            # print(time)

    def run_multi(self):
        devices = list()
        for dev, cont, res, utime in self._time_cr:
            # 如果实时数据容器中有数据
            if not cont.empty():
                now = cont.flist.get_time()
                # print(dev, utime, utime[0] + self._delta, now)
                # 当前一次最后数据时间与该次最初数据时间相差过大时则短线通知
                if utime[0] + self._delta < now:
                    devices.append(dev)
                utime[0] = now

        if len(devices) > 0:
            self._context.worker.disconnect(devices)
