"""

@create on: 2021.01.06
"""
from datetime import datetime, timedelta
from typing import Dict

from scdap.api import device_data
from scdap.data import FeatureList
from scdap.core.threadpool import submit

from ...base import BaseGetController


class DesignerGetController(BaseGetController):
    def transfer_mode(self) -> str:
        return 'designer'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._start_time = None
        self._stop_time = None
        self._delta_time = None
        self._current_time = None
        self._columns = self._context.worker.get_column()

    def set_time_range(self, start: datetime, stop: datetime, delta: timedelta = None):
        self._start_time = start
        self._stop_time = stop
        if self._context.multi_dev:
            default_delta = timedelta(hours=1)
        else:
            default_delta = timedelta(days=1)
        self._delta_time = delta or default_delta
        self._current_time = start

    def reset(self):
        self._start_time = None
        self._stop_time = None
        self._delta_time = None
        self._current_time = None

    def is_finished(self) -> bool:
        """
        特征数据是否已经没有了

        :return:
        """
        return self._current_time is None or self._current_time >= self._stop_time

    def run(self):
        if self.is_finished():
            return

        if self._current_time >= self._stop_time:
            return

        next_time = min(self._current_time + self._delta_time, self._stop_time)

        for aid in self._context.devices:
            data = device_data.get_data(aid, self._current_time, next_time, self._columns[aid])
            cont = self._context.crimp.get_container(aid)
            cont.flist.extend_itemlist(data)
        self._current_time = next_time

    def set_flist(self, flists: Dict[str, FeatureList]):
        for aid, flist in flists.items():
            cont = self._context.crimp.get_container(aid)
            cont.flist.extend_itemlist(flist)
