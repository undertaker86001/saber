"""

@create on: 2021.02.02
"""
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from scdap.flag import convert_column
from scdap.api.device_data import get_data
from scdap.data import FeatureList, FeatureItem
from scdap.transfer.rabbitmq.get.coder import RabbitMQFeatureItemKV


class Stack(object):
    algorithm_id = 219
    delta = timedelta(minutes=60)
    start = datetime(2021, 4, 14, 0)
    pool = ThreadPoolExecutor(10)

    def __init__(self, dev_columns: dict, transfer: str):
        self.dev_columns = dev_columns
        for aid, col in dev_columns.items():
            self.dev_columns[aid] = convert_column(RabbitMQFeatureItemKV(), list(map(str, col)))

        self.cache: Dict[str, FeatureList] = dict()
        self.start = Stack.start
        self.transfer = transfer
        self._seq = {aid: -1 for aid in dev_columns.keys()}

    def get_seq(self, algorithm_id: str):
        seq = self._seq[algorithm_id]
        self._seq[algorithm_id] += 1
        return seq

    def get(self, algorithm_id: str) -> Tuple[Optional[FeatureItem], Optional[FeatureList]]:
        flist = self.cache.get(algorithm_id)
        if flist:
            if flist.next():
                return flist.get_ref(), flist
            del self.cache[algorithm_id]

        if not self.cache:
            self.load()

        return None, None

    def load(self):
        stop = self.start + self.delta
        for aid, col in self.dev_columns.items():
            self.cache[aid] = get_data(aid, self.start, stop, select_column=col)
            print('load data:', aid, self.cache[aid].size())

        self.start = stop
