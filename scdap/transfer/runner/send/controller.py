"""

@create on: 2020.12.11
scdap.runner专用
"""
from typing import List

from scdap import data
from ...base import BaseSendController
from scdap.transfer.rabbitmq.get import get_feature_list_encoder
from scdap.transfer.rabbitmq.send import get_result_list_encoder


class RunnerSendController(BaseSendController):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache: List[dict] = list()
        self.fencoder = get_feature_list_encoder()

    def _create_encoder(self) -> data.ResultListEncoder:
        """
        runner使用的是rabbitmq的结果数据编码器

        :return:
        """
        return get_result_list_encoder()

    def transfer_mode(self) -> str:
        return 'runner'

    def run(self):
        for container, result in self._context.crimp.generator_cr():
            rdata = self._encode(result)
            fdata = self.fencoder.encode(container.flist)[self.fencoder.kv.data]
            for f, r in zip(fdata, rdata):
                r['feature'] = f
            self.cache.extend(rdata)

    def get_result(self) -> List[dict]:
        result = self.cache
        self.cache = list()
        return result
