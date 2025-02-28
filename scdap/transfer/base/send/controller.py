"""

@create on: 2021.01.02

"""
from abc import ABCMeta, abstractmethod
from typing import List

from scdap import data
from scdap.core.controller import BaseController


class BaseSendController(BaseController, metaclass=ABCMeta):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._encoder = self._create_encoder()

    def _create_encoder(self) -> data.ResultListEncoder:
        event_encoder = data.EventEncoder(data.EventKV())
        si_encoder = data.StatItemEncoder(data.StatItemKV())
        ri_encoder = data.ResultItemEncoder(data.ResultItemKV(), event_encoder, si_encoder)
        return data.ResultListEncoder(data.ResultListKV(), ri_encoder)

    def _encode(self, result: data.Result) -> List[dict]:
        result = self._encoder.encode(result.rlist)
        if result:
            self.logger_debug(f'encode -> {result}')
        return result

    def interface_name(self):
        return f'controller:{self.get_controller_name()}.{self.transfer_mode()}'

    @staticmethod
    def get_controller_name() -> str:
        return 'send'

    @abstractmethod
    def transfer_mode(self) -> str:
        """
        获得传输方式的类型

        :return:
        """
        pass
