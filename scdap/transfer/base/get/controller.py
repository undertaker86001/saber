"""

@create on: 2021.01.02
"""
from abc import ABCMeta, abstractmethod

from scdap import data
from scdap.core.controller import BaseController


class BaseGetController(BaseController, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._decoder = self._create_decoder()

    def _create_decoder(self) -> data.FeatureListDecoder:
        fi_decoder = data.FeatureItemDecoder(data.FeatureItemKV())
        return data.FeatureListDecoder(data.FeatureListKV(), fi_decoder)

    def _decode(self, container: data.Container, obj: data.TYPE_JSON) -> int:
        if not obj:
            return 0

        self.logger_debug(f'decode -> {obj}')
        size = container.size()
        self._decoder.decode(obj, container.flist)
        return container.size() - size

    def interface_name(self):
        return f'controller:{self.get_controller_name()}.{self.transfer_mode()}'

    @staticmethod
    def get_controller_name() -> str:
        return 'get'

    @abstractmethod
    def transfer_mode(self) -> str:
        """
        获得传输方式的类型

        :return:
        """
        pass
