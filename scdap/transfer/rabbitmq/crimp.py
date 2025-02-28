"""

@create on: 2021.01.04
"""
from typing import Type

from ..base import CRImplementer, BaseSendController, BaseGetController

from .get import *
from .send import *


class RabbitMQCRImplementer(CRImplementer):
    def need_sleep(self):
        return False

    def get_controller_class(self) -> Type[BaseGetController]:
        return RabbitMQGetController

    def send_controller_class(self) -> Type[BaseSendController]:
        return RabbitMQSendController
