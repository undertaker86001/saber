"""

@create on: 2021.01.02
rabbitmq-node-queue -> send_controller -> crimp -> ... -> worker -> ... -> get_controller -> rabbitmq-result-queue
"""
from .get import *
from .send import *

from .crimp import RabbitMQCRImplementer
crimp_class = RabbitMQCRImplementer
