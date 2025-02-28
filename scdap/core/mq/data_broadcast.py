"""

@create on: 2020.12.15
"""
import json
from typing import Dict, Union
from .base import MQBaseClass, Exchange, Message


class DataBroadcast(object):
    """
    使用rabbitmq/kombu进行数据广播
    不支持多线程
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 vhost: str = None, heartbeat: int = None, new_mqbase: bool = False):
        args = (host, port, user, password, vhost, heartbeat)
        self._mqbase = MQBaseClass(*args) if new_mqbase else MQBaseClass.get_instance(*args)
        # 广播用交换机
        self._exchanges: Dict[str, Exchange] = dict()

    def add_exchange(self, exchange_name: str, exchange_type: str = None):
        """
        新增交换机负责广播数据

        :param exchange_name: 交换机名称
        :param exchange_type: 交换机类型
        """
        self._exchanges[exchange_name] = self._mqbase.get_exchange(exchange_name, exchange_type)

    def broadcast(self, exchange_name: str, data: Union[list, dict], routing_key: str, seq: int = -1) -> bool:
        """
        广播数据

        :param exchange_name: 交换机名称
        :param data: 数据
        :param routing_key: 路由键
        :param seq: 序列号
        :return: 是否发送成功
        """
        exchange = self._exchanges[exchange_name]
        try:
            exchange.declare(passive=True)
        except:
            return False
        message = Message(json.dumps(data, ensure_ascii=False), application_headers={'seq': seq})
        exchange.publish(message, routing_key)
        return True

    def reconnect(self):
        """
        重新连接服务器
        """
        info = [(exchange.name, exchange.type) for exchange in self._exchanges.values()]
        self.close_connect()

        self._mqbase.connect(True)

        for exchange_name, exchange_type in info:
            self.add_exchange(exchange_name, exchange_type)

        return True

    def clear_exchange(self):
        """
        清理交换机
        """
        self._exchanges.clear()

    def pop_exchange(self, exchange_name: str):
        """
        移除交换机

        :param exchange_name: 交换机名称
        """
        self._exchanges.pop(exchange_name)

    def close_connect(self):
        """
        关闭至服务器的连接
        """
        self.clear_exchange()
        self._mqbase.close()
