"""

@create on: 2020.10.21
"""
from typing import Dict, Tuple, Union
from .base import MQBaseClass, SimpleQueue, Exchange, Queue


class _DataQueue(object):
    def __init__(self, exchange_name: str, queue_name: str, exchange: Exchange, queue: Queue,
                 simple_queue: SimpleQueue):
        self.exchange = exchange
        self.queue = queue
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.simple_queue = simple_queue
        self.routing_key = self.queue.routing_key


class DataSender(object):
    """
    使用rabbitmq/kombu进行数据传输-数据发送类/生产者
    不支持多线程
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 vhost: str = None, heartbeat: int = None, new_mqbase: bool = False):
        args = (host, port, user, password, vhost, heartbeat)
        self._mqbase = MQBaseClass(*args) if new_mqbase else MQBaseClass.get_instance(*args)
        # 添加的队列
        self._queues: Dict[str, _DataQueue] = dict()
        # 在重连时缓存已经注册的队列信息
        self._add_info: Dict[Union[str, int], Tuple] = dict()
        self._connect_timestamp = self._mqbase.connect_timestamp()

    def is_reconnected(self) -> bool:
        """
        判断mqbase是否重新链接了

        """
        return self._connect_timestamp != self._mqbase.connect_timestamp()

    def add_node(self, exchange_name: str, queue_name: Union[str, int],
                 exchange_opts: dict = None, queue_opts: dict = None):
        """
        添加节点

        :param exchange_name: 交换机名称
        :param queue_name: 队列名称/router_key
        :param exchange_opts: 交换机参数
        :param queue_opts: 队列参数
        """
        exchange_opts = exchange_opts or dict()
        queue_opts = queue_opts or dict()

        exchange = self._mqbase.get_exchange(exchange_name, **exchange_opts)
        queue = self._mqbase.get_queue(queue_name, exchange, **queue_opts)
        simple_queue = self._mqbase.get_simplequeue(queue)
        data_queue = _DataQueue(exchange_name, queue_name, exchange, queue, simple_queue)
        self._queues[queue_name] = data_queue
        self._add_info[queue_name] = (exchange_name, queue_name, exchange_opts, queue_opts)

    def has_queue(self, queue: Union[str, int, Queue]) -> bool:
        """
        确认队列是否存在

        :param queue: 队列名称
        :return: 是否存在队列
        """
        # return True
        if not isinstance(queue, Queue):
            queue = self._queues[queue]
        return self._mqbase.has_queue(queue)

    def send_data(self, queue_name: Union[str, int], data: Union[str, list, dict], routing_key: str = None) -> bool:
        """
        发送数据

        :param queue_name: 队列名称
        :param data: 待发送的数据
        :param routing_key: 路由键
        :return: 是否存在队列并且发送成功
        """
        queue = self._queues[queue_name]
        if self.has_queue(queue.queue):
            queue.simple_queue.put(data, routing_key=routing_key)
            return True
        return False

    def clear_node(self):
        """
        清空登记的设备队列
        """
        for queue in self._queues.values():
            queue.simple_queue.close()
        # self._add_info.clear()
        self._queues.clear()

    def pop_node(self, queue_name: str):
        """
        移除指定队列

        :param queue_name: 队列名称
        """
        # self._add_info.pop(queue_name)
        queue = self._queues.pop(queue_name)
        queue.close()

    def reconnect(self, reconnect: bool = False):
        """
        重新连接服务器
        """
        add_info = list(self._add_info.values())

        self.clear_node()

        self._mqbase.connect(reconnect)

        for args in add_info:
            self.add_node(*args)

        self._connect_timestamp = self._mqbase.connect_timestamp()
        return True

    def close_connect(self):
        """
        关闭至服务器的连接
        """
        self.clear_node()
        self._mqbase.close()
