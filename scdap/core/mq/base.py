"""

@create on: 2020.10.20
rabbitmq基础类库
"""
import time
from threading import Lock
from socket import timeout as Timeout
from typing import Union, List, Callable, NoReturn, Optional, Iterable

from kombu.simple import SimpleQueue
from amqp.basic_message import Message
from kombu.transport.pyamqp import Channel
from amqp.exceptions import ConsumerCancelled, NotFound, NoConsumers
from kombu import Connection, Queue, Exchange, Consumer, binding


QUEUE_EXCEPTIONS = (ConsumerCancelled, NotFound, NoConsumers)


class MQBaseClass(object):
    # 使用的mq类型
    # eg: confluentkafka://localhost:12345
    uri_prefix = 'amqp'
    # 数据超过队列限制数量时的模式(x-overflow)
    # drop-head: 将最前面的数据抛掉
    # reject-publish: 抛弃后进入的数据
    overflow_mode = 'drop-head'
    # 队列可存放的数据数量大小
    queue_max_length = 60 * 60 * 1
    # 超时没被消费的数据将被抛掉, 单位秒
    queue_data_ttl = queue_max_length
    # 队列存在时间
    # 当没有任何消费者消费队列数据超过一定时长则自动销毁队列
    queue_expires = queue_data_ttl * 2

    def __init__(self, host: str, port: int, user: str, password: str, vhost: str = None, heartbeat: int = None):
        self._connection_config = {
            "hostname": host,
            "port": port,
            "virtual_host": vhost or '/',
            "userid": user,
            "password": password,
            "heartbeat": heartbeat
        }
        # 全局连接类
        self._connection: Optional[Connection] = None
        # 服务器心跳机制
        # 注意, 心跳需要手动调用方法接收与回复
        # connection.heartbeat_check()
        self._heartbeat = heartbeat
        self._lock = Lock()
        self._connect_timestamp = 0

    def connect_timestamp(self) -> float:
        return self._connect_timestamp

    def _get_connection(self) -> Connection:
        """
        获取mq连接类, 一般用于消费者获取队列数据

        :return: mq连接类
        """
        self.connect()
        return self._connection

    def has_queue(self, queue: Union[str, Queue]) -> bool:
        """
        查询是否存在queue
        如果在同一个队列数据消费者以及数据生产者在同一个MQBaseClass，
        也就是同一个Connection(MQBaseClass.get_instance会生成同一个)
        那么在调用has_queue的时候会直接消费掉生产者产生的数据, 因为queue_declare内将会调用drain_events
        所以如果想要生产者以及消费者在同一个进程内使用，则必须通过new_mqbase创建两个不同的MQBaseClass(Connection)

        :param queue: 队列名称

        :return: 是否存在
        """
        try:
            if isinstance(queue, Queue):
                queue.queue_declare(passive=True)
            else:
                self._get_channel().queue_declare(queue, passive=True)
            return True
        except QUEUE_EXCEPTIONS:
            return False

    def drain_events(self, timeout: Union[int, float, None] = None) -> bool:
        """
        获取数据

        :param timeout: 超时时间

        :return: 是否没有获取到数据并且超时
        """
        try:
            self._get_connection().drain_events(timeout=timeout)
            return True
        except Timeout:
            return False

    def _get_channel(self) -> Channel:
        """
        获取连接通道

        :return: 连接通道
        """
        return self._get_connection().channel()

    def heartbeat(self):
        """
        接收服务器心跳并且反馈
        # 注意, 如果在配置的设置了heartbeat则需要手动调用该方法
        """
        self._get_connection().heartbeat_check()

    def get_exchange(self, name: str, exchange_type: str = None,
                     auto_delete: bool = False, no_declare: bool = False) -> Exchange:
        """
        创建一个交换机Exchange

        :param name: 交换机名称
        :param exchange_type: 交换机类型, 默认为topic
        :param auto_delete: 是否在没有任何消费者连接交换机后自动删除
        :param no_declare: 是否在发现没有该交换机后自动创建
        :return: 创建的交换机
        """
        return Exchange(
            name, type=exchange_type or 'topic', auto_delete=auto_delete,
            no_declare=no_declare, channel=self._get_channel()
        )

    def get_queue(self, name: str, exchange: Exchange, routing_key: Union[Iterable[str], str] = None,
                  auto_delete: bool = True, no_declare: bool = False) -> Queue:
        """
        创建一个底层队列

        :param name: 队列名称
        :param exchange: 绑定的交换机, 队列无论如何都必须绑定交换机
        :param auto_delete: 是否在没有任何消费者连接队列后自动删除
        :param routing_key: 路由键
        :param no_declare: 是否在发现没有该队列后自动创建
        :return: 创建的交换机
        """
        bindings = None
        if isinstance(routing_key, (list, tuple, set)):
            bindings = [binding(exchange, rk) for rk in routing_key]
            routing_key = None
        return Queue(
            name, routing_key=routing_key, bindings=bindings,
            exchange=exchange, no_declare=no_declare,
            auto_delete=auto_delete, no_ack=False,
            channel=exchange.channel,
            data_ttl=self.queue_data_ttl,
            max_length=self.queue_max_length,
            queue_arguments={'x-overflow': self.overflow_mode}
        )

    def get_simplequeue(self, queue: Queue) -> SimpleQueue:
        """
        创建一个简单队列, 该队列是对底层队列的封装, 通常用于生产者发送数据至队列

        :param queue: 关联的底层队列
        :return: 创建的简单队列
        """
        return SimpleQueue(queue.channel, queue, queue.no_ack)

    def get_comsumer(self, callback: Callable[[Message], NoReturn] = None, queue: Union[Queue, List[Queue]] = None) \
            -> Consumer:
        """
        获取一个消费者类, 消费者类需要绑定队列以及队列有数据时触发的回调方法
        def callback(message: kombu.Message):
            ...

        :param callback: 待绑定的回调方法
        :param queue: 待绑定的队列
        :return: 创建的消费者类
        """
        if isinstance(queue, Queue):
            queue = [queue]

        return self._get_connection().Consumer(queue, on_message=callback)

    def close(self):
        """
        断开与mq服务器的连接
        """
        with self._lock:
            self._close()

    def _close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def connect(self, reconnect: bool = False):
        """
        与mq服务器建立连接

        :param reconnect: 是否重新连接服务器
        """
        with self._lock:
            # 确认是否已经创建连接
            if self._connection:
                if not reconnect and self._connection.connected:
                    return

                self._close()

            # 检查是否初始化配置
            if self._connection_config is None:
                raise TypeError('请配置连接参数.')
            # 创建链接
            try:
                self._connection = Connection(
                    **self._connection_config,
                    uri_prefix=self.uri_prefix
                ).ensure_connection(timeout=5)
            except Exception as exce:
                self._connection = None
                raise exce
            self._connect_timestamp = time.time()

    __instances__ = dict()
    __instances_lock__ = Lock()

    @classmethod
    def get_instance(cls, host: str, port: int, user: str, password: str,
                     vhost: str = None, heartbeat: int = None):
        """
        单例模式, 根据所有参数的hash值确定链接器是否有生成过
        注意

        :param host:
        :param port:
        :param user:
        :param password:
        :param vhost:
        :param heartbeat:
        :return:
        """
        args = (host, port, user, password, vhost, heartbeat)
        instance_key = ''.join(map(str, args)).__hash__()
        instance = cls.__instances__.get(instance_key)
        if instance:
            return instance
        with cls.__instances_lock__:
            return cls.__instances__.setdefault(instance_key, cls(*args))

    @classmethod
    def clear_instance(cls):
        with cls.__instances_lock__:
            for val in cls.__instances__.values():
                val.close()
            cls.__instances__.clear()
