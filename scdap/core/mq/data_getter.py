"""

@create on: 2020.10.21
"""
import time
from datetime import datetime
from queue import PriorityQueue
from typing import Dict, Optional, Tuple, Union, List

from scdap.logger import LoggerInterface

from .base import MQBaseClass, Consumer, Queue, Message, Exchange


DEFAULT_SEQ = 0


class _DataQueue(object):
    def __init__(self, exchange_name: str, queue_name: str, exchange: Exchange, queue: Queue):
        self.exchange = exchange
        self.queue = queue
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.routing_key = queue.routing_key


class MessageData(object):
    __slots__ = ['data', 'routing_key', 'seq']

    def __init__(self, data: str = None, routing_key: str = None, seq: int = DEFAULT_SEQ):
        self.data = data
        self.routing_key = routing_key
        self.seq = seq

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'[routing_key: {self.routing_key}, data: {self.data}, seq: {self.seq}]'

    def __ge__(self, other):
        return self.seq >= other.seq

    def __gt__(self, other):
        return self.seq > other.seq

    def __ne__(self, other):
        return self.seq != other.seq

    def __eq__(self, other):
        return self.seq == other.seq

    def __le__(self, other):
        return self.seq <= other.seq

    def __lt__(self, other):
        return self.seq < other.seq


class SequenceQueue(LoggerInterface):

    def interface_name(self):
        return f'SequenceQueue:{self._name}'

    def __init__(self, name: str, maxlen: int, max_endurance_limit: int = 0, timestamp_func=time.time):
        maxlen = maxlen + 1
        if maxlen < 1:
            raise ValueError('maxlen必须大于等于1.')
        self._name = name
        self._maxlen = maxlen
        # 获取一个顺序的和长度的队列，顺序为从低到高
        self._queue = PriorityQueue(maxsize=maxlen)
        # 当前的数据编号，默认起始为0
        self._current_seq = DEFAULT_SEQ
        # 当前的时间戳
        self._timestamp_func = timestamp_func
        # 忍耐时间指的是当出现seq顺序错乱的时候
        # 最久能够忍耐多久不抛出任何数据
        # 如果超过忍耐时间则不在管seq的顺序, 直接设置当前的seq为队列最前面一个seqDEFAULT_SEQ
        # max_endurance_limit = 0 代表可以无限忍耐, 也就是会一直卡着
        self._max_endurance_limit = max_endurance_limit
        self._temp_max_endurance_limit = max_endurance_limit
        self._endurance_limit_timestamp = self._timestamp_func()
        self._update_endurance_limit()
        # 可以抛出的数据
        # 1.可能是因为队列溢出而即将被抛弃的数据
        # 2.正常的准备被抛出的数据
        self._ready_queue = list()

    def __str__(self):
        return f'name: {self._name}, ' \
               f'size: {self.size()}({self._maxlen}), ' \
               f'current_seq: {self._current_seq}, ' \
               f'el: {datetime.fromtimestamp(self._endurance_limit_timestamp)}(max_el={self._max_endurance_limit}), ' \
               f'queue: {self._queue.queue}'

    def __repr__(self):
        return self.__str__()

    def _update_endurance_limit(self):
        """
        更新忍耐极限

        """
        self._endurance_limit_timestamp = self._timestamp_func() + self._max_endurance_limit

    def _out_of_endurance_limit(self) -> bool:
        """
        确认是不是到了忍耐极限了
        """
        return self._max_endurance_limit > 0 and self._timestamp_func() >= self._endurance_limit_timestamp

    def _first_seq(self) -> int:
        """
        返回队列第一个数据

        :return:
        """
        return self._queue.queue[0].seq

    def put(self, obj: MessageData):
        # 在 self._current_seq == DEFAULT_SEQ时代表队列刚开始接收数据
        # 此时直接配置_current_seq为第一个obj的seq
        if self._current_seq <= DEFAULT_SEQ or obj.seq <= DEFAULT_SEQ:
            self._current_seq = obj.seq - 1
            # 发现队列序列号重置了
            # 则将队列中的所有数据直接抛出
            while not self._queue.empty():
                self._ready_queue.append(self._queue.get())

        # 只允许存入seq > self._current_seq的数据
        # 小于的直接抛弃
        if obj.seq <= self._current_seq:
            self.logger_warning(f'put() -> 新的一笔数据的seq={obj.seq} <= current_seq={self._current_seq}, 该段将被遗弃.')
            return

        self._queue.put(obj)

        # 如果当前的数量已经和最大队列数相同
        # 新增数据意味着最前面的数据将被抛弃
        # 导致first_seq发生了变化
        # 所以当发现队列满了的时候
        # 需要修改self._current_seq
        if self._queue.qsize() >= self._maxlen:
            # 将因为溢出而抛弃的数据放到溢出队列中
            # 在下一次get的时候直接抛出
            self._ready_queue.append(self._queue.get())
            self._current_seq = self._first_seq() - 1
            self.logger_warning(f'put() -> 队列溢出({self.size()}({self._maxlen})), '
                                f'将在下一次get()时返回直接返回.')

    def get(self, force: bool = False) -> List[MessageData]:
        """
        批量返回数据

        """
        while 1:
            val = self._get(force)
            if val is None:
                break
            self._ready_queue.append(val)
        result = self._ready_queue.copy()
        self._ready_queue.clear()
        return result

    def _get_single(self) -> MessageData:
        self._update_endurance_limit()
        obj = self._queue.get()
        self._current_seq = obj.seq
        return obj

    def _get(self, force: bool = False) -> Optional[MessageData]:

        if self._queue.empty():
            self._update_endurance_limit()
            return None

        if force:
            return self._get_single()

        # if self._first_seq() <= self._current_seq:
        #     self._queue.get()
        #     self.logger_warning(f'get() -> 最前一个数据的seq序列为{self._first_seq()} <= {self._current_seq}将被抛弃.')
        #     if self._queue.empty():
        #         self._update_endurance_limit()
        #         return None

        # 只允许根据seq的顺序抛出
        # 既最前面一个数据(index=0, 队列最左边准备被get的数据) 必须是按顺序的
        # 1. first_seq = current_seq + 1 -> 抛出 -> 最正常的状态, 每一笔数据都是按顺序的
        # 2. first_seq > current_seq + 1 -> 堵塞 -> 异常的状态, 数据顺序错乱, 需要堵塞直到数据是按顺序的
        # 3. first_seq < current_seq + 1 -> 不会发生
        if self._first_seq() == self._current_seq + 1:
            return self._get_single()

        if self._out_of_endurance_limit():
            self.logger_warning(f'get() -> 超过seq序列顺序错误容忍极限, '
                                f'最前一个数据(seq={self._first_seq()})将被无视顺序直接返回.')
            return self._get_single()

        self.logger_warning(f'get() -> '
                            f'first_seq={self._first_seq()} != {self._current_seq + 1}, '
                            f'qsize={self._queue.qsize()}({self._maxlen}), '
                            f'堵塞数据直到序列顺序正确.')
        return None

    def empty(self):
        return self._queue.empty() and not self._ready_queue

    def size(self):
        return self._queue.qsize() + len(self._ready_queue)

    def clear(self):
        self._queue = PriorityQueue(maxsize=self._maxlen)
        self._ready_queue.clear()

    def reset(self):
        self.clear()
        self._current_seq = DEFAULT_SEQ


class DataGetter(object):
    """
    使用rabbitmq/kombu进行数据传输-数据获取类/消费者
    不支持多线程
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 vhost: str = None, heartbeat: int = None, new_mqbase: bool = False,
                 max_endurance_limit: int = 0, endurance_time_func=time.time):
        args = (host, port, user, password, vhost, heartbeat)
        # 与sequencequeue相关的参数
        self._max_endurance_limit = max_endurance_limit
        self._endurance_time_func = endurance_time_func

        self._mqbase = MQBaseClass(*args) if new_mqbase else MQBaseClass.get_instance(*args)
        # 添加的消费者
        self._queues: Dict[str, _DataQueue] = dict()
        # 消费者
        self._consumer: Consumer = self._mqbase.get_comsumer(callback=self._callback)
        # callback调用时数据将保存至该变量中
        # 队列因为一些机制原因, 在批量传数据的时候
        # 后端数据推送到rabbitmq中, mq中因为通道机制的存在(并发的通道)
        # 可能会导致即使后端按顺序推送数据至mq
        # 最终队列中取到的数据也是乱序的
        # 所以mq中会配置一个顺序队列来进行排序
        self._data_queue_dict: Dict[str, SequenceQueue] = dict()
        self._data_queue_list: List[SequenceQueue] = list()
        self._default_data_queue: list = list()

        # 缓存注册的队列信息
        self._add_info: Dict[str, Tuple] = dict()
        self._connect_timestamp = self._mqbase.connect_timestamp()

    def is_reconnected(self) -> bool:
        """
        判断mqbase是否重新链接了

        """
        return self._connect_timestamp != self._mqbase.connect_timestamp()

    def _callback(self, message: Message):
        """
        回调方法, 在获取队列数据的循环中调用

        :param message: 信息
        """
        # **注意**
        # 只要在同一个connection中, 在任何地方调用了drain_events(), 如comsumer或者其他地方也会调用该方法
        # 如果同时在send数据的时候刚好特征数据队列中也有一笔输入
        # 那么就会通知调用到这个方法
        # 也就意味着可能数据会被冲刷掉
        # 所以要在这里以队列的形式保存数据
        data = MessageData(message.decode(),
                           message.delivery_info['routing_key'],
                           message.headers.get('seq', DEFAULT_SEQ))
        message.ack()
        queue = self._data_queue_dict.get(data.routing_key)
        if queue:
            queue.put(data)
        else:
            self._default_data_queue.append(data)

    def add_node(self, exchange_name: str, queue_name: str, routing_key: Union[str, List[str]] = None,
                 exchange_opts: dict = None, queue_opts: dict = None):
        """
        订阅

        :param exchange_name: 交换机名称
        :param queue_name: 队列名称
        :param routing_key: 路由键
        :param exchange_opts: 交换机参数
        :param queue_opts: 队列参数
        """
        exchange_opts = exchange_opts or dict()
        queue_opts = queue_opts or dict()

        exchange = self._mqbase.get_exchange(exchange_name, **exchange_opts)
        queue = self._mqbase.get_queue(queue_name, exchange, routing_key, **queue_opts)
        self._consumer.add_queue(queue)
        self._queues[queue_name] = _DataQueue(exchange_name, queue_name, exchange, queue)
        self._add_info[queue_name] = (exchange_name, queue_name, routing_key, exchange_opts, queue_opts)
        self._consumer.consume()

        if not routing_key:
            return

        if isinstance(routing_key, str):
            routing_key = [routing_key]

        for key in routing_key:
            seq_queue = SequenceQueue(f'{self.__class__.__name__}:{key}',
                                      max(self._max_endurance_limit * 3, 1),
                                      self._max_endurance_limit,
                                      self._endurance_time_func)
            self._data_queue_dict[key] = seq_queue
            self._data_queue_list.append(seq_queue)

    def has_queue(self, queue: Union[str, int, Queue]) -> bool:
        """
        确认队列是否存在

        :param queue: 队列名称
        :return: 是否存在队列
        """
        if not isinstance(queue, Queue):
            queue = self._queues[queue]
        return self._mqbase.has_queue(queue)

    def declare_queue(self, queue_name: str):
        self._queues[queue_name].queue.queue_declare()

    def _get_data(self) -> List[MessageData]:
        """
        轮询调用数据的机制
        确保每一个点为的数据会被轮询的调用与发送
        """
        for queue in self._data_queue_list:
            val = queue.get()
            if val:
                self._default_data_queue.extend(val)

        # result = self._default_data_queue.copy()
        # self._default_data_queue.clear()
        # return result

        if self._default_data_queue:
            return [self._default_data_queue.pop(0)]

        return []

    def get_data(self, timeout: Union[int, float] = 3) -> List[MessageData]:
        """
        获取数据

        :param timeout: 超时时间
        :return: queue_name, 获取的数据
        """
        val = self._get_data()
        if val:
            return val

        # 注意其他mq的类也会调用drain_events从而可能触发回调callback
        self._mqbase.drain_events(timeout=timeout)
        # 加一个计数器, 防止无限循环卡住
        # count = 0
        # while cou/nt < 60 and self._mqbase.drain_events(timeout=timeout or 0.1):
        #     count += 1

        return self._get_data()

    def clear_node(self):
        """
        清空登记的设备队列
        """
        for queue in list(self._queues.keys()):
            self.pop_node(queue)

    def pop_node(self, queue_name: str):
        """
        移除指定队列

        :param queue_name: 队列名称/router_key
        """
        try:
            # self._add_info.pop(queue_name)
            self._consumer.cancel_by_queue(self._queues.pop(queue_name).queue)
        except:
            pass

    def close_consumer(self):
        if self._consumer:
            self.clear_node()
            try:
                self._consumer.cancel()
            except:
                pass
            self._consumer = None

    def reconnect(self, reconnect: bool = False):
        """
        重新连接服务器
        """
        add_info = list(self._add_info.values())

        self.close_consumer()
        # self._data_queue_list.clear()
        # self._data_queue_dict.clear()

        self._mqbase.connect(reconnect)
        self._consumer = self._mqbase.get_comsumer(callback=self._callback)

        for args in add_info:
            self.add_node(*args)
        self._connect_timestamp = self._mqbase.connect_timestamp()
        return True

    def close_connect(self):
        """
        关闭至服务器的连接
        """
        self.close_consumer()
        self._mqbase.close()
