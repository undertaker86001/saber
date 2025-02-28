"""

@create on: 2020.12.11
"""
from uuid import uuid4
from json import loads
from random import randint

from scdap import config
from scdap.core.mq import DataGetter
from scdap.data import FeatureListDecoder

from .coder import get_feature_list_decoder

from ...base import BaseGetController


class RabbitMQGetController(BaseGetController):
    """
    mq队列版本的数据获取控制器
    参数:
    host: str
    port: int
    user: str
    password: str
    vhost: str
    heartbeat: int/null
    get_timeout: int            获取数据堵塞的超时时间
    exchange_name: str          队列数据网关交换机名称
    routing_key_prefix: str     routing_key前缀规则
    queue_name_prefix: str      队列名称前缀

    """
    def transfer_mode(self) -> str:
        return 'rabbitmq'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_closed = False
        self._data_exchange = self._get_option('exchange_name', config.RABBITMQ_GET_EXCHANGE)
        self._routing_key_prefix = self._get_option('routing_key_prefix', config.RABBITMQ_GET_ROUTING_KEY_PREFIX)
        self._queue_name_prefix = self._get_option('queue_name_prefix', config.RABBITMQ_GET_QUEUE_NAME_PERFIX)
        self._max_endurance_limit = self._get_option('max_endurance_limit', config.MAX_ENDURANCE_LIMIT)

        self._get_timeout = self._get_option('get_timeout', config.RABBITMQ_GET_TIMEOUT)

        host = self._get_option('host', config.RABBITMQ_HOST)
        port = self._get_option('port', config.RABBITMQ_PORT)
        user = self._get_option('user', config.RABBITMQ_USER)
        password = self._get_option('password', config.RABBITMQ_PASSWORD)
        vhost = self._get_option('vhost', config.RABBITMQ_VHOST)
        heartbeat = self._get_option('heartbeat', config.RABBITMQ_HEARTBEAT)

        self._getter = DataGetter(host, port, user, password, vhost, heartbeat,
                                  max_endurance_limit=self._max_endurance_limit,
                                  endurance_time_func=self._context.systimestamp_s)

        # queue_name到算法点位编号的映射
        self._routing_key_to_cont = dict()

        # binding: <prefix>.<node>
        routing_keys = list()
        for node, dev in zip(self._context.nodes, self._context.devices):
            routing_key = f'{self._routing_key_prefix}.{node}'
            self._routing_key_to_cont[routing_key] = self._context.crimp.get_container(dev)
            routing_keys.append(routing_key)

        queue_name = f'{self._queue_name_prefix}' \
                     f'.aid={self._context.algorithm_id}' \
                     f'.nid={self._context.node_id}' \
                     f'.v1'
        self.logger_info(f'queue name: {queue_name}, binding: {routing_keys}')
        self._getter.add_node(self._data_exchange, queue_name, routing_keys)

    def _create_decoder(self) -> FeatureListDecoder:
        return get_feature_list_decoder()

    def run(self):
        # 因为send与get共享一个rabbitmq链接
        # 所以当某controller触发重连机制的时候
        # 另一个controller是不知道的
        # 所以在此引入一个检测机制
        # 检测是否有重连
        # 如果重连了的话则重新配置相关的订阅操作
        if self._getter.is_reconnected():
            self.logger_error(f"mq服务连接已经重新建立, 将重置相关队列.")
            self._reconnect(False)

        try:
            messages = self._getter.get_data(self._get_timeout)
        except Exception as exce:
            # 在某些情况下
            # 如果进程接收到sigterm的同时, rabbitmq正在调用drain_event
            # 那么在调用close后, 程序会反过来在此处触发链接失效的异常
            # 也就是
            # -> close():
            #   -> rabbitmq.close()
            # -> drain_event() -> raise Exception
            # -> except Exception
            # ->    do when except exception
            # -> exit(0)
            # 所以在这里必须增加一个判断是否关机的变量
            # 防止在触发关闭的时候接住了错误导致重连
            if self._is_closed:
                return

            self.logger_error(f"mq服务连接失败, 无法获取数据, 将重新建立与mq服务的链接, 错误: {exce}")
            self.logger_exception(exce)
            self._reconnect(True)
            return

        if not messages:
            self.logger_debug(f'没有获取任何数据.')
            return

        aids = set()
        nids = set()
        size = 0
        for message in messages:
            # 解析json数据
            try:
                data = loads(message.data)
            except Exception as exce:
                self.logger_warning(f"数据解码失败, 错误: {exce}.")
                return

            container = self._routing_key_to_cont.get(message.routing_key)
            if container is None:
                return

            # 将数据转换成可以解码的结构
            aid = container.get_algorithm_id()
            nid = container.get_node_id()
            data = {
                'algorithmId': aid,
                'nodeId': nid,
                'data': [data]
            }
            # 进入解码接口进行解码
            self._decode(container, data)
            size += 1
            nids.add(nid)
            aids.add(aid)

        if size:
            self.logger_seco(f'get -> [nid: {list(nids)}, aid: {list(aids)}] [size: {size}]')

    def _reconnect(self, reconnect: bool) -> bool:
        try:
            result = self._getter.reconnect(reconnect)
            self.logger_warning(f'mq服务成功重新建立链接.')
            return result
        except Exception as exce:
            max_num = self._delayer.get_max_num()
            max_num = max_num + randint(0, int(max_num / 2))
            self.logger_error(f'mq服务无法重新建立链接, 将在{max_num}s后重试, 错误: {exce}')
            self._delayer.start(max_num)
            return False

    def close(self):
        self._getter.close_connect()
        self._is_closed = True
        super().close()
