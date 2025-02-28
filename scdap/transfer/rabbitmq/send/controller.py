"""

@create on: 2020.12.11
"""
from typing import Dict
from random import randint
from collections import deque

from scdap import config, data
from scdap.core.mq import DataSender

from ...base import BaseSendController
from .coder import get_result_list_encoder
from scdap.middleware.limit import KeyFrequencyLimitation


class RabbitMQSendController(BaseSendController):
    """
    参数:
    host: str
    port: int
    user: str
    password: str
    vhost: str
    heartbeat: int/null
    cachelen: int           缓存的未发送的数据容量, 如果超过容量数据将被抛弃, 遵循先进先出的原则
    queue_name: str         发送结果数据的队列名称
    exchange: str           交换机名称
    has_feature: bool       是否在结果数据中附带特征数据
    """
    def transfer_mode(self) -> str:
        return 'rabbitmq'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_closed = False
        host = self._get_option('host', config.RABBITMQ_HOST)
        port = self._get_option('port', config.RABBITMQ_PORT)
        user = self._get_option('user', config.RABBITMQ_USER)
        password = self._get_option('password', config.RABBITMQ_PASSWORD)
        vhost = self._get_option('vhost', config.RABBITMQ_VHOST)
        heartbeat = self._get_option('heartbeat', config.RABBITMQ_HEARTBEAT)

        self._sender = DataSender(host, port, user, password, vhost, heartbeat)

        self._cachelen = self._get_option('cachelen', config.RESULT_CACHELEN)
        self._queue_name = self._get_option('queue_name', config.RABBITMQ_SEND_QUEUE_NAME)
        self._exchange_name = self._get_option('exchange_name', config.RABBITMQ_SEND_EXCHANGE)

        # 由后端创建队列
        self._sender.add_node(self._exchange_name, self._queue_name, queue_opts={'no_declare': True})
        self._cache = deque(maxlen=self._cachelen)

        self._has_feature = self._get_option('has_feature', False)
        self._encoder = self._create_encoder()
        self._feature_cache: Dict[str, deque] = {aid: deque(maxlen=self._cachelen) for aid in self._context.devices}
        self.limiter = KeyFrequencyLimitation(config.LIMIT_EVENT)

    def _create_encoder(self) -> data.ResultListEncoder:
        return get_result_list_encoder()

    def reset(self):
        for cache in self._feature_cache.values():
            cache.clear()

    def run(self):
        # 因为send与get共享一个rabbitmq链接
        # 所以当某controller触发重连机制的时候
        # 另一个controller是不知道的
        # 所以在此引入一个检测机制
        # 检测是否有重连
        # 如果重连了的话则重新配置相关的订阅操作
        if self._sender.is_reconnected():
            self.logger_error(f"mq服务连接已经重新建立, 将重置相关队列.")
            self._reconnect(False)
        aids = set()
        nids = set()
        size = 0
        while self._cache:
            nid, aid, result = self._cache.popleft()
            try:
                # 后端的结果数据接收队列配置的交换机类型是direct
                # 意味着队列的routing_key必须与队列名称相同
                # 队列才能够接收到数据
                result = self.limiter.limit_event(result)  # 限制算法的事件频率
                self.logger_info(str(result))
                if self._sender.send_data(self._queue_name, result, self._queue_name):
                    aids.add(aid)
                    nids.add(nid)
                    size += 1
                else:
                    self.logger_warning(
                        f'mq服务中不存在队列: [{self._queue_name}], '
                        f'在{self._delayer.get_max_num()}s后尝试再次发送数据.'
                    )
                    self._cache.appendleft((nid, aid, result))
                    self._delayer.start()
                    break
            except Exception as e:
                # 在某些情况下
                # 如果进程接收到sigterm的同时, rabbitmq正在调用drain_event
                # 那么在调用close后, 程序会反过来在此处触发链接失效的异常
                # 也就是
                # -> close():
                #   -> rabbitmq.close()
                # -> drain_event() -> raise Exception
                # -> except Exception:
                # ->    do something when except exception
                # -> exit(0)
                # 所以在这里必须增加一个判断是否关机的变量
                # 防止在触发关闭的时候接住了错误导致重连
                if self._is_closed:
                    return

                self.logger_error(f"mq服务操作失败, 准备重连, 错误: {e}")
                self.logger_exception(e)
                # 对于发送失败的数据将重回队列之中
                # 并且应该放置在最左边
                self._cache.appendleft((nid, aid, result))
                self._reconnect(True)
                self.exception(e)
                break

        if size:
            self.logger_seco(f'send -> [nid: {list(nids)}, aid: {list(aids)}] [size: {size}]')

    def need_run(self) -> bool:
        for aid, cont, res in self._context.crimp.generator_dcr():
            # 部分环境下需要返回带特征的结果数据
            fcache = self._feature_cache[aid]
            nid = cont.get_node_id()
            if self._has_feature:
                for obj in cont.flist:
                    fcache.append(self._encoder.encode(obj))

            if res.empty():
                continue

            for obj in self._encode(res):
                if self._has_feature:
                    obj['feature'] = fcache.popleft()

                self._cache.append((nid, aid, obj))

        return super().need_run()

    def _reconnect(self, reconnect: bool) -> bool:
        try:
            return self._sender.reconnect(reconnect)
        except Exception as exce:
            max_num = self._delayer.get_max_num()
            max_num = max_num + randint(0, int(max_num / 2))
            self.logger_error(f'mq数据发送服务重连失败, 将在{max_num}s后重试, 错误: {exce}')
            self._delayer.start(max_num)
            return False

    def close(self):
        self._sender.close_connect()
        self._is_closed = True
        super().close()
