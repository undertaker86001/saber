"""

@create on: 2021.01.24
"""
import json
import pytest
from pyrabbit import Client

from scdap.core.mq import DataBroadcast, DataGetter, DataSender


class TestRabbitMQ(object):
    conf = {
        'host': '119.3.26.214',
        'port': 5672,
        'user': 'dap-cicd',
        'password': 'O2rHMeiX',
        'vhost': '/dap-cicd',
        'new_mqbase': True
    }

    exchange = 'dap-cicd-test-gateway'
    result_queue = 'py.compute.result'

    def test_broadcast_to_getter(self):
        # 测试数据接收 getter / 广播器 broadcast
        queue = 'test_broadcast_to_getter'
        routing_key = queue + '1'
        exchange = 'dap-cicd-unittest'

        broadcast = DataBroadcast(**self.conf)
        getter = DataGetter(**self.conf)
        try:
            broadcast.add_exchange(exchange)
            getter.add_node(exchange, queue, routing_key)
            for i in range(10):
                data = {'data': i}
                broadcast.broadcast(exchange, data, routing_key)
                # time.sleep(1)
                assert json.loads(getter.get_data()[0].data) == data
        finally:
            getter.close_connect()
            broadcast.close_connect()

    def test_broadcast_to_getter_multi_routing_key(self):
        # 测试数据接收 getter / 广播器 broadcast
        queue = 'test_broadcast_to_getter_multi_routing_key'
        routing_key = ['1', '2', '3', '4']
        routing_key = [queue + rk for rk in routing_key]
        exchange = 'dap-cicd-unittest'
        broadcast = DataBroadcast(**self.conf)
        getter = DataGetter(**self.conf)
        try:
            broadcast.add_exchange(exchange)
            getter.add_node(exchange, queue, routing_key)
            for i in range(10):
                data = {'data': i}
                for rk in routing_key:
                    broadcast.broadcast(exchange, data, rk)
                    # time.sleep(1)
                    resp = getter.get_data()[0]
                    assert resp.routing_key == rk
                    assert json.loads(resp.data) == data
        finally:
            getter.close_connect()
            broadcast.close_connect()

    def test_sender_to_getter(self):
        # 测试数据接收 getter /  sender
        queue = 'test_sender_to_getter'
        routing_key = queue + '1'
        exchange = 'dap-cicd-unittest'
        sender = DataSender(**self.conf)
        getter = DataGetter(**self.conf)
        try:
            sender.add_node(exchange, queue)
            getter.add_node(exchange, queue, routing_key)
            for i in range(10):
                data = json.dumps({'data': i})
                sender.send_data(queue, data, routing_key)
                # time.sleep(1)
                assert getter.get_data()[0].data == data
        finally:
            getter.close_connect()
            sender.close_connect()

    def test_getter_when_delete_queue_suddenly(self):
        queue = 'test_getter_when_delete_queue_suddenly'
        routing_key = queue + '1'
        exchange = 'dap-cicd-unittest'
        broadcast = DataBroadcast(**self.conf)
        getter = DataGetter(**self.conf)
        try:
            broadcast.add_exchange(exchange)
            getter.add_node(exchange, queue, routing_key)
            for i in range(10):
                data = {'data': i}
                broadcast.broadcast(exchange, data, routing_key)
                # time.sleep(1)
                assert json.loads(getter.get_data()[0].data) == data

            # 删除队列
            manager: Client = getter._mqbase._connection.get_manager()
            manager.delete_queue(self.conf['vhost'], queue)

            data = {'data': 0}
            broadcast.broadcast(exchange, data, routing_key)
            with pytest.raises(Exception):
                json.loads(getter.get_data()[0].data)
            getter.reconnect()
            for i in range(10):
                data = {'data': i}
                broadcast.broadcast(exchange, data, routing_key)
                # time.sleep(1)
                assert json.loads(getter.get_data()[0].data) == data
        finally:
            getter.close_connect()
            broadcast.close_connect()

    def test_sender_when_delete_queue_suddenly(self):
        # 测试数据接收 getter /  sender
        queue = 'test_sender_when_delete_queue_suddenly'
        routing_key = queue + '1'
        exchange = 'dap-cicd-unittest'
        sender = DataSender(**self.conf)
        getter = DataGetter(**self.conf)
        try:
            sender.add_node(exchange, queue)
            getter.add_node(exchange, queue, routing_key)
            for i in range(10):
                data = json.dumps({'data': i})
                sender.send_data(queue, data, routing_key)
                # time.sleep(1)
                assert getter.get_data()[0].data == data

            # 删除队列
            manager: Client = getter._mqbase._connection.get_manager()
            manager.delete_queue(self.conf['vhost'], queue)
            assert not sender.send_data(queue, json.dumps({'data': 0}))

            # 重连
            getter.reconnect()
            sender.reconnect()

            for i in range(10):
                data = json.dumps({'data': i})
                sender.send_data(queue, data)
                # time.sleep(1)
                assert getter.get_data()[0].data == data
        finally:
            getter.close_connect()
            sender.close_connect()
