"""

@create on: 2021.02.02
"""
import os
import json
import time
import logging
from pprint import pprint
from typing import Dict
from threading import Thread

from flask import Flask, request

from scdap import config
from scdap.core.mq import DataBroadcast, DataGetter
from scdap.transfer import rabbitmq

from .stack import Stack

conf = (
    config.RABBITMQ_HOST, config.RABBITMQ_PORT,
    config.RABBITMQ_USER, config.RABBITMQ_PASSWORD,
    config.RABBITMQ_VHOST
)
delta_time = 1

app = Flask(__name__)
app.logger.disabled = True
# 关闭日志
log = logging.getLogger('werkzeug')
log.disabled = True
log.setLevel(logging.FATAL)
os.environ["WERKZEUG_RUN_MAIN"] = "true"

stask: Dict[str, Stack] = dict()

rabbitmq_encoder = rabbitmq.get_feature_list_encoder().item_encoder



def print_red(*message):
    print('\033[0;31m%s\033[0m' % ' '.join(map(str, message)))


def print_green(*message):
    print('\033[0;32m%s\033[0m' % ' '.join(map(str, message)))


@app.route('/register/', methods=['POST'])
def register():
    """
    在debug模式下进程启动时会通过该接口发送登记信息
    记录需要获取的特征名称以及联动的算法点位编号列表
    :return:
    """
    r = request.get_json()
    pprint(r)
    s = Stack(r['dev'], r['transfer'])
    for dev in r['devices']:
        stask[dev] = s
    return {'message': 'ok', 'code': 200}


@app.route('/py/api/data/put/', methods=['POST'])
def put():
    resp = json.loads(request.get_data())
    print_green('http get data:', resp)
    return {'code': 200}


def boardcast_data():
    print('start send data')
    broadcast = DataBroadcast(*conf, new_mqbase=True)
    exchange_name = 'gateway-node-data-exchange'
    broadcast.add_exchange(exchange_name)
    while 1:
        next_time = time.time() + delta_time

        for dev, s in stask.copy().items():
            if s.transfer != 'rabbitmq':
                continue

            seq = s.get_seq(dev)
            # if random.randint(0, 5) == 0:
            #     continue

            data, flist = s.get(dev)

            print_red(f'rabbitmq send dev: {dev}, seq: {seq}, {data}')
            if data:
                data = rabbitmq_encoder.encode(data, flist)
                broadcast.broadcast(exchange_name, data, f'scene.{dev}',  seq)
        time.sleep(max(0., next_time - time.time()))


def get_data():
    getter = DataGetter(*conf, new_mqbase=True)
    getter.add_node('', 'py.compute.result')
    while 1:
        print_green('rabbitmq get data:', getter.get_data())


def main(delta=1, start_time=None):
    if start_time:
        Stack.start = start_time
    global delta_time
    delta_time = delta
    thread = Thread(target=app.run, daemon=True, args=('0.0.0.0', 8846))
    thread.start()
    thread = Thread(target=get_data, daemon=True)
    thread.start()
    boardcast_data()


if __name__ == '__main__':
    main()
