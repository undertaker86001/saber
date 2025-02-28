"""

@create on: 2021.06.04
"""
from random import randint
from datetime import datetime
from scdap.data.result_item import Event, StatItem


def assert_result_list(rlist, data, index):

    assert rlist.get_status(index) == data['status']
    assert rlist.get_time(index) == data['time']
    assert rlist.get_event(index) == data['event']
    assert rlist.get_stat_item(index) == data['stat_item']
    assert rlist.get_score(index) == data['score']
    assert rlist.get_health_define(index) == data['health_define']


def assert_event(e1: Event, e2: Event):
    assert e1.algorithm_id == e2.algorithm_id
    assert e1.node_id == e2.node_id
    assert e1.start == e2.start
    assert e1.stop == e2.stop
    assert e1.time == e2.time
    assert e1.score == e2.score
    assert e1.check_result == e2.check_result
    assert e1.etype == e2.etype
    assert e1.message == e2.message
    assert e1.name == e2.name
    assert e1.code == e2.code
    assert e1.status == e2.status
    assert e1.detail == e2.detail
    assert e1.extend == e2.extend


def assert_result_item(item, data):
    assert item.status == data['status']
    assert item.time == data['time']
    assert item.event == data['event']
    assert item.stat_item == data['stat_item']
    assert item.score == data['score']
    assert item.health_define == data['health_define']


def random_string():
    return str(randint(0, 100))


def random_event(hd):
    node_id = randint(0, 100)
    event = Event(randint(0, 4), str(node_id), node_id, randint(0, 10),
                  dict(zip(hd, [randint(50, 90) for _ in hd])), random_string(), random_time(),
                  random_time(), random_time(), random_string(), randint(0, 10), randint(0, 10), str(randint(0, 100)),
                  {random_string(): random_string()})
    return event


def random_stat_item(hd):
    return StatItem(random_time(), 60, {'1': 40, '0': 20}, dict(zip(hd, [randint(50, 90) for _ in hd])))


def random_item_dict(hd=('trend', 'stab')):
    event = random_event(hd)
    return {
        'status': randint(0, 10),
        'time': random_time(),
        'score': [randint(60, 90), randint(60, 90)],
        'health_define': list(hd),
        'event': [event],
        'stat_item': random_stat_item(hd)
    }


def random_itemlist_dict(hd=('trend', 'stab'), size=3):
    node_id = randint(0, 100)
    return {
        'algorithm_id': str(node_id),
        'node_id': node_id,
        'data': [random_item_dict(hd) for _ in range(size)]
    }


def random_time():
    return datetime.fromtimestamp(round(datetime.now().timestamp(), 3))


def random_list_dict(size=3, hd=('trend', 'stab')):

    return {
        'status': [randint(0, 10) for _ in range(size)],
        'time': [random_time() for _ in range(size)],
        'score': [[randint(60, 90), randint(60, 90)] for _ in range(size)],
        'health_define': [list(hd) for _ in range(size)],
        'event': [[random_event(hd)] for _ in range(size)],
        'stat_item': [random_stat_item(hd) for _ in range(size)]
    }


def item_to_decoder_src(d: dict):
    return {
        'status': d['status'],
        'time': int(d['time'].timestamp() * 1000),
        'score': d['score'],
        'health_define': d['health_define'],
        'event': [
            {
                'algorithm_id': e.algorithm_id,
                'node_id': e.node_id,
                'start': int(e.start.timestamp() * 1000) if e.start else None,
                'stop': int(e.stop.timestamp() * 1000) if e.stop else None,
                'etype': e.etype,
                'time': int(e.time.timestamp() * 1000) if e.time else None,
                'message': e.message,
                'name': e.name,
                'score': e.score,
                'code': e.code,
                'check_result': e.check_result,
                'status': e.status,
                'detail': e.detail,
                'extend': e.extend,
            } for e in d['event']
        ],
        'stat_item': {
            'time': int(d['stat_item'].time.timestamp() * 1000),
            'size': d['stat_item'].size,
            'status': d['stat_item'].status,
            'score': d['stat_item'].score
        }
    }


def itemlist_to_decoder_src(d: dict):
    return {
        'algorithm_id': d['algorithm_id'],
        'node_id': d['node_id'],
        'data': list(map(item_to_decoder_src, d['data']))
    }
