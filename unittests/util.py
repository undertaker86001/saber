"""

@create on: 2021.01.11
"""
from datetime import datetime
from random import randint

import numpy as np


def rand_event(health_define: list = None):
    from scdap.data import Event
    event = Event()
    if health_define is None:
        health_define = [str(randint(0, 100)) for i in range(3)]
    event.algorithm_id = str(randint(0, 100))
    event.node_id = randint(0, 100)
    event.status = randint(0, 10)
    event.etype = randint(0, 1)
    event.start = rand_datetime()
    event.stop = rand_datetime()
    event.time = rand_datetime(),
    event.message = str(randint(0, 100))
    event.score = {hd: randint(0, 100) for hd in health_define}
    event.name = str(randint(0, 100))
    event.check_result = randint(0, 100)
    event.code = randint(0, 100)
    return event


def rand_event_dict(health_define: list = None):
    if health_define is None:
        health_define = [str(randint(0, 100)) for i in range(3)]
    return {
        'algorithm_id': str(randint(0, 100)),
        'node_id': randint(0, 100),
        'status': randint(0, 10),
        'etype': randint(0, 1),
        'start': rand_timestamp(),
        'stop': rand_timestamp(),
        'time': rand_timestamp(),
        'message': str(randint(0, 100)),
        'score': {hd: randint(0, 100) for hd in health_define},
        'name': str(randint(0, 100)),
        'check_result': randint(0, 100),
        'code': randint(0, 100)
    }


def rand_feature_dict(fsize=24):
    return {
        'meanhf': randint(0, 100000),
        'meanlf': randint(0, 100000),
        'mean': randint(0, 100000),
        'std': randint(0, 100000),

        # 注意对于时间相关的数值为毫秒时间戳, 并且前三位尽量为000
        # 在操作过程中因为涉及到转型会丢失小部分精度故可能出现某一个时间点误差为1毫秒导致检测不通过的情况
        'time': rand_timestamp(),
        'feature1': rand_string_array(fsize, 10),
        'feature2': rand_string_array(fsize, 10),
        'feature3': rand_string_array(fsize, 10),
        'feature4': rand_string_array(fsize, 10),
        'hrtime': [rand_timestamp() for _ in range(fsize)],
        'bandspectrum': rand_string_array(fsize, 10),
        'peakfreqs': rand_string_array(randint(0, 10), 10),
        'peakpowers': rand_string_array(randint(0, 10), 10),

        'status': randint(0, 10),
        'customfeature': rand_string_array(fsize, 10),
        'temperature': randint(-100, 100)
    }


def rand_feature(fsize=24):
    from scdap.data import FeatureItem
    feature = FeatureItem()
    feature.meanhf = randint(0, 100000)
    feature.meanlf = randint(0, 100000)
    feature.mean = randint(0, 100000)
    feature.std = randint(0, 100000)
    feature.time = rand_datetime()

    feature.feature1 = rand_ndarray(fsize, 10000)
    feature.feature2 = rand_ndarray(fsize, 10000)
    feature.feature3 = rand_ndarray(fsize, 10000)
    feature.feature4 = rand_ndarray(fsize, 10000)

    feature.hrtime = [rand_timestamp() for _ in range(fsize)]
    feature.bandspectrum = rand_ndarray(fsize, 10000)
    feature.peakfreqs = rand_ndarray(randint(0, 10), 10)
    feature.peakpowers = rand_ndarray(randint(0, 10), 10)
    feature.status = randint(0, 10)
    feature.customfeature = rand_ndarray(fsize, 10000)
    feature.temperature = randint(-100, 100)
    return feature


def rand_result_dict(esize=(0, 5)) -> dict:
    health_define = [str(randint(0, 100)) for i in range(3)]
    return {
        'status': randint(0, 10),
        'time': rand_timestamp(),
        'score': rand_array(3, 0, 100),
        'health_define': health_define,
        'event': sorted([rand_event_dict(health_define) for i in range(randint(*esize))], key=lambda e: e['etype'])
    }


def rand_result(esize=(0, 5)):
    from scdap.data import ResultItem
    result = ResultItem()
    result.status = randint(0, 10)
    result.time = rand_datetime()
    result.score = rand_array(3, 0, 100)
    result.health_define = [str(randint(0, 100)) for _ in range(3)]

    for _ in range(randint(*esize)):
        event = rand_event(result.health_define)
        result.event.setdefault(event.etype, list()).append(event)
    return result


def rand_rlist_dict(size=10) -> dict:
    return {
        'algorithm_id': str(randint(0, 10000)),
        'node_id': randint(0, 10000),
        'data': [rand_result_dict() for _ in range(0, size)]
    }


def rand_rlist(size=10):
    from scdap.data import ResultList
    rlist = ResultList()
    rlist.algorithm_id = str(randint(0, 1000))
    rlist.node_id = randint(0, 1000)
    rlist.extend_itemlist([rand_result() for _ in range(0, size)])
    return rlist


def rand_flist_dict(size=10) -> dict:
    return {
        'algorithm_id': str(randint(0, 10000)),
        'node_id': randint(0, 10000),
        'data': [rand_feature_dict() for _ in range(0, size)]
    }


def rand_flist(size=10):
    from scdap.data import FeatureList
    flist = FeatureList()
    flist._algorithm_id = str(randint(0, 1000))
    flist._node_id = randint(0, 1000)
    flist.extend_itemlist([rand_feature() for _ in range(0, size)])
    return flist


def rand_array(size, min_v, max_v):
    return [randint(min_v, max_v) for _ in range(size)]


def rand_string_array(size: int, radio: int):
    """
    获取随机数组
    :param size:
    :param radio:
    :return:
    """
    return ','.join(map(lambda i: str(np.round(i, 1)), np.abs(np.random.normal(size=size)) * radio))


def rand_timestamp():
    """
    获取随机时间戳
    :return:
    """
    return randint(943891200, 1609430400) * 1000


def rand_datetime():
    return datetime.fromtimestamp(rand_timestamp() / 1000)


def rand_ndarray(size: int, radio: int):
    return np.abs(np.round(np.random.normal(size=size) * radio, 1))
