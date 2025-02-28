"""

@create on: 2021.06.02
"""
import random
from datetime import datetime

import numpy as np

from scdap.data.feature_item import FeatureList


def column():
    return [
        'meanhf', 'meanlf', 'mean', 'std',
        'feature1', 'feature2', 'feature3', 'feature4',
        'bandspectrum', 'peakfreqs', 'peakpowers',
        'status', 'customfeature', 'temperature',
        'hrtime', 'time', 'extend'
    ]


def assert_feature_list(flist, data, index):
    assert flist.get_meanhf(index) == data['meanhf']
    assert flist.get_meanlf(index) == data['meanlf']
    assert flist.get_mean(index) == data['mean']
    assert flist.get_std(index) == data['std']
    assert flist.get_feature1(index).tolist() == data['feature1'].tolist()
    assert flist.get_feature2(index).tolist() == data['feature2'].tolist()
    assert flist.get_feature3(index).tolist() == data['feature3'].tolist()
    assert flist.get_feature4(index).tolist() == data['feature4'].tolist()
    assert flist.get_bandspectrum(index).tolist() == data['bandspectrum'].tolist()
    assert flist.get_peakfreqs(index).tolist() == data['peakfreqs'].tolist()
    assert flist.get_peakpowers(index).tolist() == data['peakpowers'].tolist()
    assert flist.get_customfeature(index).tolist() == data['customfeature'].tolist()
    assert flist.get_temperature(index) == data['temperature']
    assert flist.get_hrtime(index) == data['hrtime']
    assert flist.get_time(index) == data['time']
    assert flist.get_extend(index) == data['extend']


def assert_feature_item(item, data):
    assert item.meanhf == data['meanhf']
    assert item.meanlf == data['meanlf']
    assert item.mean == data['mean']
    assert item.std == data['std']

    assert np.round(item.feature1, 0).tolist() == np.round(data['feature1'], 0).tolist()
    assert np.round(item.feature2, 0).tolist() == np.round(data['feature2'], 0).tolist()
    assert np.round(item.feature3, 0).tolist() == np.round(data['feature3'], 0).tolist()
    assert np.round(item.feature4, 0).tolist() == np.round(data['feature4'], 0).tolist()
    assert np.round(item.bandspectrum, 0).tolist() == np.round(data['bandspectrum'], 0).tolist()
    assert np.round(item.peakfreqs, 0).tolist() == np.round(data['peakfreqs'], 0).tolist()
    assert np.round(item.peakpowers, 0).tolist() == np.round(data['peakpowers'], 0).tolist()
    assert item.customfeature.tolist() == data['customfeature'].tolist()
    assert item.temperature == data['temperature']
    assert item.hrtime == data['hrtime']
    assert item.time == data['time']
    assert item.extend == data['extend']


def assert_feature_range(item_list, src_list):
    assert len(item_list) == len(src_list)
    for i in range(len(item_list)):
        v1 = item_list[i]
        v2 = src_list[i]
        if isinstance(v1, np.ndarray):
            v1 = v1.tolist()
            v2 = v2.tolist()
        assert v1 == v2


def random_item_dict():
    def random_int():
        return int(random.random() * 1000)

    def random_ndarray(size):
        return np.round(np.random.random_sample(size) * 1000, 0)

    def random_time():
        return datetime.fromtimestamp(round(datetime.now().timestamp(), 3))

    return {
        'meanhf': random_int(),
        'meanlf': random_int(),
        'mean': random_int(),
        'std': random_int(),
        'feature1': random_ndarray(24),
        'feature2': random_ndarray(24),
        'feature3': random_ndarray(24),
        'feature4': random_ndarray(24),
        'bandspectrum': random_ndarray(20),
        'peakfreqs': random_ndarray(10),
        'peakpowers': random_ndarray(5),
        'status': random.randint(0, 10),
        'customfeature': random_ndarray(24),
        'temperature': random.randint(-100, 100),
        'hrtime': [random_time() for _ in range(24)],
        'time': random_time(),
        'extend': {'test': str(random_int())}
    }


def random_itemlist_dict(size=3):
    node_id = random.randint(0, 100)
    return {
        'algorithm_id': str(node_id),
        'node_id': node_id,
        'data': [random_item_dict() for _ in range(size)]
    }


def item_to_decoder_src(d: dict, rounded):
    temp = dict()

    def normal(key):
        if key in d:
            temp[key] = d[key]

    def ndarray_ln(key):
        if key in d:
            data = np.log(d[key])
            if rounded:
                data = np.round(data, 1)
            temp[key] = ','.join(map(str, data))

    def ndarray(key):
        if key in d:
            data = d[key]
            if rounded:
                data = np.round(d[key], 1)
            temp[key] = ','.join(map(str, data))

    def time(key):
        if key in d:
            temp[key] = int(d[key].timestamp() * 1000)

    def timelist(key):
        if key in d:
            temp[key] = [int(t.timestamp() * 1000) for t in d[key]]

    normal('meanhf')
    normal('meanlf')
    normal('mean')
    normal('std')
    normal('status')
    normal('temperature')
    normal('extend')

    ndarray_ln('feature1')
    ndarray_ln('feature2')
    ndarray_ln('feature3')
    ndarray_ln('feature4')

    ndarray_ln('bandspectrum')
    ndarray_ln('peakfreqs')
    ndarray_ln('peakpowers')
    ndarray('customfeature')

    time('time')
    timelist('hrtime')

    return temp


def itemlist_to_decoder_src(d: dict, rounded):
    return {
        'algorithm_id': d['algorithm_id'],
        'node_id': d['node_id'],
        'data': [item_to_decoder_src(i, rounded) for i in d['data']]
    }


def random_list_dict(size=3):
    return {
        'meanhf': [random.random() * 1000 for _ in range(size)],
        'meanlf': [random.random() * 1000 for _ in range(size)],
        'mean': [random.random() * 1000 for _ in range(size)],
        'std': [random.random() * 1000 for _ in range(size)],
        'feature1': [np.random.random_sample(24) * 1000 for _ in range(size)],
        'feature2': [np.random.random_sample(24) * 1000 for _ in range(size)],
        'feature3': [np.random.random_sample(24) * 1000 for _ in range(size)],
        'feature4': [np.random.random_sample(24) * 1000 for _ in range(size)],
        'bandspectrum': [np.random.random_sample(20) * 1000 for _ in range(size)],
        'peakfreqs': [np.random.random_sample(10) * 1000 for _ in range(size)],
        'peakpowers': [np.random.random_sample(10) * 1000 for _ in range(size)],
        'status': [random.randint(0, 10) for _ in range(size)],
        'customfeature': [np.random.random_sample(10) * 1000 for _ in range(size)],
        'temperature': [random.randint(-100, 100) for _ in range(size)],
        'hrtime': [[datetime.now() for _ in range(24)] for _ in range(size)],
        'time': [datetime.now() for _ in range(size)],
        'extend': [{'test': str(random.random())} for _ in range(size)]
    }


def random_flist(algorithm_id, columns, size):
    flist = FeatureList(algorithm_id, column=columns)
    data = random_list_dict(size)
    flist.extend_ldict(**data)
    return flist
