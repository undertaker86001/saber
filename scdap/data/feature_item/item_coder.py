"""

@create on: 2021.05.20
"""
from scdap.util.tc import DATETIME_MIN_TIMESTAMP
from scdap.util.tc import datetime_to_long, long_to_datetime, string_to_array, array_to_string

from .item_list import FeatureList
from .item import FeatureItem, DEFAULT_TEMPERATURE
from ..coder import RefItemEncoder, RefItemDecoder, TYPE_JSON


class FeatureItemKV(object):
    status = 'status'
    meanhf = 'meanhf'
    meanlf = 'meanlf'
    mean = 'mean'
    std = 'std'

    # 高分特征字段
    feature1 = 'feature1'
    feature2 = 'feature2'
    feature3 = 'feature3'
    feature4 = 'feature4'

    bandspectrum = 'bandspectrum'
    peakfreqs = 'peakfreqs'
    peakpowers = 'peakpowers'

    time = 'time'
    hrtime = 'hrtime'

    customfeature = 'customfeature'
    temperature = 'temperature'
    extend = 'extend'


class FeatureItemEncoder(RefItemEncoder[FeatureList, FeatureItem, FeatureItemKV]):
    def encode_status(self, obj: FeatureItem):
        return int(obj.status)

    def encode_meanhf(self, obj: FeatureItem):
        return int(obj.meanhf)

    def encode_meanlf(self, obj: FeatureItem):
        return int(obj.meanlf)

    def encode_mean(self, obj: FeatureItem):
        return int(obj.mean)

    def encode_std(self, obj: FeatureItem):
        return int(obj.std)

    def encode_time(self, obj: FeatureItem):
        return datetime_to_long(obj.time)

    def encode_feature1(self, obj: FeatureItem):
        return array_to_string(obj.feature1, ln=True)

    def encode_feature2(self, obj: FeatureItem):
        return array_to_string(obj.feature2, ln=True)

    def encode_feature3(self, obj: FeatureItem):
        return array_to_string(obj.feature3, ln=True)

    def encode_feature4(self, obj: FeatureItem):
        return array_to_string(obj.feature4, ln=True)

    def encode_hrtime(self, obj: FeatureItem):
        return [datetime_to_long(time) for time in obj.hrtime]

    def encode_bandspectrum(self, obj: FeatureItem):
        return array_to_string(obj.bandspectrum, ln=True)

    def encode_peakfreqs(self, obj: FeatureItem):
        return array_to_string(obj.peakfreqs, ln=True)

    def encode_peakpowers(self, obj: FeatureItem):
        return array_to_string(obj.peakpowers, ln=True)

    def encode_customfeature(self, obj: FeatureItem):
        return array_to_string(obj.customfeature, ln=False)

    def encode_temperature(self, obj: FeatureItem):
        return int(obj.temperature)

    def encode_extend(self, obj: FeatureItem):
        return obj.extend


class FeatureItemDecoder(RefItemDecoder[FeatureList, FeatureItem, FeatureItemKV]):
    def decode_status(self, obj: TYPE_JSON):
        return int(obj.get(self.kv.status) or 0)

    def decode_meanhf(self, obj: TYPE_JSON):
        return float(obj.get(self.kv.meanhf, 0.))

    def decode_meanlf(self, obj: TYPE_JSON):
        return float(obj.get(self.kv.meanlf, 0.))

    def decode_mean(self, obj: TYPE_JSON):
        return float(obj.get(self.kv.mean, 0.))

    def decode_std(self, obj: TYPE_JSON):
        return float(obj.get(self.kv.std, 0.))

    def decode_time(self, obj: TYPE_JSON):
        return long_to_datetime(int(obj.get(self.kv.time, DATETIME_MIN_TIMESTAMP)))

    def decode_feature1(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.feature1, ''), exp=True)

    def decode_feature2(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.feature2, ''), exp=True)

    def decode_feature3(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.feature3, ''), exp=True)

    def decode_feature4(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.feature4, ''), exp=True)

    def decode_bandspectrum(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.bandspectrum, ''), exp=True)

    def decode_hrtime(self, obj: TYPE_JSON):
        return [long_to_datetime(time) for time in obj.get(self.kv.hrtime, list())]

    def decode_peakfreqs(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.peakfreqs, ''), exp=True)

    def decode_peakpowers(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.peakpowers, ''), exp=True)

    def decode_customfeature(self, obj: TYPE_JSON):
        return string_to_array(obj.get(self.kv.customfeature, ''), exp=False)

    def decode_temperature(self, obj: TYPE_JSON):
        val = obj.get(self.kv.temperature)
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
        return DEFAULT_TEMPERATURE

    def decode_extend(self, obj: TYPE_JSON):
        return obj.get(self.kv.extend) or dict()
