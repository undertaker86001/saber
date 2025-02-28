"""

@create on: 2021.01.02
"""

from scdap.data import FeatureListKV, FeatureItemKV
from scdap.data import FeatureListDecoder, FeatureItemDecoder, FeatureItemEncoder, FeatureListEncoder


class RabbitMQFeatureListKV(FeatureListKV):
    algorithm_id = 'algorithmId'
    node_id = 'nodeId'
    data = 'data'


class RabbitMQFeatureItemKV(FeatureItemKV):
    """
    在这里配置通过rabbitmq进行数据交互的特征字段名称
    会与FeatureItemKV配置的不一样
    """
    status = 'status'
    meanhf = 'meanHf'
    meanlf = 'meanLf'
    mean = 'mean'
    std = 'std'

    # 高分特征字段
    feature1 = 'feature1'
    feature2 = 'feature2'
    feature3 = 'feature3'
    feature4 = 'feature4'

    bandspectrum = 'bandSpectrum'
    peakfreqs = 'peakFreqs'
    peakpowers = 'peakPowers'

    time = 'time'
    hrtime = 'hrtime'

    customfeature = 'customFeature'
    temperature = 'temperature'
    extend = 'extend'


def get_feature_list_decoder():
    fi_decoder = FeatureItemDecoder(RabbitMQFeatureItemKV())
    return FeatureListDecoder(RabbitMQFeatureListKV(), fi_decoder)


def get_feature_list_encoder():
    fi_encoder = FeatureItemEncoder(RabbitMQFeatureItemKV())
    return FeatureListEncoder(RabbitMQFeatureListKV(), fi_encoder)

