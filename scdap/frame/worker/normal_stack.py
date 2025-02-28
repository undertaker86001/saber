"""

@create on 2021.01.14
"""
from datetime import datetime
from collections import deque
from typing import Dict, Generator, Tuple, Optional

from scdap.data import FeatureList, FeatureItem

from .base import BaseWorker
from ..function import BaseFunction


class ResultCache(object):
    """
    数据缓存中间层
    """

    def __init__(self, algorithm_id: str, columns: list, maxlen: int = None):
        self.algorithm_id = algorithm_id
        self.time = deque(maxlen=maxlen)
        self.status = deque(maxlen=maxlen)
        self.flist = FeatureList(algorithm_id, column=columns, maxlen=maxlen)

    def size(self):
        return len(self.status)

    def generator_cache(self) -> Generator[Tuple[FeatureItem, int, datetime], None, None]:
        while self.size() > 0:
            yield self.flist.get_ref(0), self.status.popleft(), self.time.popleft()
            self.flist.pop()

    def cache_flist(self, flist: FeatureList):
        self.flist.extend_itemlist(flist)

    def cache_feature(self, feature: FeatureItem):
        self.flist.append_item(feature)

    def clear(self):
        self.time.clear()
        self.status.clear()
        self.flist.clear()

    def add_result(self, status: int, time: datetime):
        self.status.append(status)
        self.time.append(time)

    def get_status(self) -> int:
        if self.size() == 0:
            raise IndexError('请确保已经调用add_result, 只有在调用add_result后才能够使用该接口.')
        return self.status[-1]

    def set_status(self, val: int):
        if self.size() == 0:
            raise IndexError('请确保已经调用add_result, 只有在调用add_result后才能够使用该接口.')
        self.status[-1] = val

    def get_time(self) -> datetime:
        if self.size() == 0:
            raise IndexError('请确保已经调用add_result, 只有在调用add_result后才能够使用该接口.')
        return self.time[-1]

    def set_time(self, val: datetime):
        if self.size() == 0:
            raise IndexError('请确保已经调用add_result, 只有在调用add_result后才能够使用该接口.')
        self.time[-1] = val


class NormalStackWorker(BaseWorker):
    """
    堵塞普通算法工作组
    只允许有拥有一个识别算法, 该算法为堵塞的识别算法
    允许拥有多个评价算法

    计算机制:
    首先decision-result-api中的接口将被替换为ResultCache的接口
    decision中调用result/set_status/get_status/get_time/set_time实际上是调用ResultCache的接口
    1. 所有数据在decision中运行一次
    2. 将container中的特征缓存至各自设备的ResultCache, 并清空container
    3. 检测ResultCache中是否有保存过状态, 即decision中调用过add_result
    4. 如果调用过add_result, 则feautre/status/time按照一一对应的顺序逐个添加至实现的container/result中,
       即在worker中调用feature.flist.append(feature)/result.add_result(status, time)
    5.之后按照正常的方式逐个调用识别算法进行计算
    """
    result_api_kwargs = {
        'decision': {
            'allow_score': False,
        },
        'evaluation': {
            'allow_add_result': False,
        },
        'other': {
        },
    }

    @staticmethod
    def get_worker_name() -> str:
        return 'normal_stack'

    def is_realtime_worker(self) -> bool:
        return False

    @staticmethod
    def process_type() -> str:
        return 'program'

    def __init__(self, *args, **kwargs):
        BaseWorker.__init__(self, *args, **kwargs)
        self._cache: Dict[str, ResultCache] = dict()
        self._function: Optional[BaseFunction] = None

    def _initial_function(self):
        if len(self._opt_decision) != 1:
            raise Exception(f'[{self.get_worker_name()}] 只允许配置一个识别算法.')

        self._register_decision(self._opt_decision[0])
        self._function = self._decision[0]

        if self._function.is_realtime_function():
            raise Exception(f'识别算法必须配置为非实时算法.')
        self._register_evaluation(self._opt_evaluation)

    def bind_crimp(self):
        super().bind_crimp()

        # 替换result中的接口为cache的接口
        # 因为累积算法的特殊性, 会将result的数据接口替换为ResultCache的接口

        def wrapper(result, result_cache: ResultCache, fname: str):
            if hasattr(result, fname):
                setattr(result, fname, getattr(result_cache, fname))

        # 必须确保cache的数据容量与container._maxlen相同以防止特征溢出
        for dev, cont, res in self._crimp.generator_dcr():
            columns = list(map(str, self.get_column()[dev]))
            cache = ResultCache(dev, columns, cont._maxlen)
            for fun in self._decision:
                api = self._api_creater.get_rapi(fun, res)
                wrapper(api, cache, 'add_result')

                wrapper(api, cache, 'set_status')
                wrapper(api, cache, 'get_status')

                wrapper(api, cache, 'set_time')
                wrapper(api, cache, 'get_time')
            self._cache[dev] = cache

    def compute(self):
        # 先运行一遍识别算法
        for device, container, result in self._drive_data(False):

            # 识别算法
            self._run_function(self._function, device, container, result)

            # 拦截特征数据
            # 缓存至cache中, 等待识别算法抛出结果数据的时候再度放到container容器中
            cache = self._cache[device]
            cache.cache_feature(container.flist.get_ref(0))
            container.flist.pop()

        # 第二遍在根据识别算法是否输出结果来决定是否运行评价算法
        for dev, cache in self._cache.items():
            # 拦截特征数据
            # 缓存至cache中, 等待识别算法抛出结果数据的时候再度放到container容器中
            container, result = self._crimp.get_cr(dev)
            # cache.cache_flist(container.flist)
            # container.clear()
            if cache.size() == 0:
                continue

            # print('结果长度：', cache.size(), '特征长度：', cache.flist.size())

            if cache.size() > cache.flist.size():
                raise self.wrap_exception(
                    Exception,
                    f'[{self.get_worker_name()}]中识别算法调用add_result()必须小于等于算法中累积的特征数量,'
                    f'每一个特征都应与结果数据一一对应, '
                    f'不应凭空吞掉特征数据, 也不应凭空生成不存在有特征与之对应的结果数据.'
                )

            # if self._inspect and self.container_size != self.result_size:
            #     raise Exception(f'特征数据数量({self.container_size})与结果数据数据不一致({self.result_size}).')

            for feature, status, time in cache.generator_cache():
                container.flist.append_item(feature)
                container.next()
                result.add_result(status, time)

                for fun in self._evaluation:
                    self._run_function(fun, dev, container, result)

                self._print_result(result)

    def clear(self):
        [cache.clear() for cache in self._cache.values()]


worker_class = NormalStackWorker
__enabled__ = True

