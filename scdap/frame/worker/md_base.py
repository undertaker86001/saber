"""

@create on: 2020.06.19
多设备联动工作组基类
"""
from abc import ABCMeta
from itertools import chain, compress
from datetime import datetime, timedelta
from typing import Tuple, List, Generator, Dict, Callable

from scdap.util.tc import DATETIME_MIN

from .base import BaseWorker, Container, Result

from ..function import BaseFunction


# 因为该方法在align中被频繁调用
# 故将其设置为一全局方法加快调用效率
# 该方法功能与np.hstack()相同, 但在align中效率更高
chain_iterable = chain.from_iterable

GENERATOR_DCR = Generator[Tuple[List[str], List[Container], List[Result]], None, None]


class MDBaseWorker(BaseWorker, metaclass=ABCMeta):
    """
    参数
    data_delta: float   橙盒数据间隔(单位秒)，默认1秒
    max_delay: int      数据同步最长延迟时间，当持续的有数据延迟导致数据累计的时候，超过该阈值将直接返回数据并且，延迟数据不在对齐
    """
    default_max_delay = 30
    default_data_delta = 1

    @staticmethod
    def process_type() -> str:
        return 'program'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 该类型算法工作组只有在多个设备情况下才适用
        # 否则直接报错
        if self.dsize == 1:
            raise self.wrap_exception(Exception, f'只允许在多设备进程配置.')

        # 数据同步最长延迟时间
        self._max_delay = self._option.get('max_delay', self.default_max_delay)
        # 橙盒数据间隔(单位秒)
        self._data_delta: int = self._option.get('data_delta', self.default_data_delta)
        self._data_delta = timedelta(seconds=self._data_delta)

        # [dev, prev_time, convert_index, first]
        # prev_time: 前一个数据的时间戳
        # convert_index: 前一次转换到的数据位置, 避免重复转换数据
        # first: 是否是第一次出现的时间戳，即没有发现重复时间戳
        #   在发现拥有重复时间戳1次数据后将重复数据的时间 + data_delta
        #   在发现拥有重复时间戳2次的数据后将抛弃第2次数据
        self._sync_data = [[dev, DATETIME_MIN, 0, True] for dev in self.devices]

        # 数据对齐时的最后一笔数据
        self._curr_time = DATETIME_MIN

        self._drive_data: Callable[[bool, bool], GENERATOR_DCR] = self._dcr_loop_generator_sync

    @classmethod
    def _check_function_type(cls, function):
        return issubclass(function, BaseFunction) and function.is_mdfunction()

    def reconvert(self) -> List[List[datetime]]:
        """
        将数据时间重置成以秒为最低单位
        todo: 未来可能会有更低间隔的数据，比如0.5s的数据等
        """
        data_delta = self._data_delta
        #
        # 因为在container.decode()阶段已经将重复数据或顺序错误的数据剔除，故不可能出现顺序错误与重复的数据
        # 当 prev == time 时则 time 设置成 time + timedelta(seconds=1)
        align_time = list()
        for sync_data in self._sync_data:
            dev, prev, index, first = sync_data
            cont = self._crimp.get_container(dev)
            size = cont.size()
            times = list(cont.flist.get_all_time())
            align_time.append(times)

            # 如果container没有新的数据或者数据都已经经过时间重置则不再重置时间
            if index == size:
                continue

            while index < size:
                # 设置时间戳中的microsecond=0, 最小单位为second
                time = times[index].replace(microsecond=0)
                # 数据正常
                if time > prev:
                    first = True
                # 数据重复但是只重复一次
                elif first and time == prev:
                    time = prev + data_delta
                    first = False
                    # print('same', cont.algorithm_id, times[index], '|', time, '|', prev, first)
                    # print(cont.algorithm_id, times[j], time, prev)
                # 数据时间小于前一个数据时间
                # 1.意味着这段数据可能是被抛弃了
                # 2.发现第二次重复数据
                # 故需要移除数据
                else:
                    cont.flist.pop(index)
                    size -= 1
                    continue
                prev = times[index] = time
                index += 1
                sync_data[:] = prev, size, first
        return align_time

    def align(self, times: List[List[datetime]]) -> Tuple[List[datetime], List[List[int]]]:
        """
        输入的数据格式：
        [[t1, t2, t3, ...], [t2, t3, t5, ...], [t1, t2, ...], ...]
               dev1               dev2             dev3
        对齐时间戳并返回设备是否拥有对应的时间戳的数组
        dev\time | t1 | t2 | t3 | t4 | ...
        -----------------------------------
          dev1   | 1  | 0  | 1  | 1  | ...
          dev2   | 1  | 0  | 1  | 1  | ...
          dev3   | 1  | 1  | 1  | 0  | ...
          dev4   | 1  | 0  | 1  | 1  | ...
          ...
        1 代表 dev 在时间戳t拥有数据
        0 代表 dev 在时间戳t不存在数据
        最终返回的结果是以上述数组保存的二维数组，不包含datetime与dev编号
        result = [[1, 1, ...], [1, 1, ...], [1, 0, ...], [1, 1, ...]]
                      t1           t2           t3           t4
        result内数组长度为设备数量
        """
        tsize = len(times)
        # 将所有设备数据时间整合成一段拥有所有设备数据时间戳的总时间戳
        # dev1: [t1, t2, t3, t5]
        # dev2: [t1, t2, t4]
        # dev3: [t0, t4, t6]
        # timeline: [t0, t1, t2, t3, t4, t5, t6]
        timeline = sorted(set(chain_iterable(times)))
        # 根据时间戳创建字典映射
        # t1: [0, 0, 0, ...]
        # t2: [0, 0, 0, ...]
        # t3: [0, 0, 0, ...]
        # t4: [0, 0, 0, ...]
        # t : ...
        result = [[0] * tsize for _ in range(len(timeline))]
        rdict: Dict[datetime, List[int]] = dict(zip(timeline, result))
        # 将设备拥有数据的时间戳位置填1
        for i in range(tsize):
            for j in range(len(times[i])):
                rdict[times[i][j]][i] = 1
        return timeline, result

    def _dcr_loop_generator_sync(self, add_result: bool = False) -> GENERATOR_DCR:
        """
        数据容器生成器
        多设备
        同步->根据多个设备数据时间进行数据同步处理
        dev1: [t1, t2, t3, t4]
        dev2: [t1, t2, t4]
        dev3: [t0, t4]
        timeline: [t0, t1, t2, t3, t4, t5, t6]
        每一次按顺序yield返回时间戳中某一刻拥有数据的设备容器
        t0 -> {dev0: data(t0)}
        t1 -> {dev1: data(t1), dev2: data(t1)}
        t2 -> {dev1: data(t2), dev2: data(t2)}
        t3 -> {dev1: data(t3)}
        t4 -> {dev1: data(t4), dev2: data(t4), dev3: data(t4)}
        """
        timeline, align_val = self.align(self.reconvert())
        size = self.dsize
        max_delay = self._max_delay
        ldcr = self._crimp.copy_ldcr()
        index = 0
        # 根据数据总体时间戳历遍所有数据
        # 在对齐时间戳算法下，只有两种情况会将数据输入至算法：
        # 1.当所有设备都在某一时间戳内拥有数据，即align返回的接口在某一刻时间戳下所有设备结果皆为1
        # 2.在连续一段时间戳长度(max_delay)中总有至少一个设备不存在数据，即align中一个至多个设备在连续的时间戳下结果为0
        for i in range(len(align_val)):
            delay = index + max_delay <= i
            if delay or sum(align_val[i]) == size:
                for index in range(index, i + 1):
                    # 挑出所有在当前时刻拥有数据的容器
                    # compress用于根据align_val[index] 获取数值为1也就是拥有数据的数据容器
                    # 与np.compress或切片效果相似
                    select = tuple(compress(ldcr, align_val[index]))
                    # 循环历遍调用数据
                    for dev, cont, res in select:
                        cont.next()
                        if add_result:
                            res.add_result(0, cont.flist.get_time())

                    yield zip(*select)

                index += 1
            # 更新sync_data中的prev_time，部分数据在数据被遗弃后仍可能在之后的某段时间内被接收到
            # 故需要更新prev_time，确保在reconvert()中将已经被遗弃的数据真正遗弃掉
            if delay:
                now = timeline[index - 1]
                for sync_data in self._sync_data:
                    if sync_data[0] < now:
                        sync_data[:] = now, index, True

    def _run_function(self, function: BaseFunction, device: List[str],
                      container: List[Container], result: List[Result]):
        """
        多设备算法运转
        """
        function.set_cr(
            {dev: self._api_creater.get_capi(function, cont) for dev, cont in zip(device, container)},
            {dev: self._api_creater.get_rapi(function, res) for dev, res in zip(device, result)}
        )
        function.compute()

    def disconnect(self, devices: List[str]):
        self.logger_info(f'{list(devices)} disconnect.')

        for fun in self._functions:
            # 使用不可变字典防止算法中修改字典导致错误
            fun.set_cr(
                {dev: None for dev in devices},
                {dev: self._api_creater.get_rapi(fun, self._crimp.get_result(dev)) for dev in devices}
            )
            fun.disconnect()

    def reset(self):
        super().reset()
        # 重置数据同步用变量
        self._sync_data = [[dev, DATETIME_MIN, 0, True] for dev in self.devices]
        self._curr_time = DATETIME_MIN

    def _print_result(self, result: List[Result] = None, position: int = None):
        if not self._show_compute_result:
            return

        for r in (result or self._crimp.generator_result()):
            for i in range(r.size()):
                super()._print_result(r, i)
