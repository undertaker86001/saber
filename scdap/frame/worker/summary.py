"""

@create on: 2021.05.10
"""
from typing import Dict, Tuple, List

from scdap.flag import option_key

from ..function import BaseFunction

from .base import BaseWorker, GENERATOR_DCR
from . import get_worker_class, get_worker_names


class _TempFunction(BaseFunction):

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_type() -> str:
        return 'summary'

    @staticmethod
    def get_function_name() -> str:
        return 'summary0'

    @staticmethod
    def get_information() -> dict:
        return {
            "version": "0.0.0",
            'author': 'null',
        }

    @staticmethod
    def is_health_function() -> bool:
        return True

    def get_health_define(self) -> List[str]:
        return []

    def compute(self):
        pass

    def set_parameter(self, parameter: dict):
        pass

    def reset(self):
        pass


class SummaryWorker(BaseWorker):
    """
    汇总算法工作组
    其将包含多个点位的子算法工作组
    等所有子算法工作组计算完毕吼

    在根据子算法工作组的结果进行汇总计算

    """
    result_api_kwargs = {
        'decision': {},
        'evaluation': {},
        'other': {
            'allow_add_result': False,
        },
    }

    def is_realtime_worker(self) -> bool:
        return False

    @staticmethod
    def get_worker_name() -> str:
        return 'summary'

    @staticmethod
    def process_type():
        return 'summary'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._sub_option: Dict[str, dict] = self._get_option(self._option, option_key.sub_option, dict())

        if len(self._sub_option) == 0:
            raise self.wrap_exception(Exception, f'请确保配置了足够多的点位来运行汇总算法工作组:{self.get_worker_name()}.')

        self._summary_function_column = set()

        self._sub_devices = tuple(self._sub_option.keys())

        self._sub_worker: Dict[str, BaseWorker] = dict()

        self._aid2worker: Dict[str, BaseWorker] = dict()

    def _initial_function(self):
        self._register_function(self._opt_other, self._other, 'summary')

        for f in self._other:
            self._summary_function_column.update(f.get_column())

        for sub_tag, sub_option in self._sub_option.items():

            sub_worker_class = sub_option[option_key.worker]
            # 校验与获取worker_class
            if isinstance(sub_worker_class, str):
                if sub_worker_class not in get_worker_names():
                    raise self.wrap_exception(
                        ValueError,
                        f'请配置正确的worker_class, 可用的worker_class包括: {get_worker_names()}'
                    )
                sub_worker_class = get_worker_class(sub_worker_class)

            elif not isinstance(sub_worker_class, BaseWorker):
                raise self.wrap_exception(Exception, f'请确保传入的worker_class继承自{BaseWorker}.')

            sub_tag = str(sub_tag)
            # 解析所有sub的所有点位id, 包括sub_option.devices
            devices = list(map(str, sub_option.get(option_key.devices, list()))) + [sub_tag]
            devices = tuple(set(devices))

            # 初始化
            sub_worker = sub_worker_class(
                sub_tag, devices,
                self._crimp.clone_sub(devices),
                sub_option.get(option_key.decision),
                sub_option.get(option_key.evaluation),
                sub_option.get(option_key.other),
                self.debug, self._show_compute_result,
                **sub_option.get(option_key.extra, dict()).get(option_key.worker) or dict()
            )
            sub_worker.initial()

            for d in devices:
                self._aid2worker[d] = sub_worker
            self._sub_worker[sub_tag] = sub_worker

    def _bind_function_to_device(self):
        for sub_worker in self._sub_worker.values():
            # 特征配置
            sub_column = sub_worker.get_column()
            for key, sub in sub_column.items():
                sub = set(sub)
                sub.update(self._summary_function_column)
                sub_column[key] = list(sub)
            self._column.update(sub_column)
            self._health_define.update(sub_worker.get_health_define())
            self._score_limit.update(sub_worker.get_score_limit())
            self._default_score.update(sub_worker.get_default_score())
            self._score_reverse.update(sub_worker.get_score_reverse())

    def bind_crimp(self):
        for sub_worker in self._sub_worker.values():
            sub_worker.bind_crimp()

        for aid, cont, res in self._crimp.generator_dcr():
            sub_worker = self._sub_worker[aid]
            global_parameter = {
                'column': sub_worker.get_column()[aid],
                'health_define': sub_worker.get_health_define()[aid],
                'default_score': sub_worker.get_default_score()[aid],
                'health_info': sub_worker.get_health_info()[aid]
            }

            # 创建一个虚假的算法, 让他配置所有的特征和健康度
            # 然后通过这个算法注册接口
            # 这样子就可以实现各个算法所需的接口都能够进行注册
            function = _TempFunction(aid, sub_worker.devices, 0, 0, self.debug,
                                     self._net_load_mode, global_parameter)
            function.initial()

            for _ in self._other:
                self._api_creater.register_capi(function, cont, self.container_api_kwargs['other'])
                self._api_creater.register_rapi(function, res, self.result_api_kwargs['other'])

    def clear(self):
        super().clear()
        for sub_worker in self._sub_worker.values():
            sub_worker.clear()

    def reset(self):
        super().reset()
        for sub_worker in self._sub_worker.values():
            sub_worker.reset()

    def flush(self):
        """
        summary_worker不需要刷新, 各个sub_worker会自行刷新

        """
        pass

    def sub_workers(self) -> Dict[str, BaseWorker]:
        return self._sub_worker

    def set_parameter(self, parameter: dict):
        """
        param = {
            "summary": {
                fid: dict(),
                fid: dict(),
            }

            "algorithm_id": {
                fid: dict(),
                fid: dict(),
            },
            "algorithm_id": {
                fid: dict(),
                fid: dict(),
            }
            ...
        }

        :param parameter:
        :return:
        """
        super().set_parameter(parameter.get('summary', dict()))
        for aid, sub_worker in self._sub_worker.items():
            sub_worker.set_parameter(parameter.get(aid, dict()))

    def sub_devices(self) -> Tuple[str]:
        """
        子点位主tag编号列表
        self.devices包括sub_option.devices

        """
        return self._sub_devices

    def _drive_data_by_result(self) -> GENERATOR_DCR:
        """
        依据result驱动数据

        :return:
        """
        index = 0
        ldcr = self._crimp.copy_ldcr()
        size = self.dsize
        # 多设备轮询进入算法工作组进行计算
        while 1:
            algorithm_id, container, result = ldcr[index]
            if not result.next():
                del ldcr[index]
                size -= 1

                if size == 0:
                    break

                index %= size
                continue

            index = (index + 1) % size

            container.next()
            yield algorithm_id, container, result

    def compute(self):
        # 各个子worker先行运算
        for worker in self._sub_worker.values():
            worker()

        # 重置指针重新循环一遍数据
        self._crimp.reset_position()

        # 运行summary
        for device, container, result in self._drive_data_by_result():
            for function in self._other:
                self._run_function(function, device, container, result)

    def print_message(self):
        self.logger_info('[main worker]:')
        self.logger_info('-' * 50)
        super().print_message()
        self.logger_info('-' * 50)
        self.logger_info(f'[sub_worker]:')
        for aid, worker in self._sub_worker.items():
            self.logger_info('-' * 50)
            self.logger_info(f'[sub_worker: {aid}]')
            worker.print_message()


worker_class = SummaryWorker
__enabled__ = True

