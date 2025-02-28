"""

@create on: 2021.04.25
"""
from typing import Type

from scdap import config
from scdap.gop import loc
from scdap.flag import option_key
from scdap.wp import WorkerProcess
from scdap.gop.check import check_option
from scdap.gop.func import get_summary_option, get_program_option

from .context import SummaryContext, Context


class Summary(WorkerProcess):
    """
    汇总算法进程
    目前有两个配置:
    device_option: 普通算法进程配置
    summary_option: 汇总算法进程配置
    当调用summary_option时, 汇总算法会根据配置的devices从device_option中获取各个联动点位的普通算法进程配置

    与普通的算法进程类似, 不同点在于其worker将包含其他囊括的需要汇总计算的子点位worker, 既:
    [get] -> ... -> [ worker ---> sub_worker1 ] -> ... -> [send]
                             ---> sub_worker2
                             ---> sub_worker3
                             ---> sub_worker4

    因为启动进程的是summary, workerprocess运用的是summary的配置
    所以子算法进程配置中只有与算法相关的配置有效(decision/evaluation/other/devices/worker/tag)
    子进程中的extra无效, 统一使用summary汇总进程配置中的extra
    {
        "tag": str,
        "devices": ["sub_tag1", "sub_tag2", "sub_tag3", ...],
        ...
        "extra": {
            ...
            worker: {
                "sub_option": {
                    "sub_tag1": {
                        "tag": str,
                        "worker": str,
                        "devices": [...],
                        "decision": [...],
                        "evaluation": [...],
                        "other": [...],
                        "extra": {...}, -> 不使用, 统一使用外部option的extra
                    },
                    "sub_tag2": {...},
                    "sub_tag3": {...},
                    ...
                }
            }
        }
    }

    """
    @property
    def process_type(self) -> str:
        return 'summary'

    def context_class(self) -> Type[Context]:
        return SummaryContext

    def __init__(self, tag: str, transfer_mode: str = None, option: dict = None,
                 debug: bool = None, load_loc: bool = None, load_net: bool = None,
                 load_net_mode: str = None, auto_set_parameter: bool = True):
        self.logger_info(f'初始化summary.')
        tag = str(tag)

        load_loc = config.LOAD_LOCAL_OPTION if load_loc is None else load_loc
        load_net = config.LOAD_NET_OPTION if load_net is None else load_net
        load_net_mode = config.LOAD_NET_OPTION_MODE if load_net_mode is None else load_net_mode
        debug = config.DEBUG if debug is None else debug

        self.logger_info(f'数据获取来源配置: '
                         f'net_mode: {load_net_mode}, '
                         f'from_loc: {load_loc}, '
                         f'from_net: {load_net}, '
                         f'from_reg: True.')

        if load_loc:
            loc.load_loc_program()
            loc.load_loc_summary()

        # 查询summary_option配置
        option = option or get_summary_option(tag, load_net, load_loc, load_net_mode)

        if option is None:
            raise self.wrap_exception(Exception, f'[{self.process_type}:{tag}]的进程配置获取失败.')

        try:
            check_option(option)
        except Exception as e:
            raise self.wrap_exception(type(e), f'[{self.process_type}:{tag}]的进程配置检查发现错误: {e}')

        # 根据summary_option配置的devices和tag查询device_option配置
        devices = list(map(str, sorted([tag] + option.get(option_key.devices, list()))))

        extra = self._get_option(option, option_key.extra, dict())
        worker_option_ori = self._get_option(extra, option_key.worker, dict())
        sub_option_ori = self._get_option(worker_option_ori, option_key.sub_option, dict())

        sub_option = dict()
        for dev in devices:
            sub_option[dev] = get_program_option(dev, load_net, load_loc, load_net_mode)

        # sub_option = dict(zip(devices, map(get_program_option, devices)))
        sub_option.update(sub_option_ori)

        # 点位编号, 包括所有sub_option的devices
        total_devices = set()
        for aid, sub in sub_option.items():
            if sub is None:
                raise self.wrap_exception(Exception, f'无法找到[program:{aid}]的进程配置.')

            try:
                check_option(sub)
            except Exception as e:
                raise self.wrap_exception(type(e), f'[program:{aid}]的进程配置检测错误: {e}')
            total_devices.add(aid)
            total_devices.update(sub[option_key.devices])

        if len(sub_option) <= 1:
            raise self.wrap_exception(ValueError, f'summary适合于多点位的汇总计算, 如果不需要配置多点位则不应使用summary.')

        # 将sub_option配置至extra.worker.sub_option
        # 在初始化worker的时候worker即可通过传入的参数option获取sub_option
        option.setdefault(option_key.extra, dict()) \
            .setdefault(option_key.worker, dict())[option_key.sub_option] = sub_option

        option[option_key.devices] = list(total_devices)

        super().__init__(tag, option, transfer_mode, debug, load_loc, load_net, load_net_mode)

        if auto_set_parameter:
            self.logger_info(f'配置算法参数.')
            self.set_parameter()

        self.print_message()
        self.logger_info(f'初始化summary完成.')
