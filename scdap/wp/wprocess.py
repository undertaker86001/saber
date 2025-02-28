"""

@create on: 2020.07.03
"""
from copy import deepcopy
from time import time, sleep
from typing import List, Type, Union
from abc import ABCMeta, abstractmethod

from scdap import config
from scdap.api import email
from scdap.flag import option_key
from scdap.logger import LoggerInterface
from scdap.extendc import get_controller_class
from scdap.core.controller import BaseController
from scdap.frame.controller import WorkerController
from scdap.transfer.base import BaseGetController, BaseSendController

from .context import Context


class WorkerProcess(LoggerInterface, metaclass=ABCMeta):
    """
    算法工作组进程基类
    参数:
    ------------------------------------
    *tag: int               进程组编号
    *worker: str            算法工作组
    *devices: list[int]     联动算法点位编号
    *decision: list[dict]   识别算法
    *evaluation: list[dict] 评价算法
    *other: list[dict]      其他算法
    ------------------------------------
    clock_time: int         进程唤醒与计算的间隔时间
    extra: dict                 其他参数/controller/worker
                            extra: {
                                'c_switch': {
                                    'probe': true,
                                    'stat': true,
                                    'alarm': true,
                                    'secular': true,
                                }
                                'base_controller': {},
                                'transfer_mode': str,
                                'email': str,
                                'get': {...},
                                'worker': {...},
                                'send': {...},

                                'container': {...},
                                'result': {...},
                                ''
                                ...
                            }
    """

    def context_class(self) -> Type[Context]:
        return Context

    def interface_name(self):
        return 'wprocess'

    @property
    @abstractmethod
    def process_type(self) -> str:
        # 算法类型, 这里用于配置进程的类型，目前有两种，分别为summary/program
        pass

    def __init__(self, tag: str, option: dict, transfer_mode: str,
                 debug: bool, load_loc: bool, load_net: bool, load_net_mode: str):

        tag = str(tag)

        # 获取参数
        self._option = option

        if self._option is None:
            raise self.wrap_exception(Exception,
                                      f'无法找到[{self.process_type}:{tag}]的进程配置, 请按照如下顺序进行排查:'
                                      f'1).请检查是否已关闭vpn; '
                                      f'2).请确保scdap.config.load()读取了正确的配置; '
                                      f'3).如果url中包含ot/t等字段或者ip为192.168.x.x, 请确保当前电脑连接的网络处在公司内网中(注意实验室与办公室不处在同一网段); '
                                      f'4).请确保sqlapi服务的url正确, 部分配置使用了k8s中的服务发现url, 在非k8s内部环境下无法使用; '
                                      f'5).请确保已经在线上(develop/ot/master)中配置了该点位, 如果没有请上传; '
                                      f'6).联系管理员排查问题;')

        self._extra = self._get_option(self._option, option_key.extra, dict())

        # 算法定时执行时间
        # 进程是基于数据驱动的机制，所以需要获取数据
        # 但是获取数据应有一定间隔，以防止对数据请求过于频繁倒是数据提供方崩溃
        # 所以需要配置获取的睡眠间隔
        clock_time = self._get_option(self._option, option_key.clock_time, config.PROGRAM_CLOCK_TIME)
        if clock_time <= 0:
            clock_time = 2
            self.logger_warning(f'clock_time必须大于0, 默认将时间间隔修改为{clock_time}s.')

        # 测试用配置
        if debug and config.DEBUG_CLOCK_TIME is not None:
            self.logger_info(f'debug模式下修改clock_time为{config.DEBUG_CLOCK_TIME}s.')
            clock_time = config.DEBUG_CLOCK_TIME

        # 解析配置数据
        # 将配置与其中的算法参数缓存至本地
        # parse_option(self.process_type, self._option)

        transfer_mode = transfer_mode or self._get_option(self._extra, option_key.transfer_mode, config.TRANSFER_MODE)

        devices = self._option.get(option_key.devices, list())
        devices = list(map(str, devices))

        # sub_option: extra.worker.sub_option
        main_devices = list(self._extra.get(option_key.worker, dict()).get(option_key.sub_option, dict()).keys())

        self._context = self.context_class()(
            tag, self.process_type, devices, main_devices,
            clock_time, transfer_mode, debug, load_loc, load_net, load_net_mode
        )

        # 按顺序初始化各个组件
        # 1.crimp
        # 2.worker
        # 3.互相关联与绑定worker与crimp
        # 4.controller
        # ...

        # 创建与初始化容器
        self._context.initial_crimp(self._extra.get('container'), self._extra.get('result'))

        # 初始化算法工作组
        self._context.initial_worker(
            self._option.get(option_key.worker),
            self._option.get(option_key.decision),
            self._option.get(option_key.evaluation),
            self._option.get(option_key.other),
            config.SHOW_COMPUTE_RESULT,
            self._extra.get('worker'),
        )

        # 将worker与crimp互相关联在一起
        # 主要是互相获取各自需要的内容
        self._context.bind_worker_and_crimp()

        # email报警服务
        if config.OPEN_EMAIL_SERVE:
            self.logger_info('启动email推送服务.')
            email.open_serve()
            email_list = list(filter(lambda s: s, self._extra.get(option_key.email, '').split(',')))
            email.add_addr(email_list)

            self.logger_info(f'email推送服务推送对象: {email.get_addr()}.')

        # ------------------------------------------------------------------------------------------------
        # controller
        # ------------------------------------------------------------------------------------------------
        self._controllers: List[BaseController] = list()

        self.logger_info('-' * 50)
        self.logger_info('controller'.center(50))
        self.logger_info('-' * 50)

        BaseController.initial_base(self._extra.get('base_controller'))

        # 获取可配置的控制器开关选项
        self._controller_switch: dict = config.DEFAULT_CONTROLLER_SWITCH.copy()
        self._controller_switch.update(self._extra.get('c_switch', dict()))

        # 探针控制器, k8s需要使用
        self.probe_controller = self._create_controller('probe', default_switch=False)

        # 定时参数更新模块
        self.crontab_controller = self._create_controller('crontab', default_switch=False)

        # 数据接收控制器
        self.get_controller: BaseGetController = \
            self._create_controller('get', self._context.crimp.get_controller_class())

        # 算法工作组计算控制器
        self.compute_controller: WorkerController = self._create_controller('compute', WorkerController)

        # 统计模块
        self.stat_controller = self._create_controller('stat', default_switch=True)

        # 报警模块
        self.alarm_controller = self._create_controller('alarm', default_switch=False)

        # 数据结果发送控制器
        self.send_controller: BaseSendController = \
            self._create_controller('send', self._context.crimp.send_controller_class())

        self.logger_info('controller 初始化完毕.')
        self.logger_info('-' * 50)

        if self._context.debug and config.REGISTER_TO_DEBUG_SERVER:
            # debug模式下会至自用的测试后端登记点位信息让测试后端发送数据
            # 使得可以在无需java后端的情况下进行进程的运行测试
            self.debug_reigster()

    def _get_option(self, option: dict, key: str, default):
        val = option.get(key, None)
        if val is None:
            return default
        return val

    @property
    def context(self):
        return self._context

    @property
    def option(self) -> dict:
        return deepcopy(self._option)

    def call_controllers(self):
        for controller in self._controllers:
            controller()
        # 清理缓存数据
        self._context.crimp.clear()

    def reset(self):
        """
        重置算法工作组内所有算法
        """
        self._context.reset()
        for c in self._controllers:
            c.reset()

    def finish(self):
        for c in self._controllers:
            c.finish()

    def clear(self):
        self._context.clear()

    def _create_controller(self, controller_name: str = None,
                           controller_class: Type[BaseController] = None,
                           default_switch: bool = True) \
            -> Union[None, BaseController, WorkerController, BaseGetController, BaseSendController]:

        if not self._controller_switch.get(controller_name, default_switch):
            self.logger_warning(f'controller:{controller_name} 不启用.')
            return None

        option = self._get_option(self._extra, controller_name, dict())
        contoller_type = controller_class or get_controller_class(controller_name)
        controller = contoller_type(self._context, **option)
        controller.initial()
        controller.logger_info(f'初始化完成.')

        if controller.can_use():
            self._controllers.append(controller)
            self.logger_info(f'controller:{controller_name} 已注册至运行列表.')
        else:
            self.logger_warning(f'controller:{controller_name} 不适用当前环境或者配置, 不启用.')
            controller.close()

        return controller

    def print_message(self):

        self.logger_info('-' * 50)
        self.logger_info('version'.center(50))
        self.logger_info('-' * 50)

        from scdap import __version__
        self.logger_info(f'scdap: {__version__}')
        from scdap_algorithm import __version__
        self.logger_info(f'scdap_algorithm: {__version__}')

        self.logger_info('-' * 50)
        self.logger_info('worker_process'.center(50))
        self.logger_info('-' * 50)
        self.logger_info(f'get controller: {self.get_controller.transfer_mode()}')
        self.logger_info(f'send controller: {self.send_controller.transfer_mode()}')
        self.logger_info(f'controller: {[c.get_controller_name() for c in self._controllers]}')
        self.logger_info('-' * 50)

        self._context.print_message()

    def close(self):
        for controller in self._controllers:
            try:
                controller.close()
            except Exception as e:
                self.logger_error(f'调用[controller:{controller.get_controller_name()}]的close()触发错误: {e}')
        self.logger_info(f'服务调用close()关闭进程并且资源回收完毕.')

    def set_parameter(self, parameter: dict = None,
                      gnet: bool = None, gloc: bool = None, net_load_mode: str = None):
        """
        设置参数，算法将读取param中的参数
        传入参数表规则：
        param = {
            fid: dict(),
            fid: dict(),
            ...
        }

        :param parameter: 参数表，如果设置为None则将根据gnet与gloc的设置从对应位置获取阈值
        :param gnet: 是否从数据库中获取阈值
        :param gloc: 是否从本地获取阈值
        :param net_load_mode: 从网络中读取数据的模式 http/sql
        """
        self._context.set_parameter(parameter, gnet, gloc, net_load_mode)

    def serve_forever(self):
        self.logger_info('服务启动.')
        clock_time = self._context.clock_time
        need_sleep = self._context.crimp.need_sleep()
        while 1:
            wake_up_time = time() + clock_time
            self.call_controllers()
            # 运行超时将不进行睡眠直接进入下一次计算
            if need_sleep and clock_time > 0:
                sleep(max(wake_up_time - time(), 0))

    def debug_reigster(self):
        from scdap.flag import column
        from scdap.util.session import simple_post, parse_router
        param = dict()

        for aid, sub_column in self._context.worker.get_column().items():
            if column.hrtime in sub_column:
                sub_column.remove(column.hrtime)

            param[aid] = list(map(str, sub_column))

        try:
            resp = simple_post(
                url=parse_router(config.DEBUG_SERVER_URL, config.PROGRAM_DEBUG_REGISTER_ROUTER),
                json={
                    'dev': param,
                    'transfer': self._context.transfer_mode,
                    'nodes': self._context.nodes,
                    'devices': self._context.devices
                }
            )
        except Exception as e:
            self.logger_warning(f'debug_reigster()调用失败, 错误: {e}')
            return

        resp.close()
        self.logger_info(f'debug_reigster()调用成功.')
