"""

@create on 2020.02.19

控制器基类
"""
from abc import ABCMeta, abstractmethod

from scdap import config
from scdap.api import email
from scdap.logger import LoggerInterface

from scdap.util.delayer import Delayer


class BaseController(LoggerInterface, metaclass=ABCMeta):
    """
    控制器基类
    可选配置参数：
    max_num: int 延迟器最高延迟
    max_exception_count: int 允许捕获的最大异常数量, 异常超过该数量将抛出异常, 相当于结束进程
    exception_reset_delta: int 异常计数重置时间, 超过该时间将重置异常计数
    exception_counter_switch: bool 是否启动异常计数
    """
    default_delayer_delta = 5

    # 异常捕获相关的机制
    max_exception_count = config.CONTROLLER_MAX_EXCEPTION_COUNT
    exception_reset_delta = config.CONTROLLER_EXCEPTION_RESET_DELTA
    exception_counter_switch = config.CONTROLLER_EXCEPTION_COUNTER_SWITCH

    def __init__(self, context, **option):
        # 因为可能会造成递归调用
        # 所以在内部调用Context进行变量的备注
        from scdap.wp import Context
        self._context: Context = context
        self._option = option or dict()
        # 延迟器
        self._delay_max_num = self._get_option('delay_max_num', self.default_delayer_delta)
        self._delayer = Delayer(self._delay_max_num)

    def _get_option(self, key: str, default):
        val = self._option.get(key)
        if val is None:
            return default
        return val

    def can_use(self) -> bool:
        """
        该模块是否能够使用
        可能部分模块因为系统的原因无法使用或者其他什么

        :return:
        """
        return True

    def need_run(self) -> bool:
        return not self._delayer.need_delay()

    def __str__(self):
        return self.get_controller_name()

    def finish(self, *args, **kwargs):
        """
        结束整个控制器, 进行收尾工作
        一般是在测试的时候使用
        进程模式下不使用

        :return:
        """
        pass

    def reset(self):
        pass

    @staticmethod
    @abstractmethod
    def get_controller_name() -> str:
        """
        获取控制器名称

        :return: 名称
        """
        pass

    @property
    def name(self) -> str:
        """
        获取控制器名称

        :return: 名称
        """
        return self.get_controller_name()

    @abstractmethod
    def run(self):
        """
        通用的控制器运行方法
        """
        pass

    def stop(self) -> bool:
        """
        终止控制器

        :return:
        """
        return False

    def initial(self):
        """
        初始化方法
        需要在创建后进行初始化的操作在该处实现
        """
        pass

    def close(self):
        """
        关闭控制器方法
        需要在关闭控制器前进行清理的相关操作在该处实现
        """
        self.logger_warning('close controller.')

    def exception(self, exception: Exception):
        """
        当run()运行抛出异常的时候将调用该方法

        :param exception:
        """
        pass

    exception_count = 0
    exception_timeout = 0

    @staticmethod
    def initial_base(option: dict = None):
        """
        初始化全局控制器相关的变量

        :param option:
        """

        def _set_option(key: str, default):
            setattr(BaseController, key, option.get(key, default))

        option = option or dict()
        _set_option('max_exception_count', config.CONTROLLER_MAX_EXCEPTION_COUNT)
        _set_option('exception_reset_delta', config.CONTROLLER_EXCEPTION_RESET_DELTA)
        _set_option('exception_counter_switch', config.CONTROLLER_EXCEPTION_COUNTER_SWITCH)

    def on_exception(self, exception: Exception):
        self.logger_error('运行过程中发生错误.')
        # debug模式下直接抛出错误即可
        if self._context.debug:
            self.exception(exception)
            raise

        self.logger_exception(exception)
        try:
            self.exception(exception)
        except Exception as _exception:
            self.logger_error('调用exception()处理异常时发生错误.')
            self.logger_exception(_exception)

        email_kwargs = {
            "text": f'Process catch exception.\n'
                    f'Process: {self._context.algorithm_id}\n'
                    f'Catch exception at controller: {self.get_controller_name()}\n'
                    f'exception: {exception}\n',
            "to_addr": config.WARNING_EMAIL_ADDRESS,
            "topic": f'{config.COMMON_NAME} Controller捕获异常.'
        }

        # 是否启用异常计数
        if not BaseController.exception_counter_switch:
            extra_message = f'Controller异常计数器: [switch: off]'
            email_kwargs['text'] += extra_message
            self.logger_error(extra_message)
            email.send_email(**email_kwargs)
            return

        # 距离上一次异常捕获的时间过长
        # 直接重置异常计数
        current_timestamp = self._context.systimestamp_ms()

        # current_timestamp是毫秒时间戳
        # exception_timeout是秒级时间戳
        if current_timestamp >= BaseController.exception_timeout:
            BaseController.exception_count = 0

        BaseController.exception_timeout = current_timestamp + BaseController.exception_reset_delta * 1000
        # 累加异常计数
        BaseController.exception_count += 1
        # 如果异常计数超过阈值, 则直接抛出错误, 相当于终止进程
        extra_message = f'Controller异常计数器: [switch: on, ' \
                        f'count: {BaseController.exception_count}({BaseController.max_exception_count})]'
        email_kwargs['text'] += extra_message
        self.logger_error(extra_message)
        email.send_email(**email_kwargs)
        if BaseController.exception_count >= BaseController.max_exception_count:
            raise exception

    def __call__(self, *args, **kwargs):
        """
        请使用cotroller()

        :param args:
        :param kwargs:
        :return:
        """
        try:
            if self.need_run():
                self._context.crimp.reset_position()
                self.run()
        except Exception as exception:
            self.on_exception(exception)

    def interface_name(self):
        return f'controller:{self.get_controller_name()}'
