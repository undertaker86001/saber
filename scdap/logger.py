"""

@create on: 2020.01.23

日志
"""
__all__ = ['Logger', 'logger', 'LoggerInterface']

import os
import sys
import atexit
import loguru

from functools import partial
from abc import abstractmethod, ABCMeta
from typing import Callable, Any, NoReturn, Type

from loguru._logger import Logger as _Logger
from loguru._logger import Core as _Core

from . import config


class Logger(object):
    DETAIL_FORMAT = "<red>[pid:{process}]</red> " \
                    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | " \
                    "<level>{level:<7}</level> | " \
                    "<level>{message}</level>"

    NORMAL_FORMAT = "<level>{message}</level>"

    def __init__(self):
        # 旧版本下loguru的创建方式与新版本所需参数不同
        if loguru.__version__ < '0.5.0':
            self._logger = _Logger(_Core(), None, -1, False, False, False, False, None, {})
        else:
            self._logger = _Logger(_Core(), None, -1, False, False, False, False, True, None, {})

        self._initialed = False

        atexit.register(self._logger.remove)

        self._logger.level('SECO', 15, color='<fg #7AFEC6>')

        # debug接口
        self.debug: Callable[[Any], NoReturn] = partial(self._logger.debug)
        # 介于debug与info的中间日志登记
        # 主要是在算法中使用
        self.seco: Callable[[Any], NoReturn] = partial(self._logger.log, "SECO")
        # 通用日志接口
        self.info: Callable[[Any], NoReturn] = partial(self._logger.info)
        # 警告接口
        self.warning: Callable[[Any], NoReturn] = partial(self._logger.warning)
        # 异常追踪接口
        self.exception: Callable[[Any], NoReturn] = partial(self._logger.exception)
        # 异常接口
        self.error: Callable[[Any], NoReturn] = partial(self._logger.error)
        self._stdout_id = None
        self.set_normal_stdout()

    def set_normal_stdout(self, level: str = None):
        self.set_stdout(level, self.NORMAL_FORMAT)

    def set_stdout(self, level: str = None, f: str = None):
        if self._stdout_id is not None:
            self._logger.remove(self._stdout_id)
        level = level or config.STDOUT_LEVEL
        level = level.upper()
        self._stdout_id = self._logger.add(
            sys.stdout,
            format=f or self.DETAIL_FORMAT,
            level=level
        )

    def initial(self, name, path,
                nsink=None, nrotation=None, nretention=None, ntrack=None, nlevel=None,
                esink=None, erotation=None, eretention=None, etrack=True, elevel=None,
                ):
        """
        初始化日志
        日志只允许初始化一次
        """
        if self._initialed:
            self.warning('日志已完成初始化.')
            return
        self._initialed = True

        log_path = os.path.join(path, str(name))
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        # 通常日志
        self._logger.add(
            os.path.join(log_path, nsink or config.DEFAULT_LOG_PARAM['nsink']),
            format="[pid:{process}] {time:YYYY-MM-DD HH:mm:ss} | {level: <7} | {message}",
            rotation=nrotation or config.DEFAULT_LOG_PARAM['nrotation'],
            retention=nretention or config.DEFAULT_LOG_PARAM['nretention'],
            backtrace=ntrack or config.DEFAULT_LOG_PARAM['ntrack'],
            level=nlevel or config.DEFAULT_LOG_PARAM['nlevel'],
            mode='a'
        )

        esink = esink or config.DEFAULT_LOG_PARAM['esink']
        # 异常日志
        if esink is not None:
            self._logger.add(
                os.path.join(log_path, esink),
                rotation=erotation or config.DEFAULT_LOG_PARAM['erotation'],
                retention=eretention or config.DEFAULT_LOG_PARAM['eretention'],
                backtrace=etrack or config.DEFAULT_LOG_PARAM['etrack'],
                level=elevel or config.DEFAULT_LOG_PARAM['elevel'],
                mode='a'
            )


class LoggerInterface(metaclass=ABCMeta):
    """
    日志接口类
    继承后需要配置log_name(), 配置后将格式化输出
    """

    @abstractmethod
    def interface_name(self):
        pass

    def logger_debug(self, message: str):
        logger.debug(f'[{self.interface_name()}] {message}')

    def logger_seco(self, message: str):
        logger.seco(f'[{self.interface_name()}] {message}')

    def logger_info(self, message: str):
        logger.info(f'[{self.interface_name()}] {message}')

    def logger_warning(self, message: str):
        logger.warning(f'[{self.interface_name()}] {message}')

    def logger_error(self, message: str):
        logger.error(f'[{self.interface_name()}] {message}')

    def logger_exception(self, e: Exception):
        logger.exception(e)

    def wrap_exception(self, exception: Type[Exception] = Exception, message: str = ''):
        """
        格式化抛出异常
        :param exception:
        :param message:
        :return:
        """
        return exception(f'[{self.interface_name()}]: {message}')


logger = Logger()
