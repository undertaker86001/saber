"""

@create on: 2020.12.25
"""
import smtplib

from typing import List, Union, Optional

from email.utils import formataddr
from email.mime.text import MIMEText

from concurrent.futures import ThreadPoolExecutor

from scdap import config
from scdap.logger import LoggerInterface


class _EmailConnector(LoggerInterface):

    def interface_name(self):
        return 'email-connector'

    def __init__(self):
        if config.EMAIL_SMTP_CONNECT_TYPE == 'tls':
            self.connect = self._connect_tls
        elif config.EMAIL_SMTP_CONNECT_TYPE == 'ssl':
            self.connect = self._connect_ssl
        else:
            raise Exception('email服务链接类型必须为ssl/tls.')

        self._server: Union[None, smtplib.SMTP_SSL, smtplib.SMTP] = None

    def close(self):
        if self._server:
            try:
                self._server.close()
            except Exception as e:
                self.logger_error('链接关闭失败')
                self.logger_exception(e)

    def is_connected(self) -> bool:
        """

        :return: 通过ehlo方法判断服务器是否处在链接当中
        """
        if self._server is None:
            return False
        try:
            self._server.ehlo()
        except:
            return False
        return True

    def _connect_ssl(self):
        if self.is_connected():
            return True
        try:
            self._server = smtplib.SMTP_SSL(config.EMAIL_SMTP_HOST, config.EMAIL_SMTP_PORT)
            self._server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
        except Exception as e:
            self.logger_error('ssl类型链接建立失败.')
            self.logger_exception(e)
            return False
        return self.is_connected()

    def _connect_tls(self):
        try:
            self._server = smtplib.SMTP(config.EMAIL_SMTP_HOST, config.EMAIL_SMTP_PORT)
            self._server.starttls()
            self._server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
        except Exception as e:
            self.logger_error('smtp类型链接建立失败.')
            self.logger_exception(e)
            return False
        return self.is_connected()

    def send_email(self, *args, **kwargs) -> bool:
        return self._server.sendmail(*args, **kwargs)


class Email(LoggerInterface):
    def interface_name(self):
        return 'email'

    def __init__(self):
        self._email_server: Optional[_EmailConnector] = None
        self._from_addr = None
        self._to_addr = list()
        self._is_opened = False
        self._pool = ThreadPoolExecutor(1)
        self._count = 0

    def add_addr(self, addr: Union[str, List[str]]):
        """
        设置默认的email发送对象

        :param addr: 设置默认的发送email对象
        """
        if isinstance(addr, str):
            self._to_addr.append(addr)
        else:
            self._to_addr.extend(addr)

    def open_serve(self):
        self._is_opened = True

    def get_addr(self) -> List[str]:
        return self._to_addr.copy()

    def connecet(self) -> bool:
        """
        连接服务
        如果之前的连接失败则重新连接
        """
        if self._email_server is None:
            self._email_server = _EmailConnector()
            return self._email_server.connect()

        if not self._email_server.is_connected():
            self.logger_warning('email服务链接已断开, 准备重新建立链接.')
            self._email_server.close()
            self._email_server = _EmailConnector()
            return self._email_server.connect()

        return True

    def _pool_do_send_email(self, email_id, from_addr, to_addr, message):
        if not self.connecet():
            self.logger_error('email服务链接已断开, 无法发送邮件.')
            return
        try:
            self._email_server.send_email(from_addr, to_addr, message.as_string())
            self.logger_info(f'email id: {email_id} 发送成功.')
        except Exception as e:
            self.logger_error(f'email id: {email_id} 线程发送邮件失败.')
            self.logger_exception(e)
        import random
        if random.randint(0, 3) == 0:
            self._email_server.close()

    def send_email(self, text, to_addr: Union[str, List[str]] = None, topic='Sucheon-dap Alarm'):
        """
        发送报警信息

        :param text: 报警内容
        :param to_addr: 目标email对象, 可以是一个包含多个email地址的列表或者是以(,)隔开的包含多个email地址字符串
        :param topic: 发送的标题
        """
        if not self._is_opened:
            self.logger_warning('email服务没有被启用, 无法发送任何邮件.')
            return
        text = f'System: {config.COMMON_NAME}\n{text}'

        message = MIMEText(text, _charset='utf-8')
        if to_addr is None:
            to_addr = self._to_addr
        if not to_addr:
            self.logger_warning('必须至少配置一个授信人地址.')
            return

        if isinstance(to_addr, (tuple, list)):
            to_addr = ','.join(to_addr)

        message['to'] = to_addr
        message['from'] = formataddr((config.COMMON_NAME, config.EMAIL_ADDRESS))
        message['subject'] = topic
        email_id = self._count
        self._count += 1
        self.logger_info(f'id: {email_id}, to:{to_addr}, topic: {topic}, text:{text.encode()}')
        self._pool.submit(self._pool_do_send_email, email_id, config.EMAIL_ADDRESS, to_addr, message)

    def join(self):
        return self._pool.shutdown()


_email = Email()
