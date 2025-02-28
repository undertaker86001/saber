"""

@create on 2020.02.19
算法进程
"""
from scdap import config
from scdap.gop import loc
from scdap.wp import WorkerProcess
from scdap.util.tc import dict_to_str
from scdap.gop.check import check_option
from scdap.gop.func import get_program_option
from scdap.util.session import parse_router, simple_post


class Program(WorkerProcess):
    """
    参数:
    ---------------------------
    其他部分请查看scdap.wp.worker_process
    ---------------------------
    """

    @property
    def process_type(self) -> str:
        return 'program'

    def __init__(self, tag: str, transfer_mode: str = None, option: dict = None,
                 debug: bool = None, load_loc: bool = None, load_net: bool = None,
                 load_net_mode: str = None, auto_set_parameter: bool = True):

        self.logger_info(f'初始化{self.process_type}.')
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

        option = option or get_program_option(tag, load_net, load_loc, load_net_mode)

        if option is None:
            raise self.wrap_exception(Exception, f'[{self.process_type}:{tag}]的进程配置获取失败.')

        try:
            check_option(option)
        except Exception as e:
            raise self.wrap_exception(type(e), f'{self.process_type}:{tag}]的进程配置检查发现错误: {e}')

        super().__init__(tag, option, transfer_mode, debug, load_loc, load_net, load_net_mode)

        if auto_set_parameter:
            self.logger_info(f'配置算法参数.')
            self.set_parameter()

        self.print_message()
        self.logger_info(f'初始化{self.process_type}完成.')
