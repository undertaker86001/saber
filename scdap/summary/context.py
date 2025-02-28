"""

@create on: 2021.05.07
"""
from typing import Optional

from scdap.wp import Context
from scdap.util.parser import parser_id
from scdap.frame.worker.summary import SummaryWorker
from scdap.gop.func import list_program_parameter, register_parameter, get_summary_parameter


class SummaryContext(Context):
    worker: Optional[SummaryWorker]

    @property
    def process_type(self) -> str:
        return 'summary'

    def set_parameter(self, parameter: dict = None, gnet: bool = None, gloc: bool = None, net_load_mode: str = None):
        """
        设置参数，算法将读取param中的参数
        传入参数表规则：
        param = {
            "summary": {
                fid: dict(),
                fid: dict(),
            },

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

        :param parameter: 参数表，如果设置为None则将根据gnet与gloc的设置从对应位置获取阈值
        :param gnet: 是否从数据库中获取阈值
        :param gloc: 是否从本地获取阈值
        :param net_load_mode: 从网络中读取数据的模式 http/sql
        """
        if gnet is None:
            gnet = self.load_net
        if gloc is None:
            gloc = self.load_loc
        if net_load_mode is None:
            net_load_mode = self.net_load_mode

        parameter = parameter or dict()

        temp = {
            'summary': self._load_parameter(parameter.get('summary', dict()), self.worker, gnet, gloc, net_load_mode)
        }

        for sub_worker in self.worker.sub_workers().values():
            temp[sub_worker.tag] = self._load_parameter(parameter.get(sub_worker.tag, dict()),
                                                        sub_worker, gnet, gloc, net_load_mode)
        self.worker.set_parameter(temp)
