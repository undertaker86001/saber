"""

@create on: 2021.05.18
"""
from scdap.summary import Summary, SummaryContext
from scdap.gop.func import register_parameter

from .designer import Designer


class SummaryDesigner(Designer):
    _context: SummaryContext
    _process: Summary

    @property
    def process_type(self):
        return 'summary'

    @staticmethod
    def process_class():
        return Summary

    @property
    def sub_workers(self):
        return self._context.worker.sub_workers()

    def sub_devices(self):
        return self._context.sub_devices

    def sub_nodes(self):
        return self._context.sub_nodes

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

        super().set_parameter(parameter, gnet, gloc, net_load_mode)

    def register_parameter(self, parameter: dict):
        """
        暂时缓存传入的参数至进程中
        缓存的参数拥有最高的读取优先度

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
        """

        def _set_parameter(tag, param, process_type):
            for fid_, p_ in param.items():
                register_parameter(tag, fid_, process_type, p_)

        _set_parameter(self.tag, parameter.get('summary', dict()), self.process_type)

        workers = self.sub_workers
        for aid, val in parameter.items():
            _set_parameter(aid, val, workers[aid].process_type())
