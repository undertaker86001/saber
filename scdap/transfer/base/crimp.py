"""

@create on: 2021.01.04
"""
from abc import abstractmethod, ABCMeta
from typing import Type, List, Dict, Tuple, Generator, Iterable

from scdap.data import Container, Result

from .get.controller import BaseGetController
from .send.controller import BaseSendController

GENERATOR_C = Generator[Container, None, None]
GENERATOR_R = Generator[Result, None, None]

TYPE_CR = Tuple[Container, Result]
GENERATOR_CR = Generator[TYPE_CR, None, None]

TYPE_DCR = Tuple[str, Container, Result]
GENERATOR_DCR = Generator[TYPE_DCR, None, None]


class CRImplementer(metaclass=ABCMeta):
    """
    数据传输相关方式的类
    """

    def __init__(self):
        # 数据容器
        self._containers: List[Container] = list()
        self._results: List[Result] = list()
        self._lcr: List[TYPE_CR] = list()
        self._ldcr: List[TYPE_DCR] = list()
        # ------------------------------------------------
        # algorithm_id/algoritityId/tag
        # ------------------------------------------------
        self._dcontainers: Dict[str, Container] = dict()
        self._dresults: Dict[str, Result] = dict()
        self._dcr: Dict[str, TYPE_CR] = dict()
        # ------------------------------------------------
        # node_id
        # ------------------------------------------------
        self._dcontainers_by_node: Dict[int, Container] = dict()
        self._dresults_by_node: Dict[int, Result] = dict()
        self._dcr_by_node: Dict[int, TYPE_CR] = dict()

    def copy_ldcr(self) -> List[TYPE_DCR]:
        return self._ldcr.copy()

    def get_cr(self, algorithm_id: str) -> TYPE_CR:
        return self._dcr[algorithm_id]

    def get_cr_by_node(self, node_id: int) -> TYPE_CR:
        return self._dcr_by_node[node_id]

    def get_container(self, algorithm_id: str) -> Container:
        return self._dcontainers[algorithm_id]

    def get_container_by_node(self, node_id: int) -> Container:
        return self._dcontainers_by_node[node_id]

    def get_result(self, algorithm_id: str) -> Result:
        return self._dresults[algorithm_id]

    def get_result_by_node(self, node_id: int) -> Result:
        return self._dresults_by_node[node_id]

    def generator_container(self) -> GENERATOR_C:
        for c in self._containers:
            yield c

    def generator_result(self) -> GENERATOR_R:
        for r in self._results:
            yield r

    def generator_cr(self) -> GENERATOR_CR:
        for simple in self._lcr:
            yield simple

    def generator_dcr(self) -> GENERATOR_DCR:
        for simple in self._ldcr:
            yield simple

    def initial(self, context, container_option: dict = None, result_option: dict = None):
        """
        创建数据容器
        """
        for index, (aid, nid) in enumerate(zip(context.devices, context.nodes)):
            container = self.container_class()(aid, nid, index, context.systime, context.debug,
                                               **(container_option or dict()))

            result = self.result_class()(aid, nid, index, context.systime, context.debug,
                                         **(result_option or dict()))

            self._add_cr(container, result)

    def _add_cr(self, container, result):
        aid = container.get_algorithm_id()
        nid = container.get_node_id()

        self._containers.append(container)
        self._dcontainers[aid] = container
        self._dcontainers_by_node[nid] = container

        self._results.append(result)
        self._dresults[aid] = result
        self._dresults_by_node[nid] = result

        self._lcr.append((container, result))
        self._ldcr.append((aid, container, result))
        self._dcr[aid] = (container, result)
        self._dcr_by_node[nid] = (container, result)

    def clone_sub(self, algorithm_ids: Iterable[str]):
        """
        根据配置的algorithm_ids克隆cr容器

        :param algorithm_ids: 待克隆的点位容器列表
        :return: CRImplementer
        """
        sub_crimp = type(self)()

        for aid in algorithm_ids:
            container = self.get_container(aid)
            result = self.get_result(aid)
            sub_crimp._add_cr(container, result)

        return sub_crimp

    def reset_position(self):
        """
        重置数据容器的指针

        :return:
        """
        for c, r in self._lcr:
            c.reset_position()
            r.reset_position()

    def reset(self):
        self.clear()

    def clear(self):
        [(c.clear(), r.clear()) for c, r in self._lcr]

    def position_to_end(self):
        """
        将数据容器的内部指针拨至最后

        :return:
        """
        for c, r in self._lcr:
            c.flist.position_to_end()
            r.rlist.position_to_end()

    def clear_by_rsize(self):
        """
        根据result的长度来移除container的数据

        :return:
        """
        for c, r in self._lcr:
            if r.size():
                c.flist.remove_range(0, r.size())
            r.clear()

    def clear_container(self):
        for c in self._containers:
            c.clear()

    def clear_result(self):
        for r in self._results:
            r.clear()

    def container_class(self) -> Type[Container]:
        return Container

    def result_class(self) -> Type[Result]:
        return Result

    @abstractmethod
    def need_sleep(self) -> bool:
        pass

    @abstractmethod
    def get_controller_class(self) -> Type[BaseGetController]:
        pass

    @abstractmethod
    def send_controller_class(self) -> Type[BaseSendController]:
        pass
