"""

@create on: 2021.01.06
"""

from typing import Dict, Tuple

from scdap.data import FeatureList, ResultList

from ...base import BaseSendController


class DesignerSendController(BaseSendController):
    def transfer_mode(self) -> str:
        return 'designer'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._columns = self._context.worker.get_column()
        self._cache: Dict[str, Tuple[FeatureList, ResultList]] = {
            aid: [FeatureList(aid, nid, column=self._columns[aid]), ResultList(aid, nid)]
            for aid, nid in zip(self._context.devices, self._context.nodes)
        }

    def get_cache(self) -> Dict[str, Tuple[FeatureList, ResultList]]:
        return self._cache

    def clear(self):
        self._cache = {
            aid: [FeatureList(aid, nid, column=self._columns[aid]), ResultList(aid, nid)]
            for aid, nid in zip(self._context.devices, self._context.nodes)
        }

    def reset(self):
        self.clear()

    def run(self):
        for dev, cont, res in self._context.crimp.generator_dcr():
            cache_cont, cache_res = self._cache[dev]
            if not res.empty():
                cache_res.extend_itemlist(res.rlist)
                cache_cont.extend_itemlist(cont.flist.sub_itemlist(0, res.size() - 1))
