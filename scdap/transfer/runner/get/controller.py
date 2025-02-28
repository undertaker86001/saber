"""

@create on: 2020.12.11
"""


from ...base import BaseGetController


class RunnerGetController(BaseGetController):

    def transfer_mode(self) -> str:
        return 'simple'

    def run(self):
        pass

    def put_feature(self, algorithm_id: str, feature: dict):
        feature = {key.lower(): val for key, val in feature.items()}
        container = self._context.crimp.get_container(algorithm_id)
        feature = {
            self._decoder.kv.algorithm_id: algorithm_id,
            self._decoder.kv.node_id: self._context.algorithm_id2node_id(algorithm_id),
            self._decoder.kv.data: [feature]
        }
        self._decode(container, feature)
