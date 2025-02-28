"""

@create on 2020.02.25

"""

from abc import ABCMeta

from .base import BaseFunction


class BaseEvaluation(BaseFunction, metaclass=ABCMeta):
    """
    评价算法
    单健康度
    允许多设备
    拥有默认参数:
    score_threshold: List[int] 健康度阈值
    score_recommendation: List[int] 健康度阈值区间
    """

    def is_realtime_function(self) -> bool:
        return True

    @staticmethod
    def get_function_type():
        return 'evaluation'

    @staticmethod
    def is_health_function() -> bool:
        return True

    def _check_self(self):
        super()._check_self()
        # 检查接口get_health_info()中threshold/recommendation的配置是否正确
        for hi in self.get_health_info():
            threshold = hi.get('threshold')
            if threshold is None or not isinstance(threshold, (list, tuple)):
                raise self.wrap_exception(TypeError, '实现get_health_info()时请确保threshold为list/tuple.')

            for t in threshold:
                if not isinstance(t, int):
                    raise self.wrap_exception(TypeError, '实现get_health_info()时请确保threshold内的元素类型为int.')
                if not 0 <= t <= 100:
                    raise self.wrap_exception(
                        ValueError, '实现get_health_info()时请确保threshold内的元素类型范围处在[0, 100].'
                    )

            recommendation = hi.get('recommendation')
            if recommendation is None or not isinstance(recommendation, (list, tuple)):
                raise self.wrap_exception(TypeError, '实现get_health_info()时请确保recommendation为list/tuple.')

            if len(recommendation) != len(threshold) + 1:
                raise self.wrap_exception(
                    ValueError, '实现get_health_info()时请确保recommendation配置的元素数量等于threshold的数量 + 1.'
                )

            for r in recommendation:
                if not isinstance(r, int):
                    raise self.wrap_exception(TypeError, '实现get_health_info()时请确保recommendation内的元素类型为int.')
