"""

@create on 2020.02.24

事件编号
"""
from typing import Optional

from .define_base import DefineBase


class EventType(DefineBase):
    """
    事件添加步骤:
    1. 在此处添加一个新的type_id与对应的事件名称xxx, 格式为xxx = type_id
    2. 通知后端新增的type_id的事件内容, 并且通知其需要使用到的事件字段以及字段的含义
    3. 在scdap.data.result.Result中新增一个接口命名为 def add_xxxx_event(self, *, ...), 必须配置(*)
        **注意后缀必须带有event, 为的是方便自动化添加事件接口**
        class Result(...):
            ...
            def add_xxxx_event(self, *, message: str = '', ...):
                # ...
                # ...
                # 按需要配置相应的校验代码
                self._add_event(message=message, ...)
            # ...
    4. 在scdap.frame.api.rapi.ResultAPI中新增与3新增的接口相同的接口, 并且添加详细的参数与接口描述, 无需实现接口内容, 直接配置为pass
        该位置的接口主要用于IDE接口补全, 无实际用处
        class ResultAPI(...):
            ...
            def add_xxxx_event(self, *, message: str = '', ...):
                # ...
                pass
            # ...
    5. 这一步如果名字配置正确则一般不需要配置, 不过还是说明一下
        在scdap.frame.api.rapi.ResultAPIEntity中
        class ResultAPIEntity():
            def __init__(...):
                # ...
                if allow_event:
                    # ...
                    这里的内容就是接口注册的位置
                    已经配置自动解析的功能了, 所以如果名字格式正确则将自动注册
    至此, 事件添加完毕

    """
    __item_type__ = int
    # 异常报警
    alarm = 0
    # 周期事件
    period = 1
    # 零件加工
    part = 2
    # 质检
    quality_inspection = 3
    # 操作开始(可能是一次质检事件的开始, 也可以是一次周期事件的开始)
    operation_start = 4
    # 操作结束(可能是一次质检事件的结束, 也可以是一次周期事件的结束)
    operation_stop = 5
    # 来源于橙盒传感器的数据有时候需要传输到前端显示
    # 所以特此增加一个事件专门用于传感器数据的传输
    extend_event = 6
    # 展示某些算法给出的提示信息
    # 没有任何实际效果
    # 只是用来向界面的查看人员展示信息而已
    show_message_event = 7
    # 状态报警
    status_alarm = 8
    # 综合（整机）报警
    integrate_alarm = 9
    # 特征值报警事件
    features_alarm = 10


event_type = EventType()


class QualityInspectionItem(object):
    """
    质检结果类型
    check_result
    """
    _type = 3

    def __init__(self, code: int, check_result: int):
        if check_result not in {0, 1, 2}:
            raise ValueError(f'check_result参数传入的数值错误, 数值必须为[0, 1, 2].')
        self._check_result = check_result
        self._code = code
        self._cn_name: Optional[str] = None

    @property
    def check_result(self) -> int:
        return self._check_result

    @property
    def code(self) -> int:
        return self._code

    @property
    def type(self) -> int:
        return self._type

    @property
    def cn_name(self) -> str:
        if self._cn_name:
            return self._cn_name
        from scdap import config
        from scdap.api import event_define
        from scdap.logger import logger
        try:
            result = event_define.get_event_define(self._type, self._code, config.LOAD_NET_OPTION_MODE)
        except Exception as e:
            logger.error(f'质检结果类型[type:{self._type} code:{self._code}]获取失败, 暂时解析为unknown.')
            logger.exception(e)
            result = {'cn_name': 'unknown'}
        self._cn_name = result['cn_name']
        return self._cn_name
