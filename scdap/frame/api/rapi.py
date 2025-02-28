"""

@create on: 2021.01.19
"""
import warnings
from typing import List, Union
from datetime import datetime
from functools import partial

from scdap.data import Result
from scdap.flag import QualityInspectionItem


def _result_add_result(add_result, set_score, health_size, score_index, status: int, time: datetime, *score, **kwargs):
    add_result(status, time)
    _result_set_total_score(set_score, health_size, score_index, *score, **kwargs)


def _result_get_score(get_score, score_index, index: int = 0, *args, **kwargs):
    """
    为了实现 result.get_score() 等价于 result.get_score(0)的效果, 配置index的默认数值为0
    """
    return get_score(score_index[index], *args, **kwargs)


def _result_get_health_define(get_health_define, score_index, *args, **kwargs):
    health_define = get_health_define()
    return [health_define[i] for i in score_index]


def _result_set_score(set_score, score_index, index: int, score: int = None, *args, **kwargs):
    """
    为了实现 result.set_score(96) 等价于result.set_score(0, 95)的效果, 配置score的数值为None,
    当index拥有数值而socre为None触发该转换
    """
    if score is None:
        score = index
        index = 0
    set_score(score_index[index], score, *args, **kwargs)


def _result_get_prev_score(get_prev_score, score_index, index: int = 0, *args, **kwargs):
    """
    为了实现 result.get_prev_score() 等价于 result.get_prev_score(0)的效果, 配置index的默认数值为0
    """
    return get_prev_score(score_index[index], *args, **kwargs)


def _result_set_total_score(set_score, health_size, score_index, *score, **kwargs):
    if len(score) != health_size:
        raise Exception(f'传入的健康度数量有误, 健康度数量必须等于{health_size}')
    for i in range(health_size):
        set_score(score_index[i], score[i], **kwargs)


def _result_get_total_score(get_score, score_index, *args, **kwargs):
    return [get_score(i, *args, **kwargs) for i in score_index]


class ResultAPIEntity(object):
    def __init__(self, result: Result, function, *,
                 allow_status: bool = True, allow_time: bool = True, allow_score: bool = True,
                 allow_add_result: bool = True, allow_event: bool = True):
        from scdap.frame.function import BaseFunction
        function: BaseFunction
        # 因为__getattr__被禁用, 所以无法使用getattr方法缓存一些配置
        # 故在此声明一个变量用于解决这个情况
        self.option = dict()

        self.get_algorithm_id = result.get_algorithm_id
        self.get_device_id = result.get_algorithm_id
        self.get_node_id = result.get_node_id

        # 对于api内接口，设置任何结果数据的数值使用的index都是内置的index，不允许使用自定义的index
        # 故使用partial提前设置index
        if allow_status:
            self.get_status = partial(result.rlist.get_status, index=None)
            self.set_status = partial(result.rlist.set_status, index=None)

        if allow_time:
            self.get_time = partial(result.rlist.get_time, index=None)
            self.set_time = partial(result.rlist.set_time, index=None)

        if allow_score:
            self.get_health_define = partial(_result_get_health_define, result.get_health_define,
                                             function.__score_index__)
            self.get_score = partial(_result_get_score, result.get_score, function.__score_index__)
            self.set_score = partial(_result_set_score, result.set_score, function.__score_index__)
            self.set_score_force = partial(_result_set_score, result.set_score_force, function.__score_index__)

            self.get_prev_score = partial(_result_get_prev_score, result.get_prev_score, function.__score_index__)

            self.get_total_score = partial(
                _result_get_total_score,
                result.get_score,
                function.__score_index__
            )
            self.set_total_score = partial(
                _result_set_total_score,
                result.set_score,
                function.get_health_size(),
                function.__score_index__
            )

        if allow_event:
            # 事件接口
            # self.add_alarm_event = result.add_alarm_event
            # self.add_period_event = result.add_period_event
            # self.add_part_event = result.add_part_event
            # self.add_extend_event = result.add_extend_event
            # self.add_show_message_event = result.add_show_message_event
            # self.add_status_alarm_event = result.add_status_alarm_event
            # self.add_integrate_alarm_event = result.add_integrate_alarm_event
            # self.add_features_alarm_event = result.add_features_alarm_event
            self.add_quality_inspection = result.add_quality_inspection
            self.add_operation_start = result.add_operation_start
            self.add_operation_stop = result.add_operation_stop
            # 自动注册event接口
            # 格式必须为add_xxx_event
            # 如果非正规格式则需要手动添加
            for key, val in type(result).__dict__.items():
                if key.startswith('add') and key.endswith('event'):
                    setattr(self, key, partial(val, result))

        if allow_status and allow_add_result:
            if function.is_realtime_function():
                raise TypeError(f'算法: [{function.get_health_size()}]已经通过is_realtime_function()接口配置为实时算法,'
                                f'但配置的算法工作组中禁止该算法使用add_result()接口, 请检查算法类与算法工作组配置是否冲突.')
            # add_result接口
            if allow_score:
                self.add_result = partial(
                    _result_add_result,
                    result.add_result, result.set_score,
                    function.get_health_size(), function.__score_index__
                )
            else:
                self.add_result = result.add_result_without_score

    def __getattr__(self, item):
        raise NotImplementedError(
            f'{item}()是没有注册的接口, 可能是没有配置正确的算法工作组导致没有调用接口的权限'
            f'或者是在算法类里面调用了禁止调用的接口.'
        )

    def __setstate__(self, state):
        """
        pickle需要调用的接口
        如果不重载该方法的话pickle在调用时会进入到__getattr__中导致抛出pickle不可接收的错误最终报错
        """
        super().__setstate__(state)

    def __getstate__(self):
        """
        pickle需要调用的接口
        如果不重载该方法的话pickle在调用时会进入到__getattr__中导致抛出pickle不可接收的错误最终报错
        """
        return super().__getstate__()


class ResultAPI(object):
    """
    详情请查看 scdap.frame.api.capi.ContainerAPI的说明
    其他说明:
    为了达成指定评价算法的指定设备数据接口只配置各种拥有的健康度数据,
    通过 BaseFunction.__score_index__ / function.get_health_size() 以及对应的Result.xxx_score接扣再加上partial来实现
    """

    def __new__(cls, *args, **kwargs):
        return ResultAPIEntity(*args, **kwargs)

    def get_algorithm_id(self) -> int:
        """
        获得算法点位编号

        :return: 算法点位编号
        """
        pass

    def get_device_id(self) -> str:
        """
        获得算法点位编号, 旧版本接口, 不再使用, 请使用get_algorithm_id()获取编号

        :return: 算法点位编号
        """
        warnings.warn('device_id已弃用, 取代的是algorithm_id, 请使用get_algorithm_id()而不是get_device_id()',
                      DeprecationWarning)
        pass

    def get_node_id(self) -> int:
        """
        获得点位的后端编号

        :return: 点位的后端编号
        """
        pass

    def get_status(self) -> int:
        """
        获得状态

        :return:状态
        """
        pass

    def set_status(self, status: int):
        """
        设置状态

        :param status: 状态，请使用scdap.flag.dev_status进行设置
        """
        pass

    def get_time(self) -> datetime:
        """
        获得结果时间

        与container中的时间可能有不同，该时间默认为container中的时间

        可通过result接口自行设置

        :return: 时间
        """
        pass

    def set_time(self, time: datetime):
        """
        设置结果时间

        :param time: 结果时间
        """
        pass

    def get_health_define(self) -> List[str]:
        """
        获取健康度定义列表

        :return: 健康度列表
        """
        pass

    def get_score(self, score_index: int = 0) -> int:
        """
        获得健康度分数
        对于单算法多健康度，score_index的顺序为 Function.function_id的健康度score_index=0
        其余的健康度score_index按照Function.other_score_function_ids设置的顺序

        例如:

        function_id = 100 -> score_index = 0

        other_score_function_ids = (101, 102)

        function_id = 101 -> score_index = 1

        function_id = 102 -> score_index = 2

        ------------------------------------------

        如果get_score不传入任何参数则等价于获取算法类的第一个健康度数值

        等价关系: get_score() == get_score(0)

        ------------------------------------------

        :return: 健康度分数
        """
        pass

    def set_score(self, score_index: int, score: int = None):
        """
        设置健康度
        **范围必须为0-100**
        在默认情况下，当设置的健康度小于等于0时，则健康度默认延续之前计算出来的非0数值
        对于单算法多健康度，score_index的顺序为 Function.function_id的健康度score_index=0
        其余的健康度score_index按照Function.other_score_function_ids设置的顺序

        例如:

        function_id = 100 -> score_index = 0

        other_score_function_ids = (101, 102)

        function_id = 101 -> score_index = 1

        function_id = 102 -> score_index = 2

        ------------------------------------------

        如果set_score只传入一个参数则等价于设置算法类中第一个健康度的数值

        等价关系: set_score(98) == set_score(0, 98)

        ------------------------------------------

        :param score_index: 健康度位置
        :param score: 健康度
        """
        pass

    def set_score_force(self, score_index: int, score: int = 0):
        """
        强制设置健康度
        **范围必须为0-100**
        与set_score功能相似, 不同在于遇见0不会延续而是强制设置健康度为0
        对于单算法多健康度，score_index的顺序为 Function.function_id的健康度score_index=0
        其余的健康度score_index按照Function.other_score_function_ids设置的顺序

        例如:

        function_id = 100 -> score_index = 0

        other_score_function_ids = (101, 102)

        function_id = 101 -> score_index = 1

        function_id = 102 -> score_index = 2

        ------------------------------------------

        如果set_score只传入一个参数则等价于设置算法类中第一个健康度的数值

        等价关系: set_score(98) == set_score(0, 98)

        ------------------------------------------

        :param score_index: 健康度位置
        :param score: 健康度
        :param score_index:
        :param score:
        :return:
        """
        pass

    def get_prev_score(self, score_index: int = 0) -> int:
        """
        获取前一次的健康度数值

        ------------------------------------------

        如果get_prev_score只不传入任何参数则等价于获取算法类前一次的第一个健康度数值

        等价关系: get_prev_score() == get_prev_score(0)

        ------------------------------------------

        :param score_index: 健康度数值位置
        :return: 健康度数值
        """
        pass

    def set_total_score(self, *score):
        """
        设置所有健康度

        在默认情况下，当设置的健康度小于等于0时，则健康度默认延续之前计算出来的非0数值

        ------------------------------------------

        :param score: 多个健康度
        """

    def get_total_score(self) -> List[int]:
        """
        获取所有健康度

        ------------------------------------------

        :return: 健康度列表
        """
        pass

    def add_result(self, status: int, time: datetime, *score):
        """
        添加结果，结果包含状态、时间、健康度，该方法一般于worker中使用，不可重复在一次计算中使用

        一般的算法使用set_xxx()的方法即可

        在默认情况下，当设置的健康度小于等于0时，则健康度默认延续之前计算出来的非0数值

        在识别算法下，add_result中的参数score不可使用

        :param status: 状态
        :param time: 时间
        :param score: 健康度，默认为前一次计算的健康度，识别算法使用该参数无效
        """
        pass

    def add_alarm_event(self, *, health_define: str, start: datetime = None,
                        stop: datetime = None, message: str = ''):
        """
        添加报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 0
        name: 指定的需要报警的健康度名称
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        message: 备注
        报警事件将通过配置的name中保存的health_define, 从score中获取所需的数值
        如: name = ["trend"], score = {"trend": 99, "error": 1}

        则其会获取trend的健康度作为报警事件入库

        :param health_define: 报警事件所代表的健康度名称(health_define), **必填**
        :param start: 事件起始事件, 可以不存在
        :param stop: 事件结束时间, 可以不存在
        :param message: 事件信息
        """
        pass

    def add_period_event(self, *, health_define: Union[str, List[str]],
                         start: datetime, stop: datetime, message: str = ''):
        """
        添加周期性事件
        使用到的全部参数(指传输给后端会使用的字段):
        type: 1
        name: 指定的周期性健康度名称, 如果该周期存在多共个周期性健康度分数, 则使用(,)隔开
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        message: 备注
        周期性事件将通过配置的name中保存的health_define, 从score中获取所需的数值
        如: name = ["trend"], score = {"trend": 99, "error": 1}
        则其会获取trend的健康度作为周期性事件入库

        :param health_define: 报警事件所代表的健康度名称(health_define), 如果周期内存在多个分数, 则传入一个列表 **必填**
        :param start: 事件起始事件, **必填**
        :param stop: 事件结束时间, **必填**
        :param message: 事件信息
        """
        pass

    def add_part_event(self, *, name: str, start: datetime, stop: datetime, message: str = ''):
        """
        添加加工周期事件(或者是一次质检结果(OK/NG))
        使用到的全部参数(指传输给后端会使用的字段):
        type: 2
        name: 加工零件的名称
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        message: 备注

        :param name: 加工零件名称, **必填**
        :param start: 事件起始事件, **必填**
        :param stop: 事件结束时间, **必填**
        :param message: 事件信息
        """
        pass

    def add_quality_inspection(self, *, qi: QualityInspectionItem, detail: str, start: datetime,
                               stop: datetime, message: str = ''):
        """
        添加质检结果
        使用到的全部参数(指传输给后端会使用的字段):
        type: 3
        name: 质检结果中文名称, 按道理不应该传，但是后端处理起来方便一些，所以就传了
        code: 具体的质检事件类型分类编号，具体可以查看 scdap.flag.QualityInspectionItem
        check_result: 之间类型的大类分类编号，具体可以查看 scdap.flag.QualityInspectionItem
        start: 事件起始时间
        stop: 事件结束时间
        score: 所有健康度数值的kv映射: {"trend": 99, "error": 1}
        detail: 质检设备型号
        message: 备注，可以在前端显示一条信息展示出来

        :param detail: 质检设备型号
        :param qi: 质检结果类型, **必填**
        :param start: 质检事件起始时间, **必填**
        :param stop: 质检事件结束事件, **必填**
        :param message: 其他信息，可以在前端显示一条信息展示出来
        :return:
        """

    def add_operation_start(self, *, start: datetime, message: str = ''):
        """
        抛出一次操作的起始时间事件
        主要是用于通知前后端事件的开始
        方便前后端进行相关数据的显示

        使用到的全部参数(指传输给后端会使用的字段):
        type: 4
        start: 事件起始时间
        message: 备注

        :param start: 操作起始时间, **必填**
        :param message: 其他信息
        :return:
        """

    def add_operation_stop(self, *, stop: datetime, message: str = ''):
        """
        抛出一次操作的结束时间事件
        主要是用于通知前后端事件的开始
        方便前后端进行相关数据的显示

        使用到的全部参数(指传输给后端会使用的字段):
        type: 5
        stop: 事件结束时间
        message: 备注

        :param stop: 操作结束时间, **必填**
        :param message: 其他信息
        :return:
        """
        pass

    def add_extend_event(self, *, extend: str, message: str = ''):
        """
        以事件的形式传输传感器数据至后端

        使用到的全部参数(指传输给后端会使用的字段):
        type: 6
        name: 需要到界面显示的某一些信息, 主要是在质检中使用
        message: 备注

        :param extend: 传感器数据, **必填**
        :param message: 其他信息
        :return:
        """
        pass

    def add_show_message_event(self, *, message: str):
        """
        展示某些算法给出的提示信息
        没有任何实际效果
        只是用来向界面的查看人员展示信息而已

        使用到的全部参数(指传输给后端会使用的字段):
        type: 7
        message: 需要到界面显示的某一些信息, 会以弹窗的形式显示出来

        :param message: 需要到界面显示的某一些信息, 会以弹窗的形式显示出来
        :return:
        """
        pass

    def add_status_alarm_event(self, *, alarm_status: str, start: datetime = None, message: str = ''):
        """
        添加状态报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 8
        name: 指定的需要报警的状态名称
        start: 事件起始时间
        message: 备注

        :param alarm_status: 报警事件所代表的状态名称, **必填**
        :param start: 事件起始时间, 可以不存在
        :param message: 事件信息
        """
        pass

    def add_integrate_alarm_event(self, *, start: datetime = None, stop: datetime = None, message: str = ''):
        """
        添加综合报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 9
        start: 事件起始时间
        message: 备注

        :param start: 事件起始时间, 可以不存在
        :param stop: 事件结束时间，可以不存在
        :param message: 事件信息
        """
        pass

    def add_features_alarm_event(self, *, start: datetime = None, stop: datetime = None, message: str = ''):
        """
        添加特征值报警记录，该记录将在添加后由产品界面进行报警
        使用到的全部参数(指传输给后端会使用的字段):
        type: 10
        start: 事件起始时间
        message: 备注

        :param start: 事件起始时间, 可以不存在
        :param stop: 事件结束时间，可以不存在
        :param message: 事件信息
        """
        pass