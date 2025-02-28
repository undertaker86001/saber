"""

@create on: 2021.01.02
"""
from scdap import data


class RabbitMQResultListKV(data.ResultListKV):
    algorithm_id = 'algorithmId'
    node_id = 'nodeId'
    data = 'data'


class RabbitMQResultItemKV(data.ResultItemKV):
    time = 'dataTime'
    status = 'status'
    score = 'health'
    event = 'event'
    stat_item = 'statItem'


class RabbitMQEventKV(data.EventKV):
    algorithm_id = 'algorithmId'
    node_id = 'nodeId'
    etype = 'type'
    start = 'startTime'
    stop = 'endTime'
    time = 'alarmTime'
    name = 'name'
    code = 'code'
    check_result = 'checkResult'
    detail = 'detail'
    extend = 'extend'


class RabbitMQStatItemKV(data.StatItemKV):
    time = 'time'
    status = 'status'
    score = 'score'
    size = 'size'


class RabbitMQResultItemEncoder(data.ResultItemEncoder):
    def encode_score(self, obj: data.ResultItem):
        return dict(zip(self.encode_health_define(obj), super().encode_score(obj)))


class RabbitMQResultListEncoder(data.ResultListEncoder):
    def encode(self, obj: data.ResultList) -> data.TYPE_JSON:
        # {
        #     "nodeId": int,
        #     "algorithmId": str,
        #     "dataTime": timestamp,
        #     "status": int,
        #     "health": {
        #         "trend": score1,
        #         "stab": score2,
        #         ...
        #     },
        #     "event": [
        #       {
        #         "nodeId": 737                // 设备编号
        #         "algorithmId": "xxx"         // 算法使用的点位编号
        #         "type": 0,                   // 大类事件分类, 报警推送、周期性事件、加工周期、质检结果等
        #         "name": "trend",             // 事件标注, 根据type确认功能
        #         "startTime": 1609322658200,  // 事件起始时间
        #         "endTime": 1609322658200,    // 事件结束事件
        #         "alarmTime": 1609322658200   // 事件抛出时间
        #         "status": 0,                 // 事件抛出时的状态
        #         "score": {
        #           "trend": score1,           // 取当前的健康度, 无论什么情况都发送所有健康度数值
        #           "stab": scire2
        #         }
        #         "message": "...",
        #         "code": int,              // 具体的某一类事件编码, 时间编码将配置在事件列表中
        #         "checkResult": int,       // 结果大分类 | 0->正常 | 1->异常 | 2->无效,
        #         "detail": str             // 各种用途,
        #         "extend": {}              // 扩展字段, 未使用, 新增的需求字段可以存放在这里，这样子就不用改动很大
        #       }
        #   ]
        # }
        # event:
        # type = 0 -> 报警推送 -> name = 报警的健康度名称
        # type = 1 -> 周期性事件(健康度) -> name = 指定的周期性健康度名称
        # type = 2 -> 零件加工周期 -> name = 零件名称
        # type = 3 -> 质检结果 -> code = 质检详细结果编码 | checkResult -> 质检结果大类分类, detail -> 质检设备编号
        # type = 4 -> 某一类操作开始的标识符 start -> 必填
        # type = 5 -> 某一类操作结束的标识符 stop -> 必填
        # type = 6 -> 传递extend数据至前端以显示数据, name -> extend 必填
        # type = 7 -> 只是用来向界面的查看人员展示信息而已, message 必填
        node_id = self.encode_node_id(obj)
        algorithm_id = self.encode_algorithm_id(obj)
        result = list()
        kv = RabbitMQResultItemKV()
        for o in self.encode_data(obj):
            temp = {
                self.kv.node_id: node_id,
                self.kv.algorithm_id: algorithm_id,
                **o
            }

            # 部分参数不需要编码可以去除
            temp.pop(kv.health_define, None)
            if temp.get(kv.stat_item) is None:
                temp.pop(kv.stat_item, None)
            result.append(temp)

        return result


def get_result_list_encoder():
    event_encoder = data.EventEncoder(RabbitMQEventKV())
    si_encoder = data.StatItemEncoder(RabbitMQStatItemKV())
    ri_encoder = RabbitMQResultItemEncoder(RabbitMQResultItemKV(), event_encoder, si_encoder)
    return RabbitMQResultListEncoder(RabbitMQResultListKV(), ri_encoder)
