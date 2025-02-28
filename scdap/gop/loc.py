"""

@create on: 2020.01.23

**注意**
在配置算法工作组的时候，必须指定一个主设备作为算法工作组的标签（算法点位编号）
算法工作组在创建以及删除的时候都以主设备的算法点位编号为依据进行操作

本地分别保存设备配置以及算法参数
都暂存于lworkers中
为了读取方便，设置变量lparameter用于查询对应算法工作组中对应算法的算法参数

本地算法工作组配置结构：
options = {
    # 取一个算法点位编号作为算法工作组的标签
    tag: {
        'tag': tag,
        'devices': [tag, ...],
        'process_type': 'program/summary/...',
        'worker': 'worker_name',
        # 算法工作组获取数据的间隔时间
        'clock_time': int(default: 2),
        'decision': [
            {
                'function': 'decision_name',
                'global_parameter': {},
                ...
                other...
                ...
            },
            ...
        ],
        'evaluation': [
            {
                'function': 'evaluation_name',
                'global_parameter': {},
                'loc_parameter': '...',     // 本地参数路径
                ...
                other...
                ...
            },
            ...
        ],
        'other': [
            {
                'function': 'other_name',
                'global_parameter': {},
                ...
                other...
                ...
            },
        ],
        "extra": {
            "eparam1": ...,
            "eparam2": ...,
        },
        "description": "..."
    }
}

本地算法工作组参数结构：
parameters = {
    [process_type]-[worker_tag]-[function_id]: {
        ...
        other
        ...
    },
    ...
}

"""
import os

from copy import deepcopy
from typing import Tuple, Dict, Optional

from scdap import config
from scdap.logger import logger
from scdap.flag import option_key
from scdap.util.parser import parser_id
from scdap.frame.function import BaseFunction
from scdap.util.tc import load_json, load_pickle
from scdap.core.process_type import check_process_type

__all__ = [
    'parse_option',
    'parse_parameter',

    'get_parameter',
    'get_option',
    'get_program_option',
    'get_summary_option',
    'get_summary_parameter',
    'get_program_parameter',

    'load_loc_program',
    'load_loc_summary',
    'clear_loc',

    'parse_program_option',
    'parse_program_parameter',
    'parse_summary_option',
    'parse_summary_parameter',

    'join_process_name',
    'reparse_parameter_tag',
    'repase_process_name'
]

# 本地设备配置
options = dict()

# 本地设备配置算法参数
parameters = dict()


def join_process_name(tag: str, process_type: str) -> str:
    """
    获取进程名称，进程名称可能由两部分组成：
    进程类型-标签名
    [process_type]-[tag]

    :param tag: 既设备组编号
    :param process_type: 进程类型
    :return: 进程组合标识名称
    """
    return f'{process_type}-{tag}'


def repase_process_name(pname: str) -> Tuple[str, str]:
    """
    反向解析, join_process_name()的反操作

    :param pname:
    :return:
    """
    process_type, tag = pname.split('-')[:2]
    return process_type, tag


def get_parameter_tag(tag: str, function_id: int, process_type: str) -> str:
    """
    根据 tag, function_id, process_type合并获取保存于loc的参数标签，该标签可用于获取参数的索引

    :param tag: 设备标签
    :param function_id: 算法编号
    :param process_type: 进程类型
    :return: 保存于loc的参数标签
    """
    return f"{join_process_name(tag, process_type)}-{function_id}"


def reparse_parameter_tag(ptag: str) -> Tuple[str, str, int]:
    """
    反向解析, get_parameter_tag的反操作

    :param ptag:
    :return:
    """
    process_type, tag = repase_process_name(ptag)
    fid = int(ptag.split('-')[2])
    return process_type, tag, fid


def get_parameter(tag: str, function_id: int, process_type: str) -> dict:
    """
    根据 tag, function_id, process_type获取保存于loc的参数

    :param tag: 设备标签
    :param function_id: 算法编号
    :param process_type: 进程类型
    :return: 参数
    """
    return parameters.get(get_parameter_tag(tag, function_id, process_type))


def get_program_parameter(tag: str, function_id: int) -> dict:
    return get_parameter(tag, function_id, 'program')


def get_summary_parameter(tag: str, function_id: int) -> dict:
    return get_parameter(tag, function_id, 'summary')


def get_option(tag: str, process_type: str) -> dict:
    """
    根据 tag, process_type获取保存于loc的设备配置

    :param tag: 设备标签
    :param process_type: 进程类型
    :return: 配置
    """
    return options.get(join_process_name(tag, process_type))


def get_program_option(tag: str) -> dict:
    return get_option(tag, 'program')


def get_summary_option(tag: str) -> dict:
    return get_option(tag, 'summary')


def add_option(tag: str, process_type: str, option: dict):
    """
    添加设备配置至loc本地

    :param tag: 设备标签
    :param option: 待保存配置
    :param process_type: 进程类型
    """
    name = join_process_name(tag, process_type)
    if name in options:
        logger.warning(f'[option:{tag}:{process_type}]'
                       f'在本地缓存中存在相同的配置, 该配置将被覆盖.')
    options[name] = option


def add_parameter(tag: str, function_id: int, process_type: str, param: dict):
    """
    添加算法参数至loc本地

    :param tag: 设备标签
    :param param: 待保存参数
    :param function_id: 算法编号
    :param process_type: 进程类型
    """
    name = get_parameter_tag(tag, function_id, process_type)
    if name in parameters:
        logger.warning(f'[parameter:{tag}:{process_type}:{function_id}]'
                       f'在本地缓存中存在相同的算法参数, 该算法参数将被覆盖.')
    parameters[get_parameter_tag(tag, function_id, process_type)] = param


def clear_loc():
    """
    清空本地设备配置与算法参数
    """
    options.clear()
    parameters.clear()


def load_loc(process_type: str, loc_option_dir: str = None, loc_parameter_dir: str = None):
    """
    读取本地设备与参数列表
    ./loc:
    -config.json
    -sc1(dir)
        -dev_option1.json
        -dev_option2.json
        -config.json
        ...
    -sc2(dir)
        -dev_option3.json
        -dev_option4.json
        -config.json
        ...

    :param process_type:
    :param loc_option_dir: 本地点位配置路径
    :param loc_parameter_dir: 本地参数路径
    """
    if process_type == 'summary':
        loc_option_dir = loc_option_dir or config.LOC_SUMMARY_OPTIONS_DIR
    else:
        loc_option_dir = loc_option_dir or config.LOC_PROGRAM_OPTIONS_DIR

    # 读取场景总配置
    # {
    #   "exclude": [""]
    # }
    if not os.path.exists(loc_option_dir):
        logger.error(f"""
        未找到存放算法进程的目录：loc-program-option，通过全局配置scdap.config.LOC_PROGRAM_OPTIONS_DIR进行配置
        请参考wiki教程：https://gitlab.sucheon.com/algorithm/wiki/-/wikis/%E7%AE%97%E6%B3%95%E6%9C%8D%E5%8A%A1%E6%A1%86%E6%9E%B6%E5%8E%9F%E7%90%86/%E7%82%B9%E4%BD%8D%E9%85%8D%E7%BD%AE
        配置目录格式必须为：
        loc-program-option/
        loc-program-option/<scene-name>/
        loc-program-option/<scene-name>/node1.json
        loc-program-option/<scene-name>/node2.json
        loc-program-option/<scene-name>/node3.json
        即建议在配置的目录下新建一个以场景区分的文件夹后，在这个场景文件夹中新增配置文件，配置文件的内容必须为.json后缀且如开头的格式一样进行配置
        """)
        return

    tconfig = load_json(os.path.join(loc_option_dir, 'config.json'), dict())
    exclude_scene = tconfig.get('exclude', list())
    for sfile in os.listdir(loc_option_dir):
        if sfile in {'__pycache__', '.idea'}:
            continue

        if sfile in exclude_scene:
            continue

        scene_path = os.path.join(loc_option_dir, sfile)
        if not os.path.isdir(scene_path):
            if not config.LOAD_NET_OPTION_MODE:
                logger.error(f"""
                本地算法配置目录格式必须为：loc-program-option/<scene-name>/node1.json
                请参考wiki教程：https://gitlab.sucheon.com/algorithm/wiki/-/wikis/%E7%AE%97%E6%B3%95%E6%9C%8D%E5%8A%A1%E6%A1%86%E6%9E%B6%E5%8E%9F%E7%90%86/%E7%82%B9%E4%BD%8D%E9%85%8D%E7%BD%AE
                配置目录格式必须为：
                loc-program-option/
                loc-program-option/<scene-name>/
                loc-program-option/<scene-name>/node1.json
                loc-program-option/<scene-name>/node2.json
                loc-program-option/<scene-name>/node3.json
                即建议在配置的目录下新建一个以场景区分的文件夹后，在这个场景文件夹中新增配置文件，配置文件的内容必须为.json后缀且如开头的格式一样进行配置
                """)
            continue

        # 读取场景的配置文件
        # config.json
        # {
        #     "exclude": [],
        #     "scene_name": "",
        #     "scene_id": 0,
        #     "class": "program"
        # }

        sconfig = load_json(os.path.join(scene_path, 'config.json'), dict())

        # 需要排除场景文件
        exclude = sconfig.get('exclude', list())

        for dfile in os.listdir(scene_path):
            if dfile == 'config.json':
                # 是场景配置文件
                continue

            if not dfile.endswith('.json'):
                # 非配置.json文件
                continue

            if os.path.splitext(dfile)[0] in exclude:
                # 在排除名单中
                continue

            dev_options = load_json(os.path.join(scene_path, dfile), dict())

            if isinstance(dev_options, dict):
                dev_options = [dev_options]

            if len(dev_options) == 0:
                continue

            for dev_option in dev_options:
                # 添加设备配置至本地
                parse_option(process_type, dev_option, True, loc_parameter_dir=loc_parameter_dir)


def parse_option(process_type: str, option: dict, save: bool = True, loc_parameter_dir: str = None) \
        -> Tuple[Optional[dict], Optional[Dict[int, dict]]]:
    """
    将配置解析并保存至本地
    返回的结果结构：
    return option, parameters
    option: {'tag': ... }
    parameter: {
        fid1(int): parameter,
        fid2(int): parameter,
        ...
    }

    :param process_type:
    :param option: 待解析的设备配置
    :param save: 是否将解析的配置保存至本地loc
    :param loc_parameter_dir: 本地参数路径
    :return: 解析的配置与参数
    """
    check_process_type(process_type)
    tag = option[option_key.tag]

    if save:
        add_option(tag, process_type, option)
    fun = parse_parameter(process_type, option, save, loc_parameter_dir)
    return option, fun


def parse_parameter(process_type: str, option: dict, save: bool = True, loc_parameter_dir: str = None) \
        -> Optional[Dict[int, dict]]:
    """
    将配置解析并保存至本地
    返回结果结构：
    {
        fid1(int): parameter,
        fid2(int): parameter,
        ...
    }

    :param process_type:
    :param option: 待解析的设备配置
    :param save: 是否将解析的配置保存至本地loc
    :param loc_parameter_dir: 本地参数路径
    :return: 解析的配置与参数
    """
    check_process_type(process_type)
    tag = option[option_key.tag]

    functions = option[option_key.other] + option[option_key.decision] + option[option_key.evaluation]

    if process_type == 'summary':
        loc_parameter_dir = loc_parameter_dir or config.LOC_SUMMARY_PARAMETERS_DIR
    else:
        loc_parameter_dir = loc_parameter_dir or config.LOC_PROGRAM_PARAMETERS_DIR

    ps = dict()
    for func in functions:
        if not isinstance(func, dict):
            raise Exception('请在设备配置中算法配置为: {"function": "...", ...}的字典格式.')
        function_class = func[option_key.function]
        # 类型检查 (str/BaseFunction)
        # 获取function_id
        if isinstance(function_class, str):
            # 通过解析字符串名称获取fid
            function_id = parser_id(function_class)
        elif issubclass(function_class, BaseFunction):
            # 通过接口获取fid
            function_id = function_class.get_function_id()
        else:
            raise Exception('请在设备配置中配置正确的function, function的类型必须为[str, BaseFunction].')

        # 本地参数文件路径
        loc_parameter = func.get(option_key.loc_parameter)
        if loc_parameter:
            func.pop(option_key.loc_parameter)
            loc_parameter = os.path.join(loc_parameter_dir, loc_parameter)
            # *.pkl
            if loc_parameter.endswith('.pkl'):
                parameter = load_pickle(loc_parameter)
            # *.json
            elif loc_parameter.endswith('.json'):
                parameter = load_json(loc_parameter)
            else:
                raise Exception('参数文件的类型错误, 必须为[*.json, *.pkl].')

        else:
            parameter = deepcopy(func)
            # parameter.pop(option_key.function, None)
            # parameter.pop(option_key.global_parameter, None)
            # parameter.pop(option_key.loc_parameter, None)
        if parameter:
            if save:
                add_parameter(tag, function_id, process_type, parameter)
            ps[function_id] = func
    return ps


def load_loc_program(loc_option_dir: str = None, loc_parameter_dir: str = None):
    """
    读取本地设备与参数列表(program)
    ./loc:
    -config.json
    -sc1(dir)
        -dev_option1.json
        -dev_option2.json
        -config.json
        ...
    -sc2(dir)
        -dev_option3.json
        -dev_option4.json
        -config.json
        ...

    :param loc_option_dir: 本地点位配置路径
    :param loc_parameter_dir: 本地参数路径
    """
    load_loc('program', loc_option_dir, loc_parameter_dir)


def parse_program_option(option: dict, save: bool = True, loc_parameter_dir: str = None) \
        -> Tuple[Optional[dict], Optional[Dict[int, dict]]]:
    """
    将配置解析并保存至本地(program)
    返回的结果结构：
    return option, parameters
    option: {'tag': ... }
    parameter: {
        fid1(int): parameter,
        fid2(int): parameter,
        ...
    }

    :param option: 待解析的设备配置
    :param save: 是否将解析的配置保存至本地loc
    :param loc_parameter_dir: 本地参数路径
    :return: 解析的配置与参数
    """
    return parse_option('program', option, save, loc_parameter_dir)


def parse_program_parameter(option: dict, save: bool = True, loc_parameter_dir: str = None) \
        -> Optional[Dict[int, dict]]:
    """
    将配置解析并保存至本地(program)
    返回结果结构：
    {
        fid1(int): parameter,
        fid2(int): parameter,
        ...
    }

    :param option: 待解析的设备配置
    :param save: 是否将解析的配置保存至本地loc
    :param loc_parameter_dir: 本地参数路径
    :return: 解析的配置与参数
    """
    return parse_parameter('program', option, save, loc_parameter_dir)


def load_loc_summary(loc_option_dir: str = None, loc_parameter_dir: str = None):
    """
    读取本地设备与参数列表(program)
    ./loc:
    -config.json
    -sc1(dir)
        -dev_option1.json
        -dev_option2.json
        -config.json
        ...
    -sc2(dir)
        -dev_option3.json
        -dev_option4.json
        -config.json
        ...

    :param loc_option_dir: 本地点位配置路径
    :param loc_parameter_dir: 本地参数路径
    """
    load_loc('summary', loc_option_dir, loc_parameter_dir)


def parse_summary_option(option: dict, save: bool = True, loc_parameter_dir: str = None) \
        -> Tuple[Optional[dict], Optional[Dict[int, dict]]]:
    """
    将配置解析并保存至本地(program)
    返回的结果结构：
    return option, parameters
    option: {'tag': ... }
    parameter: {
        fid1(int): parameter,
        fid2(int): parameter,
        ...
    }

    :param option: 待解析的设备配置
    :param save: 是否将解析的配置保存至本地loc
    :param loc_parameter_dir: 本地参数路径
    :return: 解析的配置与参数
    """
    return parse_option('summary', option, save, loc_parameter_dir)


def parse_summary_parameter(option: dict, save: bool = True, loc_parameter_dir: str = None) \
        -> Optional[Dict[int, dict]]:
    """
    将配置解析并保存至本地(program)
    返回结果结构：
    {
        fid1(int): parameter,
        fid2(int): parameter,
        ...
    }

    :param option: 待解析的设备配置
    :param save: 是否将解析的配置保存至本地loc
    :param loc_parameter_dir: 本地参数路径
    :return: 解析的配置与参数
    """
    return parse_parameter('summary', option, save, loc_parameter_dir)
