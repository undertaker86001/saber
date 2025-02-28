"""

@create on: 2020.12.11
获取历史特征数据相关的接口
"""
__all__ = ['generator_get_data', 'get_data']

import os
import pickle
from threading import Lock
from datetime import datetime, timedelta
from typing import Generator, Optional, Iterable, List

import numpy as np

from scdap import config
from scdap.logger import logger
from scdap.data import FeatureList
from scdap.api import device_define
from scdap.transfer import rabbitmq
from scdap.core.threadpool import submit
from scdap.util.tc import lrtime_to_hrtime
from scdap.util.session import parse_router, get
from scdap.flag import column, format_column, convert_column

# 数据缓存结构的版本
# 每次更新了数据结构或者缓存结构后需要对应的更新版本号
# 防止数据读取错误
__version__ = '2'
__mkdir_lock__ = Lock()


def _mkdir(path: str):
    if os.path.exists(path):
        return
    with __mkdir_lock__:
        if os.path.exists(path):
            return
        try:
            os.makedirs(path)
        except Exception as e:
            logger.warning(f'文件夹路径: {path}创建失败，错误: {e}, '
                           f'如果是多进程异步的运行, 可能是有多个进程同时在创建相同的文件夹导致的错误, 重启即可')
            raise


def _split_datetime(start: datetime, stop: datetime, delta: timedelta, ignore: bool = True):
    """
    将时间切分
    如：
    start = datetime(2020, 1, 1, 10)
    stop = datetime(2020, 1, 1, 12)
    delta = timedelta(hours=1)
    结果为 [[datetime(2020, 1, 1, 10), datetime(2020, 1, 1, 11)], [datetime(2020, 1, 1, 11), datetime(2020, 1, 1, 12)]]

    :param start:
    :param stop:
    :param delta:
    :param ignore:
    :return:
    """
    if start >= stop:
        return list()
    temp_start = datetime(*start.timetuple()[:4])
    temp_end = datetime(*stop.timetuple()[:4])
    if stop > temp_end:
        temp_end += delta
    count = int((temp_end - temp_start) / delta)
    if count <= 0:
        return [[start, stop]]
    if not ignore and stop > temp_end:
        count += 1
    start_step = np.arange(count) * delta + temp_start
    end_step = start_step + delta
    if not ignore:
        start_step[0] = max(start_step[0], start)
        end_step[-1] = min(end_step[-1], stop)
    return list(zip(start_step, end_step))


def _get_data(url: str, algorithm_id: str, start: datetime, stop: datetime,
              select_column: Iterable[str], reso: Optional[int] = None, from_cache: bool = None) -> FeatureList:
    from_cache = from_cache if from_cache is not None else config.DEVICE_DATA_CACHE

    select_column = format_column(select_column)

    # 禁用缓存则直接返回数据
    if not from_cache:
        return _do_request(url, algorithm_id, start, stop, select_column, reso)

    cache_dir = os.path.join(config.DEVICE_DATA_CACHE_DIR, str(algorithm_id))

    _mkdir(cache_dir)

    delta = timedelta(hours=1)
    flist = FeatureList(algorithm_id, column=select_column)
    for sep_start, sep_end in _split_datetime(start, stop, delta):
        sep_dir = os.path.join(cache_dir, sep_start.strftime('%Y-%m'))

        _mkdir(sep_dir)

        sep_path = os.path.join(sep_dir, sep_start.strftime('%d-%H') + '.pkl')

        cache = _get_data_from_local(sep_path, select_column)
        if cache is None:
            cache = _get_data_from_net(sep_path, url, algorithm_id, sep_start, sep_end, select_column, reso)

        # 如果输入的时间不是整小时数
        # 需要做切割
        if sep_start < start < sep_end or sep_start < stop < sep_end:
            for f in cache.generator():
                if f.time > stop:
                    break
                if f.time >= start:
                    flist.append_item(f)
        else:
            flist.extend_itemlist(cache)
    return flist


def _get_data_from_local(path: str, select_column: List[str]) -> Optional[FeatureList]:
    """
    从本地缓存目录获取特征数据

    :param path:
    :param select_column:
    :return:
    """
    if not os.path.exists(path):
        return None

    # 如果缓存存在则直接读取
    with open(path, 'rb') as f:
        cache = f.read()

    if cache:
        logger.debug(f'从本地目录: {path}载入缓存数据.')
        try:
            cache = pickle.loads(cache)
        except Exception as e:
            logger.warning(f'缓存数据: {path}解析失败: {e}, 将移除旧的数据后重新缓存.')
            os.remove(path)
            return None

        if not isinstance(cache, dict):
            logger.warning(f'缓存数据: {path}缓存数据结构发生了变化, 将移除旧的数据后重新缓存.')
            os.remove(path)
            return None

        # 特征数据的数据结构发生过一次大幅度的调整
        # 需要根据版本好自动判断是否缓存了旧版本的数据结构
        version = cache.get('version', '1')
        if version != __version__:
            logger.warning(f'缓存数据: {path}特征数据的版本发生变化: '
                           f'v{version} -> v{__version__}, 将移除旧的数据后重新缓存.')
            os.remove(path)
            return None

        cache_column = set(cache.get('column'))
        select_column = set(select_column)
        # 只要select_column中的所有特征都能在cache_column中找到, 就不用重新缓存, 反之就要重新缓存
        if not cache_column.issuperset(select_column):
            logger.warning(f'缓存数据: {path}配置的特征发生了变化: '
                           f'{cache_column} -> {select_column}, 将移除旧的数据后重新缓存.')
            os.remove(path)
            return None

        return cache.get('cache')
    return None


def _get_data_from_net(path: str, url: str, algorithm_id: str, start: datetime, stop: datetime,
                       select_column: List[str], reso: Optional[int] = None) -> FeatureList:
    """
    将缓存保存至本地
    {
        "cache": FeatureList,
        "column": set(str)
    }

    :param path: 缓存路径
    :param url: 数据获取接口
    :param algorithm_id: 算法点位名称
    :param start: 起始时间范围(最小颗粒度为小时)
    :param stop: 结束时间范围(最小颗粒度为小时)
    :param select_column: 使用的特征
    :param reso: 高分特征的分辨率, 默认查看scdap.config.HF_RESOLUTION
    :return:
    """
    cache = _do_request(url, algorithm_id, start, stop, select_column, reso)
    # 如果当前这一个小时还没过完, 则先跳过不保存
    # 因为当前一个小时的数据还未完全上传完毕, 如果缓存的话可能会造成数据丢失
    # 比如now = 2020.02.01 15:50:00
    # 如果缓存数据2020-02/01-15.pkl
    # 那么意味着以后从缓存数据中获取的数据都只有2020.02.01 15:00:00 ~ 2020.02.01 15:50:00,
    # 将遗漏掉 2020.02.01 15:50:00 ~ 2020.02.01 16:00:00 的数据
    # 所以对于这种情况则不缓存, 直到已经过了这一个小时, 下次读取的时候再缓存
    if start < datetime.now() < stop:
        logger.debug(f'跳过数据缓存文件: {path}, 不对最近一个小时内的数据进行缓存.')
    else:
        if cache.size() == 0:
            logger.debug(f'跳过数据缓存文件: {path}, 因为数据大小为0.')
            return cache
        select_column = format_column(select_column)
        with open(path, 'wb') as f:
            pickle.dump({"cache": cache, 'column': set(select_column), 'version': __version__}, f)
        logger.debug(f'保存数据缓存文件: {path}, version: {__version__}, column: {select_column}.')
    return cache


def _do_request(url: str, algorithm_id: str, start: datetime, stop: datetime,
                select_column: List[str], reso: Optional[int] = None) -> FeatureList:
    """
    请求数据

    :param url: 数据获取接口
    :param algorithm_id: 算法点位名称
    :param start: 起始时间范围(最小颗粒度为小时)
    :param stop: 结束时间范围(最小颗粒度为小时)
    :param select_column: 使用的特征
    :param reso: 高分特征的分辨率, 默认查看scdap.config.HF_RESOLUTION
    :return:
    """
    node_id = device_define.algorithm_id2node_id(algorithm_id, False)
    select_column = format_column(select_column)

    flist = FeatureList(algorithm_id, node_id, select_column)

    try:
        resp = get(url, params={
            'nodeId': node_id,
            'startTime': int(start.timestamp() * 1000),
            'endTime': int(stop.timestamp() * 1000),
            'columns': ','.join(convert_column(rabbitmq.RabbitMQFeatureItemKV(), select_column))
        }, headers={'verification-permission': 'sucheon.registerBox'}, verify=False)

    except Exception as e:
        logger.error(f'设备: {algorithm_id}时间段:[{start}, {stop}]获取数据失败, get()运行过程中发生错误: {e}.')
        return flist

    resp.close()

    if resp.status_code != 200:
        logger.error(f'设备: {algorithm_id}时间段:[{start}, {stop}]获取数据失败, http返回错误代码{resp.status_code},'
                     f'url: {url}, 请按照如下顺序排查: '
                     f'1).请检查是否已关闭vpn; '
                     f'2).如果url中包含ot/t等字段或者ip为192.168.x.x, 请确保当前电脑连接的网络处在公司内网中(注意实验室与办公室不处在同一网段); '
                     f"3).请确保scdap.config.load(...)读取了正确的配置, 如果错误中包含http://127.0.0.1/一般代表没有正确读取配置; "
                     f"4).请联系后端相关人员, 并且提供错误的状态码(或者提供截图); "
                     f"5).联系管理员排查;")

        return flist

    resp.close()
    resp = resp.json()
    # 有两种数据接口的访问方式
    # 1. 使用后端开放的外部通用接口
    # 数据格式是按照后端接口规范编写的
    # {
    #   "code": "00000",
    #   "message": "...",
    #   "result": {
    #       "data": [
    #           {
    #               "meanHf": 100000,
    #               ...
    #           },
    #           ...
    #       ]
    #   },
    # }
    # 2. 访问本地部署的内部数据接口, 该方式将是直接访问读写hbase数据的数据服务
    # 数据格式直接返回一个列表
    # [
    #   {
    #       "meanHf": 1000000000,
    #       ...
    #   },
    #   ...
    # ]
    # 之所以这么分主要是因为对于部署的hbase服务器来说, 其访问带宽没有限制
    # 但是对于后端开放的外部通用接口来说(使用esc部署的服务)是有带宽限制的
    # 所以现在的方式是:
    # 对于公司内部网络/北京实验室等通过在对应环境内部服务器部署一个直接访问hbase的数据服务, 借此直接访问hbase, 防止带宽限制
    # 对于外部网络, 则只能走后端通用接口
    # 对于部署环境, 也同样走的是后端通用接口(当然也可以是走服务发现的方式)
    # 所以在这里将通过结果类型判断(dict/list)来确认走的是哪一种接口

    # dict走的是后端通用数据接口
    if isinstance(resp, dict):
        code = resp.get('code', '10000')
        message = resp.get('message', '')
        if str(code) != '10000':
            logger.error(f"设备: {algorithm_id}时间段:[{start}, {stop}]获取数据失败, 返回代码:{code}, 错误信息: {message}"
                         f'url: {url}, 请按照如下顺序排查: '
                         f'1).请检查是否已关闭vpn; '
                         f'2).如果url中包含ot/t等字段或者ip为192.168.x.x, 请确保当前电脑连接的网络处在公司内网中(注意实验室与办公室不处在同一网段); '
                         f"3).请确保scdap.config.load(...)读取了正确的配置, 如果错误中包含http://127.0.0.1/一般代表没有正确读取配置; "
                         f"4).请联系后端相关人员, 并且提供错误的状态码(或者提供截图); "
                         f"5).联系管理员排查;")

            return flist
        resp = resp['result']
    # list走的是直接访问hbase的内部数据接口
    else:
        resp = {'data': resp, 'algorithmId': algorithm_id, 'nodeId': node_id}

    decoder = rabbitmq.get_feature_list_decoder()
    try:
        flist = decoder.decode(resp, flist)
        # 高分时间戳数据无法从接口中获取
        # 所以必须自行解析
        if column.has_hrtime(select_column):
            hr_curr_time = None
            reso = reso or config.HF_RESOLUTION
            for feature in flist.generator():
                feature.hrtime = lrtime_to_hrtime(hr_curr_time, feature.time, reso)
                hr_curr_time = feature.time
        return flist
    except Exception as e:
        raise type(e)(f'设备: {algorithm_id}时间段:[{start}, {stop}]获取数据失败, 解析数据过程中发生错误: {e}')


def get_data(algorithm_id: str, start: datetime, stop: datetime,
             select_column: List[str] = None, use_thread: bool = False, from_cache: bool = None) \
        -> FeatureList:
    """
    通过http接口获取特征数据

    :param from_cache: 是否从缓存内读取, 默认为scdap.config.DEVICE_DATA_CACHE
    :param algorithm_id: 算法点位编号
    :param start: 起始时间
    :param stop: 结束时间
    :param select_column: 需要获取的特征
    :param use_thread: 是否使用多线程获取数据
    """
    if select_column is None:
        select_column = column.normal_column

    if start >= stop:
        raise Exception('参数start的数值必须大于参数stop的数值.')
    if use_thread:
        flist = FeatureList(algorithm_id)
        for f in generator_get_data(algorithm_id, start, stop, select_column, from_cache=from_cache):
            flist += f
    else:
        flist = _get_data(config.API_DEVICE_DATA_GET_URL, algorithm_id,
                          start, stop, select_column, from_cache=from_cache)
    flist._algorithm_id = algorithm_id
    return flist


def _get_url() -> str:
    """
    :return: 获取url
    """
    return parse_router(config.BACKEND_SERVER_URL, '/api/manager/open/feature')


def _gen_split_datetime(start: datetime, stop: datetime, delta: timedelta):
    datetime_list = list()

    temp = start
    while temp < stop:
        next_temp = min(temp + delta, stop)
        datetime_list.append((temp, next_temp))
        temp = next_temp
    return datetime_list


def generator_get_data(algorithm_id: str, start: datetime, stop: datetime, select_column: Iterable[str] = None,
                       gradatim: bool = False, delta: timedelta = None, use_thread: bool = False,
                       from_cache: bool = None) \
        -> Generator[FeatureList, None, None]:
    """
    迭代器, 按顺序抛出数据

    :param from_cache: 是否从缓存内读取, 默认为scdap.config.DEVICE_DATA_CACHE
    :param algorithm_id: 算法点位编号
    :param start: 起始时间
    :param stop: 结束时间
    :param select_column: 需要获取的特征
    :param gradatim: 是否逐步的获取数据, 即在发送一笔数据前开始获取之后一笔数据, 默认统一获取所有数据后在进行迭代器数据抛出, 至在use_thread=True时有效
    :param delta: 获取的每一段数据的间隔时间
    :param use_thread: 是否启用多线程,
    :return: 数据
    """
    if select_column is None:
        select_column = column.normal_column

    delta = delta or timedelta(days=1)
    if start >= stop:
        raise Exception('参数start的数值必须大于参数stop的数值.')

    if delta.total_seconds() < 3600:
        raise Exception('参数delta的数值必须大于或等于1小时.')

    url = config.API_DEVICE_DATA_GET_URL
    split_time = _gen_split_datetime(start, stop, delta)

    if not use_thread:
        for d in split_time:
            yield _get_data(url, algorithm_id, *d, select_column=select_column, from_cache=from_cache)
    elif gradatim:
        # 在发送一段数据的之前读取下一段数据
        # 在优化数据获取速度的同时能够节省内存空间
        # 即读数据与计算并行的模式
        previous = None
        while split_time:
            time = split_time.pop(0)
            f = submit(_get_data, url, algorithm_id, *time, select_column=select_column, from_cache=from_cache)
            if previous:
                yield previous.result()
            previous = f

        if previous:
            yield previous.result()

    else:
        for f in [
            submit(_get_data, url, algorithm_id, *d, select_column=select_column, from_cache=from_cache)
            for d in split_time
        ]:
            yield f.result()
