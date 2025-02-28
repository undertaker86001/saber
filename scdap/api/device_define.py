"""

@create on: 2020.12.08
"""
from typing import List, Tuple, Optional

from requests import exceptions

from scdap import config
from scdap.util.session import get, post, parse_router

CODE_OK = '00000'


class HttpCodeError(Exception):
    pass


class ResultNotLeaf(Exception):
    pass


class DeviceItem(object):
    def __init__(self, data: dict):
        if not data.get('isLeaf', False):
            raise ResultNotLeaf(f'DeviceItem必须是叶子节点!当前节点信息：{data}')

        self._data = data
        self.node_id = data['id']
        self.algorithm_id = data.get('algorithmId', str(self.node_id))
        self.device_name = data.get('nameZh', 'unknown')
        self.scene_id = data.get('sceneNodeId', 0)
        self.scene_name = data.get('sceneNodeName', 'unknown')
        self.uuid = data.get('')
        self.port = 0

    def __str__(self):
        return f'aid={self.algorithm_id}, ' \
               f'nid={self.node_id}, ' \
               f'name={self.device_name}, ' \
               f'scene={self.scene_name}({self.scene_id})'


class BoxItem(object):
    def __init__(self, node_id: int, data: dict):
        self._data = data
        self.node_id = node_id
        self.uuid = data.get('uuid', '')
        self.port = data.get('sshPort', 0)

    def __str__(self):
        return f'node_id={self.node_id}, ' \
               f'uuid={self.uuid}, ' \
               f'port={self.port}'


class SceneItem(object):
    def __init__(self, data: dict):
        if data['nodeType'].lower() != 'scene':
            raise Exception('该节点不是场景节点.')
        self._data = data
        self.scene_id = data['id']
        self.scene_name = data['nameZh']


def _get_url(router: str):
    return parse_router(config.BACKEND_SERVER_URL, router)


def _request_api(method, url, **kwargs) -> dict:
    try:
        resp = method(url, **kwargs, timeout=10, headers={'verification-permission': 'sucheon.registerBox'}, verify=False)
    except exceptions.ProxyError:
        raise exceptions.ProxyError('请检查是否已关闭vpn.')
    except Exception as e:
        raise type(e)(f"接口: {url}调用失败, 错误信息: {e}; "
                      f"请按照如下顺序进行排查: "
                      f'1).请检查是否已关闭vpn; '
                      f'2).如果url中包含ot/t等字段或者ip为192.168.x.x, 请确保当前电脑连接的网络处在公司内网中(注意实验室与办公室不处在同一网段); '
                      f"3).请确保scdap.config.load(...)读取了正确的配置, 如果错误中包含http://127.0.0.1/一般代表没有正确读取配置; "
                      f"4).请联系后端相关人员, 并且提供错误的状态码(或者提供截图); "
                      f"5).联系管理员排查;")

    if resp.status_code != 200:
        raise Exception(f"接口: {url}调用返回错误的状态码: {resp.status_code}, 请按照如下顺序进行排查: "
                        f'1).请检查是否已关闭vpn; '
                        f'2).如果url中包含ot/t等字段或者ip为192.168.x.x, 请确保当前电脑连接的网络处在公司内网中(注意实验室与办公室不处在同一网段); '
                        f"3).请确保scdap.config.load(...)读取了正确的配置, 如果错误中包含http://127.0.0.1/一般代表没有正确读取配置; "
                        f"4).请联系后端相关人员, 并且提供错误的状态码(或者提供截图); "
                        f"5).联系管理员排查;")

    resp.close()

    try:
        resp = resp.json()
    except Exception as e:
        raise Exception(f"接口: {url}调用错误, 无法解析返回数据: {e}.")

    if resp['code'] != CODE_OK:
        raise HttpCodeError(f"接口: {url}调用返回错误的状态码: {resp['code']}, 错误信息: {resp['message']}, "
                            f"请按照如下顺序进行排查: "
                            f"1).如果发现错误信息中包含msg:businessError, 请重启程序, 一般可以解决; "
                            f"2).请联系后端相关人员, 并提供错误截图; "
                            f"3).联系管理员排查;")

    return resp['result']


def list_device_define(page_size: int = 0, page_num: int = 1, scene_id: int = None, token: str = None) \
        -> Tuple[int, int, List[DeviceItem]]:
    """
    列出设备定义信息

    :param page_size:
    :param page_num:
    :param scene_id:
    :param token: 权限码
    :return:
    """
    params = {
        'pageSize': page_size,
        'pageNum': page_num
    }
    if scene_id:
        params['sceneId'] = scene_id
    resp = _request_api(get, _get_url('/api/manager/tree/node/leaves'), params=params)
    return resp['pageMeta']['total'], resp['pageMeta']['totalPage'], list(map(DeviceItem, resp['list']))


def get_box_define_by_node_id(node_id: int, token=None) -> BoxItem:
    """
    获取橙盒相关的内容，如端口号，uuid等

    :param node_id:
    :param token:
    :return:
    """
    response = _request_api(get, _get_url('/api/manager/box/node_id'), params={'nodeId': node_id})
    return BoxItem(node_id, response['data'])


def get_box_define_by_algorithm_id(algorithm_id: str, token=None) -> BoxItem:
    """
    获取橙盒相关的内容，如端口号，uuid等

    :param algorithm_id:
    :param token:
    :return:
    """
    return get_box_define_by_node_id(algorithm_id2node_id(algorithm_id), token)


def get_device_define(algorithm_id: str, token: str = None) -> Optional[DeviceItem]:
    item = get_device_define_batch([algorithm_id], token=token, force=False)
    if item:
        return item[0]
    return None


def get_device_define_batch(algorithm_id: List[str], token: str = None,
                            page_size: int = 0, page_num: int = 1, force: bool = True) \
        -> List[DeviceItem]:
    """
    获取设备的详细信息

    :param algorithm_id: 算法点位编号
    :param token: 权限码
    :param page_num:
    :param page_size:
    :param force: 是否强制查询的aid数量必须和结果的数量相同, 如果不相同则抛出一次
    :return: 详细信息
    """
    response = _request_api(post, _get_url('/api/manager/open/algorithm_id'),
                            json={'algorithmIdList': algorithm_id, 'pageSize': page_size, 'page_num': page_num})

    if force and len(algorithm_id) != len(response['data']):
        raise Exception(f'至少有一个algorithm_id不存在.')

    return list(map(DeviceItem, response['data']))


def get_device_define_by_node_id(node_id: int, token: str = None) -> Optional[DeviceItem]:
    """
    获取点位相关的信息，如点位名称，场景信息

    :param node_id:
    :param token:
    :return:
    """
    item = get_device_define_batch_by_node_id([node_id], token=token, force=False)
    if item:
        return item[0]
    return None


def get_device_define_batch_by_node_id(node_id: List[int], token: str = None,
                                       page_size: int = 0, page_num: int = 1, force: bool = True) \
        -> List[DeviceItem]:
    """
    获取设备的详细信息

    :param node_id: 算法点位编号
    :param token: 权限码
    :param page_num:
    :param page_size:
    :param force: 是否强制查询的aid数量必须和结果的数量相同, 如果不相同则抛出一次
    :return: 详细信息
    """
    response = _request_api(
        post, _get_url('/api/manager/open/id_list'),
        json={'idList': node_id, 'pageSize': page_size, 'page_num': page_num})

    if force and len(node_id) != len(response['data']):
        raise Exception(f'至少有一个node_id不存在.')

    return list(map(DeviceItem, response['data']))


def list_scene(token: str = None) -> List[SceneItem]:
    """
    请求获取所有列表

    :param token: 权限码
    :return:
    """
    resp = _request_api(get, _get_url('/api/manager/tree/node/scenes'), params={'pageSize': 0})
    return list(map(SceneItem, resp['list']))


def device_exist(algorithm_id: str, token: str = None) -> bool:
    """
    检查设备是否存在

    :param algorithm_id: 算法点位编号
    :param token: 权限码
    :return:
    """
    try:
        return len(get_device_define_batch([algorithm_id], force=False)) == 1
    except (HttpCodeError, ResultNotLeaf):
        return False


def device_exist_by_node_id(node_id: int, token: str = None) -> bool:
    """
    检查设备是否存在

    :param node_id: 算法点位编号
    :param token: 权限码
    :return:
    """
    try:
        return len(get_device_define_batch_by_node_id([node_id], force=False)) == 1
    except (HttpCodeError, ResultNotLeaf):
        return False


def algorithm_id2node_id(algorithm_id: str, force: bool = True) -> int:
    """
    通过algorithm_id查询node_id

    :param algorithm_id:
    :param force: 是否强制查询的aid数量必须和结果的数量相同, 如果不相同则抛出一次
    :return:
    """
    return algorithm_id2node_id_batch([algorithm_id], force=force)[0]


def node_id2algorithm_id(node_id: int, force: bool = True) -> str:
    """
    通过node_id查询algorithm_id

    :param node_id:
    :param force: 是否强制查询的aid数量必须和结果的数量相同, 如果不相同则抛出一次
    :return:
    """
    return node_id2algorithm_id_batch([node_id], force=force)[0]


def algorithm_id2node_id_batch(algorithm_id: List[str], force: bool = True) -> List[int]:
    """
    通过algorithm_id查询node_id(批量)

    :param algorithm_id:
    :param force: 是否强制查询的aid数量必须和结果的数量相同, 如果不相同则抛出一次
    :return:
    """
    if not config.ID_REFLICT_BY_API:
        return list(map(int, algorithm_id))

    result = get_device_define_batch(algorithm_id, force=force)
    if len(result) == len(algorithm_id) or force:
        return [n.node_id for n in result]
    aid2nid = {n.algorithm_id: n.node_id for n in result}
    return [aid2nid.get(aid, int(aid)) for aid in algorithm_id]


def node_id2algorithm_id_batch(node_id: List[int], force: bool = True) -> List[str]:
    """
    通过node_id查询algorithm_id(批量)

    :param node_id:
    :param force: 是否强制查询的aid数量必须和结果的数量相同, 如果不相同则抛出一次
    :return:
    """
    if not config.ID_REFLICT_BY_API:
        return list(map(str, node_id))

    result = get_device_define_batch_by_node_id(node_id, force=force)
    if len(result) == len(node_id) or force:
        return [n.algorithm_id for n in result]
    nid2aid = {n.node_id: n.algorithm_id for n in result}
    return [nid2aid.get(nid, str(nid)) for nid in node_id]
