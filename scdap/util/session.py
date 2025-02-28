"""

@create on: 2020.03.13
使用http进行连接与接口数据获取的时候使用requests.session重用底层链接
"""
import ast
import atexit
from typing import Optional
from scdap.logger import logger

import requests

_session = requests.session()
atexit.register(_session.close)

post = _session.post
get = _session.get

simple_post = requests.post
simple_get = requests.get


class PickleError(Exception):
    pass


def parse_router(url: str, router: str = None):
    if router is None:
        return url

    return url.rstrip('/') + '/' + router.lstrip('/')


def parse_url(host: str = 'localhost', port: int = None, router: str = None, protocol: str = 'http') -> str:
    """
    输入服务器域名相关信息解析成url地址链接

    :param host: 服务器域名, 默认为localhost
    :param port: 服务器端口号, 默认为80
    :param router: 服务器域名路径
    :param protocol: 协议前缀
    :return: 返回完整的路径
    """
    if router and router.startswith('http'):
        return router

    if port is not None:
        url = f"{protocol}://{host}:{port}"
    else:
        url = f"{protocol}://{host}"

    if router is not None:
        if router.startswith('/'):
            url = f"{url}{router}"
        else:
            url = f"{url}/{router}"

    return url


def do_request(method, url: str, params: dict = None, data=None, json: dict = None,
               content_type: str = None, timeout=60, binary: bool = False, json_type=False, headers: dict = None,
               decode_response: bool = True) -> Optional[dict]:
    """

    :param method:
    :param url:
    :param params:
    :param data:
    :param json:
    :param content_type:
    :param timeout:
    :param binary:
    :param json_type:
    :param headers:
    :param decode_response:
    :return:
    """
    kwargs = {
        'url': url, 'data': data, 'params': params,
        'json': json, 'timeout': timeout, 'verify': False
    }
    if content_type:
        kwargs['headers'] = {'Content-Type': content_type}

    if headers:
        kwargs['headers'].update(headers)

    try:
        response = requests.request(method, **kwargs)
    except requests.exceptions.ProxyError:
        raise requests.exceptions.ProxyError('请确保自己没有开启VPN.')

    response.close()

    if response.status_code != 200:
        raise Exception(f'sqlapi接口: {url}调用失败, http返回码为: {response.status_code}')

    if not response.content:
        return None

    if binary:
        try:
            return ast.literal_eval(response.content.decode("utf-8"))
        except Exception as e:
            raise TypeError(f'接口: {url}无法转换为字典并进行解析, 可能是不存在该段需要查询的数据: {e}')
    if json_type:
        try:
            logger.info(f"获取到的数据为：{response.content}")
            return response.json()
        except Exception as e:
            raise TypeError(f'接口: {url}无法转换为字典并进行解析, 可能是不存在该段需要查询的数据: {e}')

    response = response.json()
    if not decode_response:
        return response

    if response['code'] != '00000':
        raise Exception(f'接口: {url}调用失败, 返回码: {response["code"]}, 错误信息: {response.get("message")}')

    return response['result']['data']
