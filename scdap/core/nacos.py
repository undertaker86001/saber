"""

@create on: 2021.06.15
nacos是一个服务发现与保存配置的服务框架
算法部分只是用配置相关的
方便获取框架的配置
"""
import json
from typing import Optional

import requests
from scdap.logger import logger

__GET_ROUTER__ = '/v1/cs/configs'


def get_config(data_id: str, namespace: str, group: str,
               url: str, username: str, password: str) -> Optional[dict]:
    """
    获取nacos配置

    :param data_id: 配置名称
    :param namespace: 命名空间
    :param group: 组别
    :param url: nacos服务地址
    :param username: 用户
    :param password: 密码
    :return:
    """
    try:
        s = requests.Session()
        resp = s.get(url=f'{url}{__GET_ROUTER__}',
                            params={'group': group, 'dataId': data_id, 'tenant': namespace,
                                    'username': username, 'password': password}, verify=False)
    except Exception as e:
        logger.error(f'nacos配置获取失败, 错误: {e}')
        return None

    if resp.status_code != 200:
        logger.error(f'nacos配置获取失败, 返回错误的http code: {resp.status_code}, message: {resp.text}')
        return None

    lines = filter(lambda line: not line.lstrip(' ').startswith('//'), resp.text)
    config = json.loads(''.join(lines))

    return config
