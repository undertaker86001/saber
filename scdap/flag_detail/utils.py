"""

@create on: 2021.03.22
"""
import os
from pprint import pprint
from typing import Optional, Dict, Union, List

import requests


def package_dict(name: str, details: list, item_key: str):
    result = dict()
    for d in details:
        item = d[item_key]
        if item not in result:
            result[item] = d
        else:
            raise Exception(f'在{name}中发现多个"{item_key}"="{item}"的数据.')
    return result


def do_request(method: str, url: str, data: List[dict] = None, token: str = '') -> Optional[dict]:
    """

    :param method:
    :param url:
    :param data:
    :param token:
    :return:
    """
    response = getattr(requests, method)(url, json=data, timeout=60, headers={'Authorization': token})
    response.close()

    if response.status_code != 200:
        raise Exception(f'sqlapi接口: {url}调用失败, http返回码为: {response.status_code}')

    response = response.json()
    # 无法找到数据
    if response['code'] == 'B0100':
        return None

    if response['code'] != '00000':
        raise Exception(f'sqlapi接口: {url}调用失败, 返回码: {response["code"]}, 错误信息: {response.get("message")}')

    return response['result']


def get_sqlapi_info():
    # 获取分支名称
    # 用于区分多套环境
    # develop -> 测试环境
    # master -> 正式环境
    env = os.environ.get('CI_COMMIT_REF_NAME', 'my')
    env_upper = env.upper()

    if env == 'my':
        return env, 'http://127.0.0.1:18602/api', ''

    if env == "LOCAL":
        return env, os.environ.get("SQLAPI_SERVER_URL", "http://127.0.0.1:18602/api"), ""

    # 根据环境获取对应的sqlapi服务
    # 服务用于保存版本号至数据库
    server_url = os.environ.get(f'{env_upper}_SQLAPI_SERVER_URL')
    if server_url is None:
        print(f'can not find env value: {env_upper}_SQLAPI_SERVER_URL')
        raise SystemExit(1)

    # sqlapi接口权限token
    token = os.environ.get('SQLAPI_SERVER_TOKEN')
    if token is None:
        print(f'can not find env value: SQLAPI_SERVER_TOKEN')
        raise SystemExit(1)

    return env, server_url, token


def update_info(name: str, router: str, data: Dict[Union[int, str], dict], replace: bool = True,
                router_suffix_key: list = None):
    """
    上传接口信息至sqlapi中

    :param name: 需要上传的数据库表相关的名称备注，主要用来显示
    :param router: 路由
    :param data: 待上传的数据
    :param replace: 是否在发现数据已经存在的情况下直接替换
    :param router_suffix_key: 部分接口上传需要带多个后缀， 这里可以配置
    :return:
    """
    print(f'start update {name}')
    env, server_url, token = get_sqlapi_info()

    extra = {
        'CI_COMMIT_SHORT_SHA': os.environ.get('CI_COMMIT_SHORT_SHA', ''),
        'CI_COMMIT_REF_NAME': os.environ.get('CI_COMMIT_REF_NAME', ''),
        'CI_COMMIT_SHA': os.environ.get('CI_COMMIT_SHA', ''),
        'SCDAP_ALGORITHM_VERSION': os.environ.get('AUTO_GEN_VERSION', ''),
    }

    need_add_or_update_data = []
    for key, detail in data.items():
        print('-' * 50)
        print(f"{name}:", key)
        if not detail:
            print(f'can`t find {key} in {name}.')
            raise SystemExit(1)
        detail['extra'] = extra
        need_add_or_update_data.append(detail)
        # if router_suffix_key is None:
        #     url = f'{server_url}{router}{key}/'
        # else:
        #     url_key = ':'.join(map(str, map(detail.get, router_suffix_key)))
        #     url = f'{server_url}{router}{url_key}/'
        #
        # data = do_request('get', url, token=token)
        # data = data['data'] if data else None
        #
        # pprint(data)
        # print('->')
        # pprint(detail)
        #
        # if not data:
        #     print('add data to sql.')
        #     detail['extra'] = extra
        #     do_request('post', url, detail, token=token)
        # if data and replace:
        #     for k, v in detail.items():
        #         if data.get(k) != v:
        #             print('update date to sql.')
        #             detail['extra'] = extra
        #             do_request('put', url, detail, token=token)
        #             break
    url = f'{server_url}{router}batch'
    print(f'update-url : {url} -> \n\r')
    pprint(need_add_or_update_data)
    do_request('post', url, need_add_or_update_data, token=token)
    print('-' * 50)
    print('finish update status_define...')
