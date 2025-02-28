"""

@create on: 2021.03.22
替换版本号
需要替换版本号的文件必须配置于: .version_file
替换符号配置为: ${VERSION}

package to wheels
.whl_parameters.json -> wheel包信息配置
{
    "lib_name": "目标库目录路径名称",
    "module_name": "需要打包成的库名称",
    "package_data": {}                  // setup.package_data参数
    "packages_parameter": {             // setuptools.find_namespace_packages参数
        "include": ("*",),
        "exclude": ()
    }
    "version": "1.0.0",                 // **必填**, 版本号, 在develop(测试环境下上传至pypi-develop)master环境下才上传至正式的pypi
    "author": "Zhang",                  // 作者
    "author_email": "zhang@sucheon.com",    // email
    "install_requires": "requirements.txt"  // 依赖文件路径
    "description": "str",                   // 库的简要描述
    "long_description": "README.md",        // 完整的库说明文件路径
    "long_description_content_type": "text/markdown",   // 库说明文件类型
    "python_requires": ">=3.7",     // python版本配置
    "license": "LICENSE"            // LICENSE
    ...
}

"""
import os
import sys
import json
import warnings
from pprint import pprint
from typing import Optional
from importlib import import_module

import requests
from twine import cli
from setuptools.extension import Extension
from setuptools import setup, find_namespace_packages


def get_requirements(path: str) -> list:
    """
    解析依赖库信息
    """
    requires = list()

    if not os.path.exists(path):
        return requires

    with open(path, 'r') as file:
        for line in file.readlines():
            if len(line) <= 1:
                continue
            if line[-1] == '\n':
                line = line[:-1]
            requires.append(line)
    return requires


def from_file(path: str):
    if not os.path.exists(path):
        return ''
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def get_extensions(lib_dir: str, lib_name: str, exclude: list = ()):
    packages = find_namespace_packages(lib_name)
    packages = [f"{lib_name}.{path}" for path in packages]
    packages.append(lib_name)
    lib_dir = os.path.normpath(lib_dir)
    extensions = []
    for package in packages:
        path = os.path.join(lib_dir, package.replace('.', os.path.sep))
        for fname in os.listdir(path):
            simple_path = os.path.join(path, fname)
            if fname.endswith('.py') and fname not in exclude:
                simple_package = f'{package}.{os.path.splitext(fname)[0]}'
                # print(f'{simple_package} -> {simple_path}')
                extensions.append(Extension(simple_package, [simple_path]))
    return extensions


def get_parameter(setup_parameter: dict, setup_parameter_key: str,
                  module=None, module_key: str = None, default=None):
    p = setup_parameter.get(setup_parameter_key)
    if p is not None:
        return p

    if module is None or module_key is None:
        if default is None:
            print(f'can not find {setup_parameter_key}.')
            raise SystemExit(1)
        return default

    p = getattr(module, module_key, default)
    if p is None:
        print(f'can not find {setup_parameter_key} {module}.')
        raise SystemExit(1)
    return p


def do_request(method: str, url: str, data: dict = None, token: str = '', raise_exc: bool = True) -> Optional[dict]:
    """

    :param method:
    :param url:
    :param data:
    :param token:
    :param raise_exc:
    :return:
    """
    try:
        response = requests.request(method, url, json=data, timeout=5, headers={'Authorization': token})
    except Exception as e:
        if raise_exc:
            raise e
        else:
            warnings.warn(f'do_request: {url}发生错误: {e}')
            return None
    response.close()

    if response.status_code != 200:
        if raise_exc:
            raise Exception(f'sqlapi接口: {url}调用失败, http返回码为: {response.status_code}')
        else:
            return None

    response = response.json()
    # 无法找到数据
    if response['code'] == 'B0100':
        print(f'sqlapi接口: {url}调用失败, 返回码: {response["code"]}, 错误信息: {response.get("message")}')
        return None

    if response['code'] != '00000':
        if raise_exc:
            raise Exception(f'sqlapi接口: {url}调用失败, 返回码: {response["code"]}, 错误信息: {response.get("message")}')
        else:
            return None

    return response['result']


def package_wheel(setup_parameter: dict):
    lib_name = setup_parameter['lib_name']
    module_name = setup_parameter.get('module_name', lib_name)

    try:
        module = import_module(lib_name)
    except Exception as e:
        print(f'can not import module: {lib_name}, error: {e}')
        raise SystemExit(1)

    packages_parameter = setup_parameter.get('packages_parameter', dict())
    packages = find_namespace_packages(lib_name,
                                       include=packages_parameter.get('include', ('*',)),
                                       exclude=packages_parameter.get('exclude', ()))
    packages = [f"{lib_name}.{path}" for path in packages]
    packages.insert(0, lib_name)

    sys.argv.extend(['bdist_wheel', '-q'])
    kwargs = {
        "name": module_name,
        "packages": packages,
        "package_data": setup_parameter.get('package_data', dict()),
        "version": get_parameter(setup_parameter, 'version', module, '__version__'),
        "long_description": from_file(get_parameter(
            setup_parameter, 'long_description', default='README.md')),
        "long_description_content_type": get_parameter(
            setup_parameter, 'long_description_content_type', default="text/markdown"),
        "license": from_file(
            get_parameter(setup_parameter, 'license', default='LICENSE')),
        "author": get_parameter(
            setup_parameter, 'author', module, "__author__", default='Sucheon Algoritm Department'),
        "author_email": get_parameter(
            setup_parameter, 'author_email', module, "__email__", default='haobin.zhang@sucheon.com'),
        "description": get_parameter(
            setup_parameter, 'description', module, "__description__", default=f'Sucheon Algoritm Lib - {lib_name}'),
        "install_requires": get_requirements(
            get_parameter(
                setup_parameter, 'install_requires', default='requirements.txt')),
        "python_requires": get_parameter(
            setup_parameter, 'python_requires', default='>=3.7'),
    }

    pprint(kwargs)
    setup(**kwargs)
    return kwargs


def main(whl_parameter_path):
    # 读取gitlab中的项目名称, 以解析成whl库名称
    lib_name = os.environ.get('CI_PROJECT_NAME')
    if not lib_name:
        lib_name = os.path.split(os.path.split(__file__)[0])[1]
    lib_name = lib_name.lower().replace(' ', '_').replace('-', '_')

    # 获取分支名称
    # 用于区分多套环境
    # develop -> 测试环境
    # master -> 正式环境
    env = os.environ.get('CI_COMMIT_REF_NAME')
    env_upper = env.upper()

    # 根据环境获取对应的pypi服务器
    twine_url = os.environ.get(f'{env_upper}_TWINE_SERVER_URL')
    if twine_url is None:
        print(F'can not find env value: {env_upper}_TWINE_SERVER_URL')
        raise SystemExit(1)
    print('twine server:', twine_url)
    user = os.environ.get('TWINE_SERVER_USER')
    if user is None:
        print('can not find env value: TWINE_SERVER_USER')
        raise SystemExit(1)
    user, password = user.split('/')

    # 根据环境获取对应的sqlapi服务
    # 服务用于保存版本号至数据库
    server_url = os.environ.get(f'{env_upper}_SQLAPI_SERVER_URL')
    if server_url is None:
        print(f'can not find env value: {env_upper}_SQLAPI_SERVER_URL')
        raise SystemExit(1)
    # sqlapi接口权限token
    token = os.environ.get('SQLAPI_SERVER_TOKEN', '')

    # 读取模块打包的参数配置文件
    print(f'load file: {whl_parameter_path}')
    if os.path.exists(whl_parameter_path):
        with open(whl_parameter_path, 'r', encoding='utf-8') as f:
            whl_parameters = json.load(f)
    else:
        print(f'can`t find file: {whl_parameter_path}')
        whl_parameters = {'lib_name': lib_name}

    module_name = whl_parameters.get('module_name', lib_name)
    # 打包成whl
    print('whl parameter:')
    pprint(whl_parameters)
    whl_result = package_wheel(whl_parameters)
    print('package module success.')
    print('module:', list(os.listdir('dist/')))
    print('result module info:')
    pprint(whl_result)

    # 上传whl至pypi
    print('upload module wheel to twine.')
    cli.dispatch(['upload', '--repository-url', twine_url, '-u', user, '-p', password, 'dist/*'])
    print('upload module wheel success.')

    version = whl_result['version']
    url = f'{server_url}/module-version/{module_name}/'
    result = do_request('get', url, token=token, raise_exc=False)
    print('old module version data:')
    pprint(result)

    extra = dict()
    old_version = 'null'
    if result:
        extra = result['data']['extra']
        old_version = result['data']['version']

    data = {
        'module_name': module_name,
        'version': version,
        'description': whl_result['description'],
        'extra': extra
    }

    extra['CI_COMMIT_SHA'] = os.environ.get('CI_COMMIT_SHA')
    extra['CI_COMMIT_SHORT_SHA'] = os.environ.get('CI_COMMIT_SHORT_SHA')
    extra['CI_COMMIT_REF_NAME'] = os.environ.get('CI_COMMIT_REF_NAME')
    print('update module version data:')
    pprint(data)

    # 更新版本号信息至数据库
    if old_version == 'null':
        do_request('post', url, data, token=token, raise_exc=False)
    else:
        do_request('put', url, data, token=token, raise_exc=False)


if __name__ == '__main__':
    if sys.argv[1:]:
        pjson = sys.argv[-1]
        sys.argv.pop(-1)
    else:
        pjson = '.whl.json'
    main(pjson)
