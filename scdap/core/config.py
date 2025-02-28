"""

@create on: 2021.02.04
"""
import os
import json
import warnings


class BaseConfigure(object):
    # 配置路径
    CONFIG_PATH = 'config.json'
    # 模块名称
    # 在读取配置的时候会使用到
    # 可以查看self._load_conf()
    MODULE_NAME = 'default'
    # 模块的详细备注名称
    # 在推送email报警的时候会用到
    MODULE_DESC = 'default'

    # -----------------------------------------------------------------------
    #                             NACOS CONFIG
    # -----------------------------------------------------------------------
    # 配置可以从nacos中获取
    NACOS_URL = 'http://localhost'
    NACOS_USER = 'nacos'
    NACOS_PASSWORD = 'nacos'
    NACOS_GROUP = 'DEFAULT_GROUP'
    NACOS_NAMESPACE = 'sucheon-dap'
    NACOS_DATAID = 'default'

    def __init__(self):
        self.REMAIN_CONFIG = dict()

    @property
    def COMMON_NAME(self) -> str:
        return f'{self.MODULE_NAME}-{self.MODULE_DESC}'

    def load_from_nacos(self, data_id: str = None, namespace: str = None, group: str = None,
                        url: str = None, username: str = None, password: str = None):
        """
        从nacos中获取配置

        :param data_id: 配置名称
        :param namespace: 命名空间
        :param group: 组别
        :param url: nacos服务地址
        :param username: 用户
        :param password: 密码
        :return:
        """
        data_id = data_id or self.NACOS_DATAID
        namespace = namespace or self.NACOS_NAMESPACE
        group = group or self.NACOS_GROUP
        url = url or self.NACOS_URL
        username = username or self.NACOS_USER
        password = password or self.NACOS_PASSWORD

        from scdap.core.nacos import get_config
        config = get_config(data_id, namespace, group, url, username, password)
        if config:
            print('已从nacos中获取配置.')
            self._load_conf(config)

    def _load_conf(self, config: dict):
        # 支持下列格式
        # {
        #   "MODULE_NAME": {
        #       ...
        #   }
        # }
        if self.MODULE_NAME in config:
            config = config[self.MODULE_NAME]

        for key, val in config.items():
            if hasattr(self, key):
                # print(f'config set key: {key} -> {val}')
                setattr(self, key, val)
            else:
                self.REMAIN_CONFIG[key] = val

    def load(self, load_json: bool = False, file: str = None):
        """
        读取配置

        :param load_json: 是否载入配置
        :param file: 配置文件路径
        """
        # 因为有时候会输错, 忘了输入load_json=True
        # 所以干脆直接在发现load_json是字符串的时候直接代表需要读取目标文件的配置
        if isinstance(load_json, str):
            file = load_json
            load_json = True

        if not load_json:
            return

        file = file or self.CONFIG_PATH
        if not file.endswith('.json'):
            warnings.warn(f'配置文件: {file}不是json格式的文件.')

        if not os.path.exists(file):
            warnings.warn(f'无法找到配置文件: {file}.')
            return

        with open(file, 'r', encoding='utf-8') as f:
            # 过滤注释 //
            lines = filter(lambda line: not line.lstrip(' ').startswith('//'), f.readlines())
            config = json.loads(''.join(lines))
        if config:
            print(f'已从{file}中获取配置.')
            self._load_conf(config)
