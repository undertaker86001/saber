"""

@create on: 2021.01.11
requirements:
    pytest
    pytest-xdist
    pyrabbit
"""
import pytest

# 单元测试模块
PYTEST_MODULE = 'unittests'
# 需要排除的测试模块
EXCLUDE_MODULE = ["test_sqlapi"]
# 多进程测试数量
PROCESS_NUM = 'auto'


if __name__ == '__main__':
    from scdap import config

    # 关闭框架的日志输出
    config.STDOUT_LEVEL = 'CRITICAL'
    # 配置数据库
    from unittests.config import mysql_config
    config.MYSQL_HOST = mysql_config['host']
    config.MYSQL_PORT = mysql_config['port']
    config.MYSQL_USER = mysql_config['user']
    config.MYSQL_PASSWORD = mysql_config['password']
    config.MYSQL_SCDAP_DATABASE = mysql_config['db']
    if EXCLUDE_MODULE:
        exclude = ['-k', ' and '.join(map(lambda e: f'not {e}', EXCLUDE_MODULE))]
    else:
        exclude = list()

    pytest.main(['-v', '-x', '-n', PROCESS_NUM, PYTEST_MODULE, *exclude])
