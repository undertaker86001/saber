"""

@create on: 2021.02.02
运行测试服务
可测试内容为program的数据交互工作
"""
from datetime import datetime

if __name__ == '__main__':
    from scdap import config
    config.load("conf/my.test/master.json")

    from test_serve.feature_app import main
    delta = 0.5
    start_time = datetime(2021, 5, 8, 12)
    main(delta, start_time)
