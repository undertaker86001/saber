"""

@create on 2020.03.02
定时任务控制器, 在固定时间该控制器将固定触发对应的操作
目前拥有的操作是定时更新算法参数
"""
from .controller import CrontabController
controller_class = CrontabController
