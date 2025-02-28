"""

@create on: 2020.04.27
"""


class OptionKey(object):
    tag = 'tag'
    device_name = 'name'
    devices = 'devices'
    scene_name = 'scene_name'

    process_type = 'process_type'

    type = 'type'

    scene_id = 'scene_id'
    model = 'model'
    # 设备组算法参数
    worker = 'worker'

    function = 'function'
    functions = 'functions'
    function_id = 'function_id'

    global_parameter = 'global_parameter'

    decision = 'decision'
    evaluation = 'evaluation'
    other = 'other'
    loc_parameter = 'loc_parameter'

    # 设备组配置文件算法进程参数
    clock_time = 'clock_time'
    email = 'email'

    extra = 'extra'

    sub_option = 'sub_option'

    enabled = 'enabled'
    transfer_mode = 'transfer_mode'
    description = 'description'


option_key = OptionKey
