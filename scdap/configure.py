"""

@create on 2020.09.25
"""
from scdap.core.config import BaseConfigure


class Configure(BaseConfigure):
    # -----------------------------------------------------------------------
    #                             COMMON CONFIG
    # -----------------------------------------------------------------------
    MODULE_NAME = 'scdap'

    # 本地初始化参数文件.json保存位置
    # 如果存在该初始化参数文件，config所有参数将优先以.json中设置的数值为准
    CONFIG_JSON_PATH = "config.json"

    # 本地参数的保存目录位置
    # 在里面可以配置相应的点位配置文件
    # 当LOAD_LOCAL_OPTION=True的时候会从里面读取配置
    # 详细的可以查看scdap.gop.loc
    # 注意在获取配置的时候首先要调用方法scdap.gop.loc.load_loc_program()/scdap.gop.loc.load_loc_summary()
    # 在这之后可以通过scdap.gop.loc.get_option获取
    # 当然也可以通过scdap.gop.func里面的一系列get接口获取，详细的请查看里面的接口说明
    LOC_PROGRAM_OPTIONS_DIR = "loc-program-option"
    LOC_SUMMARY_OPTIONS_DIR = "loc-summary-option"

    # 本地参数文件缓存位置
    # 在算法配置同级字典下配置
    # {
    #       "function": "function1",
    #       "loc_parameter: 'file.json/pkl', -> LOC_PARAMETERS_DIR/op['loc_parameter']
    # }
    # 详细的可以查看scdap.gop.loc
    # 注意在获取配置的时候首先要调用方法scdap.gop.loc.load_loc_parameter()/scdap.gop.loc.load_loc_parameter()
    # 在这之后可以通过scdap.gop.loc.get_parameter获取
    # 当然也可以通过scdap.gop.func里面的一系列get接口获取，详细的请查看里面的接口说明
    LOC_PROGRAM_PARAMETERS_DIR = 'loc-program-parameter'
    LOC_SUMMARY_PARAMETERS_DIR = 'loc-summary-parameter'

    # 从框架外部导入算法
    # 注意，外部算法文件位置只能放置再main.py同级的目录下
    # 框架将使用importlib.import_module进行导入
    # 调用的地方在scdap.frame.function.fset.load
    # 里面会从配置的FUNCTION_LIB中读取相应的算法
    # 并且获取所有算法
    FUNCTION_LIB = 'scdap_algorithm.function'

    # 启动探针/就绪探针文件保存目录
    # 一般在k8s环境下使用
    # 在scdap.extendc.probe模块中
    # 具体想看scdap.extendc.probe内的说明
    PROBE_DIR = 'probe'

    # 通讯模式, 即数据传输方式
    # rabbitmq
    # 与scdap.transfer相关联
    # rabbitmq: box -> cloud -> rabbitmq-queue -> algorithm process -> rabbitmq-queue -> cloud -> show in fronted
    TRANSFER_MODE = 'rabbitmq'

    # 读取数据库配置表
    # 当配置为true的时候，算法中在获取option/prameter的时候将会优先选择从线上获取相关的内容
    LOAD_NET_OPTION = True

    # 读取本地配置表，优先级此与LOAD_NET_OPTION
    # 当配置为true的时候，算法会在LOAD_NET_OPTION获取失败后从本地环境中获取option/prameter
    LOAD_LOCAL_OPTION = True

    # 从网络读取配置/参数的模式
    # http/sql
    # 主要是为了在线上环境下优化进程占用
    # http占用的空间相对更小
    # 如果使用sql则因为需要导入数据库相关的依赖而变得非常大
    LOAD_NET_OPTION_MODE = 'http'

    # debug模式
    # 在debug模式下算法出错的时候将不会被接住而是直接报错后终止程序
    DEBUG = True

    # 测试用参数
    REGISTER_TO_DEBUG_SERVER = False

    # 高分分辨率, 关系到高分特征配置
    HF_RESOLUTION = 24

    # 部分框架会在安装的时候进行加密，故需要添加整个框架允许的扩展名
    INCLUDE_EXTENSION = {'so', 'pyd', 'py'}

    # 数据缓存
    # 因为读取线上数据的速度过慢
    # 所以通过缓存的形式来加速
    # 详细请查看scdap.api.device_data中的说明
    DEVICE_DATA_CACHE = True
    DEVICE_DATA_CACHE_DIR = 'cache'

    # 查询algorithm_id/node_id的方式
    # True  -> 通过后端接口, 互查映射关系
    # False -> 直接通过转型进行映射 前提是 int(algorithm_id) == node_id
    # 一般是用在没有后端(或者不方便访问后端)的环境下
    # 算法使用的是algorithm_id
    # 后端使用的是node_id
    # 所以算法这边在运作的时候需要先查询node_id
    # 并在对应的与后端的数据交互中使用node_id
    # 具体可以查看scdap.api.device_define.algorithm_id2node_id_batch/node_id2algorithm_id_batch
    ID_REFLICT_BY_API = True

    # -----------------------------------------------------------------------
    #                                LOG CONFIG
    # -----------------------------------------------------------------------
    # 日志储存路径
    LOG_DIR = "log"
    # 是否将日志保存至本地文件夹中
    SAVE_LOG = False
    # 是否配置成在stdout显示日志时显示详细的日志内容
    # 当为true的时候会有详细的日志时间戳信息以及日志的level信息
    SHOW_STDOUT_DETAIL = False
    # 是否展示详细的计算结果
    # 之所以有该参数主要是因为print实在是太慢了, 就算把STDOUT_LEVEL配置成高level也会比较慢
    SHOW_COMPUTE_RESULT = False
    # scdap.extendc.stat的日志数据，通过这个开关可以关闭
    SHOW_STAT_CONTROLLER_LOG = True
    # stdout的日志level
    # DEBUG/SECO/INFO/WARNING/ERROR/CRITICAL
    STDOUT_LEVEL = 'INFO'

    # 默认通用的日志配置
    # 在scdap.logger.Logger.initial中调用
    DEFAULT_LOG_PARAM = {
        # 日志命名规则
        'nsink': "{time:YYYYMMDD-HHmm}.info",
        # 日志滚存机制
        'nrotation': "1 day",
        # 日志缓存时长
        'nretention': "1 weeks",
        # 是否在发现异常的时候追溯异常的发生位置并且保存在日志中
        'ntrack': True,
        # 日志显示的等级配置
        # DEBUG/SECO/INFO/WARNING/ERROR/CRITICAL
        'nlevel': 'INFO',
        # 异常日志命名规则
        'esink': "{time:YYYYMMDD-HHmm}.err",
        # 异常日志滚存机制
        'erotation': "1 day",
        # 异常日志缓存时长
        'eretention': "1 weeks",
        # 异常日志记录的最低异常类型
        # DEBUG/SECO/INFO/WARNING/ERROR
        'elevel': 'WARNING',
        # 是否在发现异常的时候追溯异常的发生位置并且保存在异常日志中
        'etrack': True,
    }

    # -----------------------------------------------------------------------
    #                              SERVER CONFIG
    # -----------------------------------------------------------------------
    # 数据获取接口的线程池线程数量配置
    # 特征数据获取的时候可能是用到多线程
    API_THREADPOOL_SIZE = 10

    # 后端服务配置
    # 比如获取node_id的相关点位信息需要使用到后端
    BACKEND_SERVER_URL = 'http://127.0.0.1'

    # 算法服务框架专用的数据库接口服务
    # 与算法有关的，如算法参数、点位配置、状态等一系列与算法有端的东西都可以从该服务中获取
    SQLAPI_SERVER_URL = 'http://127.0.0.1:8602'

    # scdap.api.device_data历史数据获取接口url
    # 使用node_id查询
    API_DEVICE_DATA_GET_URL = 'http://127.0.0.1:8846/api/manager/open/feature'

    # scdap.api.device_data历史数据获取接口url
    # 使用algorithm_id查询
    API_DEVICE_DATA_GET_BY_AID_URL = 'https://127.0.0.1:8846/api/manager/open/feature/algorithm_id'

    # 历史数据更新
    # stat表数据更新
    DEVICE_HISTORY_UPDATE_STAT_URL = 'http://127.0.0.1/cdata/stat/batch_update'
    DEVICE_HISTORY_GET_STAT_URL = 'http://127.0.0.1/cdata/stat/queryDeviceStatusExtract'
    DEBUG_SERVER_URL = 'http://127.0.0.1:8846'

    # -----------------------------------------------------------------------
    #                              PROGRAM CONFIG
    # -----------------------------------------------------------------------
    # 进程中的控制器运行顺序如下
    # [probe] -> [crontab] -> get -> worker -> [stat] -> [alarm] -> send
    # probe: scdap.extendc.probe查看
    # crontab: scdap.extendc.crontab查看, 一般不使用了
    # stat: scdap.extendc.stat查看
    # alarm: scdap.extendc.alarm查看
    # []中代表可选
    # 配置方式:
    # {
    #   "probe": true,
    #   "stat": true,
    #   ...
    # }
    # 可以至scdap.wp.wprocess.WorkerProcess._create_controller()/_controller_switch中了解详情
    DEFAULT_CONTROLLER_SWITCH = {}

    # BaseController全局异常配置
    # controller中会有一个异常捕获次数计数器
    # 当捕获的异常次数超过配置的次数时将抛出异常, 相当于结束进程
    # 是否启用计数
    # 详细可以至scdap.core.controller.BaseController.on_exception中查看实现细节
    CONTROLLER_EXCEPTION_COUNTER_SWITCH = True
    # 最大异常次数
    CONTROLLER_MAX_EXCEPTION_COUNT = 5
    # 异常计数超时时间, 单位为秒
    CONTROLLER_EXCEPTION_RESET_DELTA = 600

    # 是否在解析的时候抛掉错误的实时数据
    # 错误的数据一般为重复的或者是时间戳错误的数据
    # 可以至scdap.data.container.Container查看详细的逻辑
    DUMP_ERROR_DATA = True

    # 根据当前的系统时间时间错误的数据
    # now_time: 当前系统时间
    # 假设配置为 [a, b]
    # 则过滤的时间为:
    # [now_time - a, now_time + b]
    # 如果配置为0代表不过滤
    # 在debug模式下不启用
    # 可以至scdap.data.container.Container查看详细的逻辑
    FILTER_DATA_TIME = [0, 0]

    # 算法进程睡眠与唤醒间隔
    # 考虑到进程在k8s管理下拥有10s左右的存活探针
    # 所以应配置睡眠时间 < 10s
    PROGRAM_CLOCK_TIME = 1
    # 部分算法会在进程配置中配置算法进程睡眠与唤醒间隔
    # 进程默认优先读取进程配置后，如果没有获取才会读取PROGRAM_CLOCK_TIME
    # 故再次添加一个全局变量用于针对这种情况能够强制修改算法进程睡眠与唤醒间隔，无视进程配置
    DEBUG_CLOCK_TIME = None

    # 算法进程的初始日志配置
    # 如果设置了数值, 在需要使用的时候会在DEFAULT_LOG_PARAM的基础上update
    PROGRAM_LOG_PARAM = {
    }

    # 容器最大容量，超过容量将删除溢出的数据
    CONTAINER_MAXLEN = 60 * 60 * 1
    RESULT_MAXLEN = 60 * 60 * 4
    # 在mq模式下如果数据发送失败则将缓存数据
    # 该参数用于配置最多的数据缓存量
    RESULT_CACHELEN = RESULT_MAXLEN * 2

    # 算法进程默认的定时更新时间
    # [day, hour, minute, second]
    PROGRAM_CRONTAB_TIME = [1, 0, 0, 0]

    # 测试用接口配置
    # 主要是向python自行设计的伪后端服务注册信息
    # 使得伪后端能够获取对应的数据供进程使用
    PROGRAM_DEBUG_REGISTER_ROUTER = '/register/'

    # -----------------------------------------------------------------------
    #                               EMAIL CONFIG
    # email报警配置
    # -----------------------------------------------------------------------

    # 是否启用email报警机制
    # scdap.api.email
    OPEN_EMAIL_SERVE = False
    # 在框架触发报警时的目标邮件
    # 在scdap.core.controller.BaseController.on_exception会使用到
    WARNING_EMAIL_ADDRESS = 'haobin.zhang@sucheon.com'

    # email stmp服务器设置
    EMAIL_SMTP_HOST = 'smtp.mxhichina.com'
    EMAIL_SMTP_PORT = 80

    # ssl/tls
    EMAIL_SMTP_CONNECT_TYPE = 'tls'
    EMAIL_ADDRESS = 'service-notification@sucheon.com'
    EMAIL_PASSWORD = 'ClJSlRl$620'

    # -----------------------------------------------------------------------
    #                          SUCHEON MYSQL CONFIG
    # 数据库配置
    # -----------------------------------------------------------------------

    # sucheon数据库相关配置
    MYSQL_HOST = "127.0.0.1"
    MYSQL_PORT = 3306
    MYSQL_SCDAP_DATABASE = "ddps_dap"

    MYSQL_USER = "root"
    MYSQL_PASSWORD = "root"

    MYSQL_CHARSET = "utf8mb4"

    # -----------------------------------------------------------------------
    #                         RABBITMQ TRANSFER CONFIG
    # -----------------------------------------------------------------------
    # scdap.transfer.rabbitmq
    # rabbitmq 相关的配置
    RABBITMQ_HOST = "localhost"
    RABBITMQ_PORT = 5672
    # 用户信息
    RABBITMQ_USER = 'sucheon-dap'
    RABBITMQ_PASSWORD = 'sucheon-dap'
    # virtual host
    RABBITMQ_VHOST = '/'
    # 是否启用心跳机制
    RABBITMQ_HEARTBEAT = None
    # 队列特征数据数据获取网关交换器名称
    RABBITMQ_GET_EXCHANGE = 'gateway-node-data-exchange'
    # 考虑到进程在k8s管理下拥有10s左右的存活探针
    # 所以应配置堵塞超时时间 < 10s
    RABBITMQ_GET_TIMEOUT = 3
    # 订阅的route_key前缀规则
    # scene.x/scene.#/scene.*
    RABBITMQ_GET_ROUTING_KEY_PREFIX = 'scene'
    # 队列名称前缀, 主要是用来防止mq中队列名称冲突
    RABBITMQ_GET_QUEUE_NAME_PERFIX = 'dap.process.get'

    # 队列因为一些机制原因, 在批量传数据的时候
    # 后端数据推送到rabbitmq中, mq中因为通道机制的存在(并发的通道)
    # 可能会导致即使后端按顺序推送数据至mq
    # 最终队列中取到的数据也是乱序的
    # 所以mq中会配置一个顺序队列来进行排序
    # 该参数用于配置顺序队列发现顺序错误的容忍时长
    MAX_ENDURANCE_LIMIT = 60
    # 算法结果发送的对应交换机
    RABBITMQ_SEND_EXCHANGE = ''
    # 算法计算结果后发送的队列名称
    RABBITMQ_SEND_QUEUE_NAME = 'py.compute.result'

    # -----------------------------------------------------------------------
    #                              REDIS CONFIG
    # -----------------------------------------------------------------------
    # scdap.core.redis
    REDIS_HOST = '127.0.0.1'
    REDIS_PORT = 6379
    REDIS_PASSWORD = ''
    REDIS_DB = 10
    REDIS_PREFIX_KEY = 'dap:process'
    # -----------------------------------------------------------------------
    #                             中间件配置
    # -----------------------------------------------------------------------
    LIMIT_EVENT = 0  # 事件是否限制, 0为不限制, 其他为限制的条数


config = Configure()
