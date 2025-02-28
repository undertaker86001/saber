"""

@create on: 2021.02.25
k8s用探针检测
"""
import os
from scdap import config
from scdap.core.controller import BaseController


class ProbeController(BaseController):
    """
    在k8s部署模式下
    首先算法进程会一直往配置的probe目录下写入两个空文件，每运行一次controller的循环就会写入一次
        - probe_dir/liveness-program-{program.tag} -> k8s的存活探针
        - probe_dir/startup-program-{program.tag}  -> k8s的启动探针

    k8s通过调用指令
        - rm probe_dir/liveness-program-{program.tag} 监控进程是否存活
        - rm probe_dir/startup-program-{program.tag}  监控进程是否启动

    所以整个流程就是
    1.算法进程跑一次controllers的循环就写入一次文件
    2.k8s定时调用一次移除指令以检测进程是否存活, 如果持续一段时间一直删除文件失败意味着进程卡住, 届时会进行相应的重启操作

    具体的逻辑请看下面定义的run()

    参数:
    probe_dir: str          探针保存的目录
    liveness_probe: str     存活探针的保存路径, 如果没有配置则配置为 probe_dir/liveness-program-{program.tag}
    startup_probe: str      启动探针的保存路径, 如果没有配置则配置为 probe_dir/startup-program-{program.tag}
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._probe_dir = self._get_option('probe_dir', config.PROBE_DIR)

        liveness_probe = os.path.join(self._probe_dir, f'liveness-program-{self._context.tag}')
        self._liveness_probe = self._get_option('liveness_probe_path', liveness_probe)

        startup_probe = os.path.join(self._probe_dir, f'startup-program-{self._context.tag}')
        self._startup_probe = self._get_option('startup_probe', startup_probe)

        # 启动探针只在启动的时候运行一次
        self._is_startup = False

        if not os.path.exists(self._probe_dir):
            os.makedirs(self._probe_dir)

    @staticmethod
    def get_controller_name() -> str:
        return 'probe'

    def run(self):
        # 启动探针
        if not self._is_startup:
            with open(self._startup_probe, 'w') as f:
                f.write(str(os.getpid()))
            self.logger_debug(f'startup probe file -> {self._startup_probe}')
            self._is_startup = True

        if os.path.exists(self._liveness_probe):
            return

        # 存货探针
        with open(self._liveness_probe, 'w') as f:
            f.write(str(os.getpid()))

        self.logger_debug(f'liveness probe file -> {self._liveness_probe}')
