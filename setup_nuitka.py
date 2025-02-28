"""

@create on: 2021.02.04
"""
import os
import shutil
import argparse
import subprocess
from functools import partial
from threading import Thread
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor

# nuitka3 --show-modules --show-memory --show-progress --remove-output
# --output-dir=dist --nofollow-imports --include-module=scdap scdap


CMD = 'nuitka3 --remove-output --nofollow-imports ' \
      '--no-pyi-file --module --output-dir={0} {1}'


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dist-dir', dest='dist_dir', type=str, default='build',
        help='编译后保存的目标目录, 默认为dist/'
    )
    parser.add_argument(
        '-t', dest='tmp', type=str, default='tmp',
        help='中间文件缓存目录, 默认为tmp/'
    )
    parser.add_argument(
        '-d', dest='dist_lib', type=str, default='scdap',  # required=True,
        help='需要编译的目标, 可以是文件也可以是目录'
    )
    parser.add_argument(
        '-k', dest='ksize', type=int, default=8,
        help='多线程编译中线程的数量, 默认为8'
    )
    return parser.parse_args()


def do_dir(dist_dir: str, fdir: str) -> list:
    cmds = list()
    for root, dirs, files in os.walk(fdir):
        for file in files:
            if not file.endswith('py'):
                continue
            to_dir = os.path.join(dist_dir, root)
            from_file = os.path.join(root, file)
            # __init__.py不能够编译
            # 否则运行会出错
            # 所以直接进行复制
            if file.startswith('__init__'):
                if not os.path.exists(to_dir):
                    os.makedirs(to_dir, exist_ok=True)

                shutil.copy(from_file, os.path.join(to_dir, file))

                continue

            cmd = CMD.format(to_dir, from_file)
            # print(cmd)
            cmds.append(cmd)
        for d in fdir:
            if d in {'.idea', '__pycache__'}:
                continue
            cmds += do_dir(dist_dir, d)
    return cmds


def do_compile(cmd, log):
    # print(cmd)
    stdout = subprocess.getstatusoutput(cmd)
    log(cmd, '\n', *stdout)


def do_log(queue: Queue, stop_flag: list):
    while stop_flag[0]:
        message = queue.get()
        print(*message)


def put_message(queue, *message):
    queue.put(message)


def main():
    parser = parse_arg()
    cmds = do_dir(parser.dist_dir, parser.dist_lib)
    queue = Queue()
    log = partial(put_message, queue)
    stop_flag = [1]
    log_thread = Thread(target=do_log, args=(queue, stop_flag))
    log_thread.start()
    pool = ThreadPoolExecutor(parser.ksize)
    [pool.submit(do_compile, cmd, log) for cmd in cmds]
    pool.shutdown()
    stop_flag[0] = 0
    log('stop log.')
    log_thread.join()


if __name__ == '__main__':
    main()
