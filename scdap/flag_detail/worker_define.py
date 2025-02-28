"""

@create on: 2021.06.22
"""
from scdap.frame.worker import get_worker_names
from scdap.flag_detail.utils import update_info


class WorkerDefine(object):
    def get_defines(self):
        return dict(zip(get_worker_names(), get_worker_names()))

    @property
    def details(self):
        return [
            {'worker_name': worker}
            for worker in get_worker_names()
        ]


def main():
    data = dict()
    for name in get_worker_names():
        data[name] = {'worker_name': name}
    update_info(__package__, '/worker-define/', data)


if __name__ == '__main__':
    main()
