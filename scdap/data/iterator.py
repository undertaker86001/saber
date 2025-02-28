"""

@create on 2020.02.25

迭代器基类
"""
from collections import deque
from typing import TypeVar, Generator, Iterable, Generic, Reversible

T = TypeVar('T')


class Iterator(Generic[T]):
    """
    迭代器基类
    该迭代器主要用于数据容器的操作
    其内置指针用于指定当前迭代器位置
    使用next()方法可推进指针向前移动以获取数据
    使用方法：
    while iterator.next():
        iterator.get_xxx()
        ...
    """

    def __init__(self, array: Iterable[T] = (), maxlen: int = None):
        self._data = deque(array, maxlen=maxlen)
        self._position = -1

    @property
    def data(self):
        """
        必须存在该参数, 因为iterator在解析的时候会使用到
        """
        return self._data

    def get(self, index: int = None) -> T:
        index = self._position if index is None else index
        return self[index]

    def get_range(self, start: int = None, stop: int = None) -> Generator[T, None, None]:
        if start is None:
            start = 0
        if start < 0:
            raise IndexError('start必须大于等于0.')
        if stop is None:
            stop = len(self._data)
        if stop > len(self._data):
            raise IndexError('stop超过指定范围.')
        if start >= stop:
            raise IndexError('start必须大于stop')
        for i in range(start, stop):
            yield self._data[i]

    def __str__(self):
        return f'size: {self.size()}, maxlen: {self._data.maxlen}, position: {self._position}'

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> {self.__str__()}]'

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return self._data.__iter__()

    def __getitem__(self, index: int) -> T:
        if index < 0:
            raise IndexError('index必须大于等于0')
        return self._data[index]

    def __setitem__(self, index: int, value: T):
        if index < 0:
            raise IndexError('index必须大于等于0')
        self._data[index] = value

    def __delitem__(self, index: int):
        # 如果需要删除某一个index的数据
        # 则将根据index与position的相对位置移动position
        # 拥有如下几种情况
        # 1. index < 0          -> 禁止的操作
        # 2. position == -1     -> 不动
        # 3. position < index   -> 不动
        # 4. position == index  -> position向前移动一格
        # 5. position > index   -> position向前移动一格
        # 可以归纳为:
        # position >= index     -> position -= 1
        if index < 0:
            raise IndexError('删除的index必须大于等于0.')
        del self._data[index]
        if self._position >= index:
            self._position -= 1

    def popleft(self) -> T:
        """
        popleft删除最左边的数据(index=0)
        position == -1      -> 不动
        position >= 0       -> position向左移动一格
        """
        val = self._data.popleft()
        if self._position >= 0:
            self._position -= 1
        return val

    def pop(self) -> T:
        """
        pop删除最右边的数据(index=len(data) - 1)
        position == -1      -> 不动
        position >= 0       -> position向左移动一格
        """
        val = self._data.pop()
        if self._position >= 0:
            self._position -= 1
        return val

    def clear(self):
        self._data.clear()
        self._position = -1

    def extend(self, it: Iterable[T]):
        self._data.extend(it)

    def extendleft(self, iter_obj: Reversible[T]):
        """
        a: [0, 1, 2, 3]
        b: [0, 1, 2, 3]
        a.extendleft(b) = [0, 1, 2, 3, 0, 1, 2, 3]
        """
        # deque的extendleft操作不一样, 所以需要加上reversed(it)
        self._data.extendleft(reversed(iter_obj))

    def append(self, o: T):
        self._data.append(o)

    def appendleft(self, o: T):
        self._data.appendleft(o)

    def _get_simple(self, key: str, index: int = None):
        return getattr(self._data[self._position if index is None else index], key)

    def _get_all_generator(self, key: str, start: int = None, stop: int = None) -> Generator:
        for f in range(start or 0, stop or len(self._data)):
            yield getattr(self._data[f], key)

    def _get_all(self, key: str, start: int = None, stop: int = None) -> list:
        return list(self._get_all_generator(key, start, stop))

    def _set_simple(self, key: str, o: object, index: int = None):
        setattr(self._data[self._position if index is None else index], key, o)

    def empty(self) -> bool:
        return len(self._data) == 0

    def size(self) -> int:
        return len(self._data)

    def get_position(self) -> int:
        """
        获取指针位置

        :return: 当前指针位置
        """
        return self._position

    def next(self) -> bool:
        """
        每次一步移动指针

        :return: 指针是否处在最后的位置
        """
        if self.position_at_the_end():
            return False
        self._position += 1
        return True

    def position_at_the_start(self) -> bool:
        """
        指针是否在起始位置

        :return: 指针是否处在最前端
        """
        return self._position == -1

    def position_at_the_end(self) -> bool:
        """
        指针是否位于最后

        :return: 指针是否处在最后段位置
        """
        return self._position >= len(self._data) - 1

    def position_to_end(self):
        """
        将位置指针移动至最后
        """
        self._position = len(self._data) - 1

    def position_to_start(self):
        """
        重置位置指针
        """
        self._position = -1

    def set_position(self, index: int):
        """
        设置指针的位置

        :param index: 指针位置
        """
        if index < -1 or index >= len(self._data):
            raise IndexError('index必须大于-1且小于size')
        self._position = index

    def reset_position(self):
        """
        重置index指针
        """
        self.position_to_start()
