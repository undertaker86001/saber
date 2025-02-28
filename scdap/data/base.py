"""

@create on: 2020.12.29
"""
from copy import deepcopy
from typing import Generic, TypeVar, List, Generator, Type


class RefItem(object):
    """
    RefItem是单一特征的object不允许单独存在, 必须与ItemList一同存在且由ItemList创建
    其类似于一个列表的指针, ItemList在初始化的时候传入自身保存的数据列表object以及该item位于整个数据列表中的位置
    在调用item内部某一个特征的时候本质上是调用传入的list位于某一个key的列表指定位置的数值

    class list(object):
        meanhf: [1, 2, 3]
        meanlf: [4, 5, 6]

    item_keys: ['meanhf', 'meanlf']

    假设创建一个
    f = RefItem(list, item_keys, 0)
    f.meanhf -> 1
    f.meanlf -> 4
    f.mean -> AttributeError

    f = RefItem(list, item_keys, 1)
    f.meanhf -> 2
    f.meanlf -> 5
    f.mean -> AttributeError

    """
    __slots__ = ['_position', '_list', '_select_keys']

    def select_keys(self) -> tuple:
        """
        获取item配置的特征或者是可用的变量名

        :return:
        """
        return self._select_keys

    # 默认数值
    # 如果配置了keys的同时但是初始化却没有数值需要填默认数值
    # 该参数则是配置默认数值
    # 由ItemList进行调用与数值配置
    __default__ = dict()

    def __init__(self, item_list, select_keys: tuple, position: int):
        self._select_keys = select_keys
        self._list = item_list
        self._position = position

    def __getattr__(self, item):
        if item in self.__slots__:
            # 比如self._item_list中存在变量meanhf = list()
            # self.meanhf -> self._item_list.meanhf[self._position]
            try:
                return getattr(self._list, item)[self._position]
            except AttributeError as e:
                raise AttributeError(f'{type(self).__name__}只配置下列特征: '
                                     f'{self.select_keys()}, 请勿使用未配置的特征: {e}.')
        return super().__getattr__(item)

    def __setattr__(self, key, value):
        # 比如self._item_list中存在变量meanhf = list()
        # self.meanhf = 1 -> self._item_list.meanhf[self._position] = 1
        if key in self.__slots__:
            try:
                getattr(self._list, key)[self._position] = value
            except AttributeError as e:
                raise AttributeError(f'{type(self).__name__}只配置下列特征: '
                                     f'{self.select_keys()}, 请勿使用未配置的特征: {e}.')

        return super().__setattr__(key, value)

    def __str__(self) -> str:
        return f', '.join([f'{k}:{getattr(self, k)}' for k in self.select_keys()])

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> {self.__str__()}]'


class ICollection(object):
    def __init__(self, select_keys: list = None):
        # 动态创建特征列表
        # 只创造需要的特征列表
        for key in (select_keys or self.__slots__):
            setattr(self, key, list())


T = TypeVar('T', bound=RefItem)


class ItemList(Generic[T]):
    def __init__(self, obj_class: Type[T], item_list, maxlen: int = None, select_keys: List[str] = None):
        self._position: int = -1
        self._size: int = 0
        self._maxlen = maxlen
        self._select_keys = tuple(select_keys or list())
        self._item_list = item_list
        self._obj_class = obj_class

    def item_list(self):
        return self._item_list

    def select_keys(self) -> tuple:
        return self._select_keys

    def size(self):
        return self._size

    def maxlen(self):
        return self._maxlen

    def sub_itemlist(self, start: int = None, stop: int = None):
        pass

    def __str__(self):
        return f'size: {self._size}, maxlen: {self._maxlen}, position: {self._position}'

    def __repr__(self):
        return f'[<{type(self).__name__}: {hex(id(self))}> {self.__str__()}]'

    def __len__(self) -> int:
        return self._size

    def __getitem__(self, index: int) -> T:
        if index < 0:
            raise IndexError('index必须大于等于0.')
        return self.get_ref(index)

    def generator(self) -> Generator[T, None, None]:
        """
        调用一个迭代生成器, 逐个返回创建一个refitem并且返回
        for ref in generator():
            xxx = ref.xxxx
            ...

        :return:
        """
        for i in range(self._size):
            yield self.get_ref(i)

    def get_value(self, name: str, index: int = None):
        """
        获得某一个字段在index位置的数据
        index=None则默认返回内置的position所处的位置

        :param name:
        :param index:
        :return:
        """
        if index is None and self._position < 0:
            raise IndexError('请在调用next()等移动position接口后再使用get_value()')

        index = self._position if index is None else index
        try:
            return getattr(self._item_list, name)[index]
        except AttributeError as e:
            raise AttributeError(f'{type(self).__name__}只配置下列字段: {self.select_keys()}, 请勿使用未配置的字段: {e}.')
        except IndexError:
            raise IndexError(f'{type(self).__name__}的当前size={self.size()}, 请勿传入获取超出范围的index={index}.')
        except:
            raise

    def get_ref(self, index: int = None) -> T:
        index = self._position if index is None else index
        return self._obj_class(self._item_list, self._select_keys, index)

    def get_last_ref(self) -> T:
        return self._obj_class(self._item_list, self._select_keys, self._size - 1)

    def get_range(self, name: str, start: int = None, stop: int = None):
        if start is None:
            start = 0
        if stop is None:
            stop = self._size

        if start == stop:
            return []

        try:
            return getattr(self._item_list, name)[start:stop]
        except AttributeError as e:
            raise AttributeError(f'{type(self).__name__}只配置下列特征: {self.select_keys()}, 请勿使用未配置的特征: {e}.')
        except:
            raise

    def remove(self, index: int):
        for slot in self._select_keys:
            del getattr(self._item_list, slot)[index]
        self._size -= 1
        if self._position >= index:
            self._position -= 1

    def remove_range(self, start: int = None, stop: int = None):
        """
        删除范围为[start, stop)的数据

        :param start:
        :param stop:
        :return:
        """
        if start is None:
            start = 0
        if stop is None:
            stop = self._size

        if start > stop:
            raise IndexError('start必须小于stop.')
        elif start == stop:
            return

        for slot in self._select_keys:
            del getattr(self._item_list, slot)[start:stop]
        size = stop - start
        self._size -= size

        # 移动position指针
        # 通过举例, 有下列几种情况:
        # arr  =  [0, 1, 2, 3, 4, 5, 6, 7]
        # index =  0, 1, 2, 3, 4, 5, 6, 7
        # start, stop = (3, 5)
        # 上述范围将移除 3, 4这两个位置的数值
        # arr = [0, 1, 2, 5, 6, 7]
        # 主要差别在于position的位置, 根据position的位置将会有不同的处理方法
        # 1. position < start
        #    -> 移除的数据位置不会干扰到position, 所以position无需做任何偏移与处理
        # 2. start <= position < stop
        #    -> position的位置处在移除的数据范围内, 则将偏移position为start - 1的位置
        # 3. stop <= position
        #    -> position等待位置虽然不在移除的数据范围内, 但是移除的数据依旧会干扰到position的位置判定, 需要将position偏移 stop - start 长度
        if self._position >= stop:
            self._position -= size
        elif self._position >= start:
            self._position = start - 1

    def pop(self):
        self.remove(0)

    def clear(self):
        for slot in self._select_keys:
            getattr(self._item_list, slot).clear()
        self._size = 0
        self._position = -1

    def _drop_left(self):
        if self._maxlen and self._size > self._maxlen:
            delete_size = self._size - self._maxlen
            for slot in self._select_keys:
                del getattr(self._item_list, slot)[:delete_size]
            self._size = self._maxlen

    def extend_itemlist(self, item_list):
        for key in self._select_keys:
            getattr(self._item_list, key).extend(getattr(item_list.item_list(), key))
        self._size += item_list.size()
        self._drop_left()

    def extend_ldict(self, **kwargs):
        """
        kwargs: {
            "key1": [...],
            "key2": [...],
            ...
        }

        :param kwargs:
        :return:
        """
        size = 0
        for key in self._select_keys:
            val = kwargs[key]
            getattr(self._item_list, key).extend(val)
            size = len(val)
            if size and len(val) != size:
                raise Exception(f'{type(self)}.extend_list()接口传入了长度不同的数据.')
        self._size += size
        self._drop_left()

    def append_item(self, item: T):
        for key in self._select_keys:
            getattr(self._item_list, key).append(getattr(item, key))
        self._size += 1
        self._drop_left()

    def append_dict(self, **kwargs):
        """
        kwargs: {
            "key1": val,
            "key2": vla,
            ...
        }

        :param kwargs:
        :return:
        """
        datas = deepcopy(self._obj_class.__default__)
        datas.update(kwargs)
        for key in self._select_keys:
            getattr(self._item_list, key).append(datas[key])
        self._size += 1
        self._drop_left()

    def empty(self) -> bool:
        return self._size == 0

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
        return self._position >= self._size - 1

    def position_to_end(self):
        """
        将位置指针移动至最后
        """
        self._position = self._size - 1

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
        if index < -1 or index >= self._size:
            raise IndexError('index必须大于-1且小于size')
        self._position = index

    def reset_position(self):
        """
        重置index指针
        """
        self.position_to_start()
