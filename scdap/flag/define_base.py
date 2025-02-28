"""

@create on: 2021.01.25

"""
from threading import Lock
from inspect import isfunction


class DefineBase(object):
    """
    定义基类

    class SomethingDefine(DefineBase):
        define1 = 1
        define2 = 2
        ...

    """
    __lock__ = Lock()
    __itemname2val__: dict
    __val2itemname__: dict
    __item_type__ = None
    __itemname2index__: dict

    __exclude__ = {
        '__module__', '__main__', '__doc__', '__dict__', '__weakref__',
        '__item_type__', '__exclude__', '__lock__', '__instance__',
        '__itemname2val__', '__val2itemname__'
    }

    def __new__(cls, *args, **kwargs):
        if cls.__item_type__ is None:
            raise Exception(f'请配置{cls.__name__}的__item_type__.')
        instance = getattr(cls, "__instance__", None)
        if instance:
            return instance

        with cls.__lock__:
            instance = getattr(cls, "__instance__", None)
            if instance:
                return instance
            instance = cls._create_instance()
            setattr(cls, "__instance__", instance)
            return instance

    @classmethod
    def _create_instance(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)

        instance.__itemname2val__ = dict()
        instance.__val2itemname__ = dict()
        instance.__itemname2index__ = dict()

        for key, val in cls.__dict__.items():
            if key in instance.__exclude__ or isfunction(val):
                continue

            if key.startswith('_') or key.endswith('_'):
                raise AttributeError(f'{type(instance).__name__}中的{key}配置的格式错误, 参数: [{val}]不允许(_)为开头或结尾.')

            if not isinstance(val, cls.__item_type__):
                raise TypeError(
                    f'{type(instance).__name__}中的{key}配置的格式错误, 参数: [{val}] 的类型必须为: {cls.__item_type__}'
                )

            if val in instance.__val2itemname__:
                raise ValueError(
                    f'{type(instance).__name__}中的{key}配置的格式错误, '
                    f'参数: [{val}] 已经登记过: [{instance.__val2itemname__[val]}]'
                )
            instance.__val2itemname__[val] = key
            instance.__itemname2val__[key] = val
            instance.__itemname2index__[key] = len(instance.__itemname2index__)

        return instance

    def get_defines(self):
        return self.__itemname2val__.copy()

    def get_val_by_itemname(self, name: str):
        return self.__itemname2val__[name]

    def get_itemname_by_val(self, val) -> str:
        return self.__val2itemname__[val]

    def get_index_by_itemname(self, name: str) -> int:
        return self.__itemname2index__[name]

    def has_val(self, val) -> bool:
        return val in self.__val2itemname__

    def has_name(self, name) -> bool:
        return name in self.__itemname2val__
