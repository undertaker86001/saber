"""

@create on: 2021.05.20
"""


def check_function(obj_class, fname):
    try:
        getattr(obj_class, fname)
    except:
        raise Exception(f'无法找到{obj_class}.{fname}, 请确保实现了该接口.')


def check_value(obj_class, name):
    try:
        getattr(obj_class, name)
    except:
        raise Exception(f'无法找到{obj_class}.{name}, 请确保实现了该变量.')


def check_default(obj_class):
    if set(obj_class.__default__.keys()) != set(obj_class.__slots__):
        raise Exception(f'{obj_class}.__default__.keys()与{obj_class}.__slots__不相同, '
                        f'请确保__default__配置的key与__slots__相同.')


def check_slot(i_class, obj_class):
    if set(i_class.__slots__) != set(obj_class.__slots__):
        raise Exception(f'{i_class}.__slots__与{obj_class}.__slots__不相同, '
                        f'请确保两个类配置的__slots__也就是特征相同切能够一一对应.')
