"""

@create on: 2020.12.07
"""
import math
from datetime import datetime
from threading import Lock
from contextlib import contextmanager
from typing import ContextManager, Any, Dict, List, Union, Iterable, Optional, Tuple, Type

from sqlalchemy.sql import func
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.dialects.mysql import BIGINT, TINYINT, VARCHAR, DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy import Column, text, create_engine

from .common_flag import CommonFlag

# 数据库表结构基类
# 所有表都必须基础自该类
Base = declarative_base()

__engine__ = None
__session_class__ = None
__engine_is_initialed__ = False
__engine_lock__ = Lock()


def initial_engine():
    global __session_class__, __engine_is_initialed__, __engine__

    if __engine_is_initialed__:
        return

    with __engine_lock__:
        if __engine_is_initialed__:
            return

        from scdap import config

        engine = create_engine(
            f'mysql+pymysql://{config.MYSQL_USER}:{config.MYSQL_PASSWORD}@'
            f'{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_SCDAP_DATABASE}',
            poolclass=NullPool
        )
        Base.bind = engine
        # scoped_session 允许使用多线程
        __session_class__ = scoped_session(sessionmaker(engine))
        __engine__ = engine
        __engine_is_initialed__ = True


class DapBaseItem(object):
    """
    sucheon_server_scdap中所有表都拥有的基础字段
    """
    id = Column(CommonFlag.id, BIGINT(20), primary_key=True, comment='唯一主键')
    create_time = Column(CommonFlag.create_time, DATETIME, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    update_time = Column(
        CommonFlag.update_time, DATETIME, nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    )
    enabled = Column(CommonFlag.enabled, TINYINT(1), nullable=False, server_default=text("'1'"))
    description = Column(CommonFlag.description, VARCHAR(255), nullable=False, server_default=text("''"))

    def __init__(self, *args, **kwargs):
        pass


def check_type(val, name: str, type_list: Union[Type[Any], Tuple[Type[Any], ...]], allow_none: bool = False):
    if allow_none and val is None:
        return
    if not isinstance(val, type_list):
        raise TypeError(f'参数: {name}错误的类型错误, 类型必须为: {type_list}')


class TableApi(object):
    """
    数据表结构接口
    """
    __lock__ = Lock()

    def __init__(self, item_class: Base, auto_create_table: bool = True):
        self._is_initialed = False
        self._metadata = Base.metadata
        self._columns = list()
        self._select_columns = list()
        self._select_columns_name = list()
        self._item_class = item_class
        self._auto_create_table = auto_create_table
        self._table = self._metadata.tables[self.get_tablename()]
        self._columns = self.colums_to_str(self._table.columns)
        self.set_select_columns()

    def get_select_columns(self) -> List[Column]:
        """
        获取在索引获取数据时需要获取的字段

        :return: 字段列表
        """
        return self._select_columns

    def get_select_columns_name(self) -> List[str]:
        """
        获取在索引获取数据时需要获取的字段的名称

        :return: 字段名称列表
        """
        return self._select_columns_name

    def select_data_to_dict(self, data: List) -> dict:
        """
        将获取的字段数据打包成字典

        :param data: 数据, 注意获取的数据必须时根据索引字段获取的

        :return: 结果
        """
        return dict(zip(self.get_select_columns_name(), data))

    def get_max_unique_id(self, unique_key, default: int = 0, session=None) -> int:
        with self.get_session(False, False, session) as session:
            resp = session.query(func.max(unique_key)).first()[0]
        return default if resp is None else resp

    def set_select_columns(self, has_id: bool = False, has_update_time: bool = False, has_create_time: bool = False,
                           has_description: bool = True, has_enabled: bool = True):
        """
        获取表存在的所有字段
        """
        self._select_columns = [getattr(self._item_class, name) for name in self._columns]
        if not has_id:
            self._select_columns.remove(self._item_class.id)
        if not has_update_time:
            self._select_columns.remove(self._item_class.update_time)
        if not has_create_time:
            self._select_columns.remove(self._item_class.create_time)
        if not has_description:
            self._select_columns.remove(self._item_class.description)
        if not has_enabled:
            self._select_columns.remove(self._item_class.enabled)
        self._select_columns_name = self.colums_to_str(self._select_columns)

    def get_tablename(self) -> str:
        """

        :return: 表名称
        """
        return self._item_class.__tablename__

    def to_dict(self, data: Union[Base, List[Base]]) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        将数据结果转换为字典

        :param data: 带转换数据

        :return: 字典
        """
        if isinstance(data, Iterable):
            return [self.to_dict(d) for d in data]
        else:
            result = dict()
            for col in self._columns:
                if hasattr(data, col):
                    result[col] = getattr(data, col)
            return result

    def colums_to_str(self, column: Union[Column, List[Column]]) -> Union[str, List[str]]:
        """
        将Column转换成字符串

        :param column: 带转换的Column

        :return: 名称
        """
        if isinstance(column, Iterable):
            return [self.colums_to_str(c) for c in column]
        else:
            return column.name.split('.')[-1]

    def initial(self):
        # 创建数据库
        # 如果数据库存在则不删除
        if self._is_initialed:
            return

        initial_engine()

        with self.__lock__:
            if self._is_initialed:
                return

            self._metadata.bind = __engine__

            if not database_exists(__engine__.url):
                create_database(__engine__.url)

            if self._auto_create_table and not self._table.exists():
                self._metadata.create_all(tables=[self._table], checkfirst=True)

            self._is_initialed = True

    def drop_table(self):
        with self.__lock__:
            if self._table.exists():
                self._metadata.drop_all(tables=[self._table], checkfirst=True)

    def create_table(self):
        with self.__lock__:
            if not self._table.exists():
                self._metadata.create_all(tables=[self._table], checkfirst=True)

    def table_update_time(self):
        with self.get_session(False) as session:
            query = f"SELECT" \
                    "   UPDATE_TIME" \
                    "FROM" \
                    "   information_schema.Tables" \
                    "WHERE" \
                    "   TABLE_NAME = '%s'" \
                    "   AND TABLE_SCHEMA = '%s'" \
                    % (self.get_tablename(), __engine__.url.database)
            data = session.execute(query).fetchall()
            if not data:
                time = datetime.fromtimestamp(0)
            else:
                time = data[0][0] or datetime.fromtimestamp(0)
            return time.timestamp()

    @contextmanager
    def get_session(self, auto_commit: bool = True, auto_rollback: bool = True, session=None) \
            -> ContextManager[Session]:
        """
        上下文管理器, 用于调用session对数据库表进行操作
        上下文管理器会自动提交

        :param auto_commit: 是否自动提交

        :param auto_rollback: 是否在操作出现异常的时候自动回滚

        :param session:

        :return: Session
        """
        # 只在需要操作数据库的时候才进行初始化
        # 因为数据库的参数是随时可变的而定义的方法则是在import的时候就会调用的
        # 故需要留到最后时刻才初始化
        if session is not None:
            yield session
            return

        self.initial()
        session = __session_class__()
        try:
            yield session
            if auto_commit:
                session.commit()
        except:
            if auto_rollback:
                session.rollback()
            raise
        finally:
            session.close()

    def add(self, instance: Union[dict, List[dict]], session=None):
        """
        新增数据至数据库中

        :param instance: 数据

        :param session:
        """
        if not isinstance(instance, (list, tuple)):
            instance = [instance]

        with self.get_session(session=session) as session:
            session.add_all([self._item_class(**i) for i in instance])

    def _combine_filter(self, unique: InstrumentedAttribute, unique_val: Union[Any, List[Any], None] = None,
                        enabled: Optional[bool] = None, extra_filter_list: List[ColumnElement] = None,
                        can_none: bool = True):

        if not can_none and unique_val is None:
            ValueError(f'unique_val不允许为None.')

        filter_list = list()

        if isinstance(unique_val, (str, int)):
            filter_list = [unique == unique_val]
        elif isinstance(unique_val, (list, tuple)):
            filter_list.append(unique.in_(unique_val))
        elif not can_none:
            raise TypeError('unique_val的类型必须为list/tuple/str/int.')

        filter_list += (extra_filter_list or list())

        if enabled is not None:
            filter_list.append(self._item_class.enabled == enabled)
        return filter_list

    def query_with_unique(self, unique, unique_val: Union[str, int, List[Union[str, int]], None] = None,
                          enabled: Optional[bool] = None, extra_filter_list: List[ColumnElement] = None,
                          order_by_list: List[ColumnElement] = None, limit: int = None, session=None) \
            -> Optional[dict]:
        """

        :param unique: 查询的主键名称
        :param unique_val: unique主键的数值
        :param extra_filter_list: 其他filter查询操作符列表
        :param order_by_list: 排序操作
        :param enabled: 是否只查询启用的数据
        :param limit: 限制的查询数量
        :param session:
        :return: 结果
        """

        filter_list = self._combine_filter(unique, unique_val, enabled, extra_filter_list)

        with self.get_session(False, False, session) as session:
            query = session.query(*self.get_select_columns())
            query = query.filter(*filter_list)

            if order_by_list:
                query = query.order_by(*order_by_list)

            if limit is not None and limit > 0:
                query.limit(limit)

            result = list(map(self.select_data_to_dict, query.all()))

        if isinstance(unique_val, (str, int)):
            if result:
                return result[0]
            else:
                return None

        key = unique.name
        return {r[key]: r for r in result}

    def delete_with_unique(self, unique: InstrumentedAttribute, unique_val: Union[str, int, List[Union[str, int]]],
                           enabled: Optional[bool] = None, extra_filter_list: List[ColumnElement] = None, session=None):
        """

        :param unique: 查询的主键名称
        :param unique_val: unique主键的数值
        :param enabled: 是否只查询启用的数据
        :param extra_filter_list: 其他filter查询操作符列表
        :param session:
        """
        filter_list = self._combine_filter(unique, unique_val, enabled, extra_filter_list, True)
        with self.get_session(session=session) as session:
            session.query(self._item_class).filter(*filter_list).delete(synchronize_session=False)

    def update_with_unique(self, unique: InstrumentedAttribute, unique_val: Union[str, int], update_kv: Dict[str, Any],
                           enabled: Optional[bool] = None, extra_filter_list: List[ColumnElement] = None, session=None):
        """
        通用的数据更新接口，在表结构中有且只有一个unique的时候可以使用

        :param unique: 查询的主键名称
        :param unique_val: unique主键的数值
        :param enabled: 是否只查询启用的数据
        :param extra_filter_list: 其他filter查询操作符列表
        :param update_kv: 需要更新的内容
        :param session:
        """
        if len(update_kv) <= 0:
            return

        filter_list = self._combine_filter(unique, unique_val, enabled, extra_filter_list, True)

        with self.get_session(session=session) as session:
            session.query(self._item_class).filter(*filter_list).update(update_kv)

    def check_exist_with_unique(self, unique: InstrumentedAttribute,
                                unique_val: Union[List[Union[str, int]], Union[str, int]],
                                enabled: Optional[bool] = None, last_one: bool = False,
                                extra_filter_list: List[ColumnElement] = None,
                                session=None) -> bool:
        """
        通用的确认某一个unique字段是否存在的接口

        :param unique: 查询的主键名称
        :param unique_val: unique字段数值
        :param enabled: 是否只查询启用的数据
        :param last_one: True->只要一个存在就返回True, False->只有所有点位都存在才返回True
        :param extra_filter_list: 其他索引操作符
        :param session:
        :return: 是否存在
        """
        filter_list = self._combine_filter(unique, unique_val, enabled, extra_filter_list)
        if isinstance(unique_val, (str, int)):
            size = 1
        else:
            size = len(unique_val)
        with self.get_session(False, False, session) as session:
            query_size = session.query(unique).filter(*filter_list).count()

        if last_one:
            return query_size > 1
        else:
            return query_size == size

    def get_exist_keys_with_unique(self, unique: InstrumentedAttribute, enabled: Optional[bool] = None,
                                   extra_filter_list: List[ColumnElement] = None,
                                   order_by_list: List[ColumnElement] = None,
                                   session=None) -> list:
        """
        获取所有存在的id

        :param unique: 需要查询的字段
        :param enabled: 是否获取启用的配置
        :param extra_filter_list: 其他索引操作符
        :param order_by_list: 排序
        :param session:
        :return: 存在的id列表
        """
        filter_list = self._combine_filter(unique, None, enabled, extra_filter_list, True)

        with self.get_session(False, False, session) as session:
            query = session.query(unique)
            if filter_list:
                query = query.filter(*filter_list)
            if order_by_list:
                query = query.order_by(*order_by_list)
            data = query.all()

        return list(map(lambda d: d[0], data))

    def query_with_none(self, enabled: Optional[bool] = None, filter_list: List[ColumnElement] = None,
                        order_by_list: List[ColumnElement] = None, limit: int = None, session=None) -> List[dict]:
        """
        通用的查询接口, 在表结构中没有unique的情况下可以使用

        :param filter_list: 其他filter查询操作符列表
        :param order_by_list: 排序操作
        :param enabled: 是否只查询启用的数据
        :param limit: 限制的查询数量
        :param session:
        :return: 结果
        """

        if enabled is not None:
            filter_list.append(self._item_class.enabled == enabled)

        with self.get_session(False, False, session) as session:
            query = session.query(*self.get_select_columns()).filter(*filter_list)
            if order_by_list:
                query = query.order_by(*order_by_list)
            if limit is not None and limit > 0:
                query = query.limit(limit)
            return [self.select_data_to_dict(data) for data in query.all()]

    def list(self, page_size: int = 0, page_num: int = 1, enabled: Optional[bool] = None,
             filter_list: List[ColumnElement] = None, order_by_list: List[ColumnElement] = None,
             session=None) -> Tuple[int, int, List[dict]]:
        """
        查询目标数据列表, 主要适用于后端的分页操作

        :param page_size: 页面数据条数
        :param page_num: 当前页数
        :param enabled:
        :param filter_list:
        :param order_by_list:
        :param session:
        :return: 总数据量, 总页面量, 查询到的数据
        """

        if enabled is not None:
            filter_list.append(self._item_class.enabled == enabled)

        with self.get_session(False, False, session) as session:
            query = session.query(*self.get_select_columns())
            if filter_list:
                query = query.filter(*filter_list)
            if order_by_list:
                query = query.order_by(*order_by_list)
            size = query.count()

            if page_size == 0:
                data = query.all()
            else:
                page_count = math.ceil(size / page_size)
                page_num = max(1, min(page_count, page_num))
                data = query[(page_num - 1) * page_size: page_num * page_size]
        return size, page_count, list(map(self.select_data_to_dict, data))
