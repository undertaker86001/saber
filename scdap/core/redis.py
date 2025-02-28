"""

@create on: 2021.05.25
"""
from typing import List, Union, Optional

import redis
from scdap import config


class RedisAPI(object):
    def __init__(self, host: str = None, port: int = None, password: str = None, db: int = None, prefix_key: str = None):
        host = config.REDIS_HOST if host is None else host
        port = config.REDIS_PORT if port is None else port
        password = config.REDIS_PASSWORD if password is None else password
        db = config.REDIS_DB if db is None else db
        self._prefix_key = config.REDIS_PREFIX_KEY if prefix_key is None else prefix_key
        self._pool = redis.ConnectionPool(max_connections=1, host=host, port=port, db=db, password=password)

    def parse_key(self, keys: Union[str, List[str]]) -> str:
        """
        拼接key为redis使用的格式
        一般redis以":"作为分隔符

        :param keys:
        :return:
        """
        if isinstance(keys, str):
            keys = [keys]
        return ':'.join([self._prefix_key] + keys)

    def get_connector(self) -> redis.Redis:
        """
        获取redis连接器, 对于其他不提供接口的操作可以使用该接口获取后自行操作

        :return:
        """
        return redis.Redis(connection_pool=self._pool)

    def exists(self, key: str) -> bool:
        return self.get_connector().exists(key)

    def delete(self, *keys) -> bool:
        return self.get_connector().delete(*keys)

    def setex(self, key: str, value: str, ttl: int = None) -> bool:
        """
        强制设置键值为key的位置的数值为value, 不管键值为key的是否存在
        ttl为该数值在redis中保存的时长, 如果为None则代表永久保存

        :param key:
        :param value:
        :param ttl:
        :return:
        """
        return self.get_connector().setex(key, ttl, value)

    def setnx(self, key: str, value: str, ttl: int = None) -> bool:
        """
        只有在key值不存在的时候才会设置数值

        :param key:
        :param value:
        :param ttl:
        :return:
        """
        return self.get_connector().set(key, value, ex=ttl, nx=True)

    def setxx(self, key: str, value: str, ttl: int = None) -> bool:
        """
        只有在key值存在的时候才会设置数值

        :param key:
        :param value:
        :param ttl:
        :return:
        """
        return self.get_connector().set(key, value, ex=ttl, xx=True)

    def get(self, key: str) -> Optional[str]:
        """
        获取键的数值

        :param key:
        :return:
        """
        val = self.get_connector().get(key)
        if val is None:
            return None
        return val.decode('utf-8')
