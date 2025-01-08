#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyRedisClient",
    "LazyRedisStandalone",
    "LazyRedisSentinel",
]

from abc import ABC, abstractmethod
from typing import List, Optional, Union

from libdata.common import ConnectionPool, LazyClient, ParsedURL


class AbstractRedisProxy(ABC):

    @abstractmethod
    def get_client(self, read=False):
        pass


class StringProxy(AbstractRedisProxy, ABC):

    def get(self, name: str):
        return self.get_client(read=True).get(name)

    def set(self, name: str, value):
        return self.get_client(read=False).set(name, value)

    def delete(self, *names: str):
        return self.get_client(read=False).delete(*names)

    def append(self, name: str, value):
        return self.get_client(read=False).append(name, value)

    def strlen(self, name: str):
        return self.get_client(read=True).strlen(name)

    def expire(self, name: str, time):
        return self.get_client(read=False).expire(name, time)


class ListProxy(AbstractRedisProxy, ABC):

    def lpush(self, name: str, *values) -> int:
        return self.get_client(read=False).lpush(name, *values)

    def rpush(self, name: str, *values) -> int:
        return self.get_client(read=False).rpush(name, *values)

    def lpop(self, name: str, count: Optional[int] = None) -> Union[str, List, None]:
        return self.get_client(read=False).lpop(name, count)

    def rpop(self, name: str, count: Optional[int] = None) -> Union[str, List, None]:
        return self.get_client(read=False).rpop(name, count)

    def lrange(self, name: str, start: int, end: int) -> list:
        return self.get_client(read=True).lrange(name, start, end)

    def llen(self, name: str) -> int:
        return self.get_client(read=True).llen(name)


class HashProxy(AbstractRedisProxy, ABC):

    def hset(
            self, name: str,
            key: Optional[str] = None,
            value: Optional[str] = None,
            mapping: Optional[dict] = None,
            items: Optional[list] = None
    ) -> int:
        return self.get_client(read=False).hset(name, key, value, mapping, items)

    def hget(self, name: str, key: str) -> Optional[str]:
        return self.get_client(read=True).hget(name, key)

    def hgetall(self, name: str) -> dict:
        return self.get_client(read=True).hgetall(name)

    def hdel(self, name: str, *keys: str):
        return self.get_client(read=False).hdel(name, *keys)

    def hlen(self, name: str) -> int:
        return self.get_client(read=True).hlen(name)


class SetProxy(AbstractRedisProxy, ABC):

    def sadd(self, name: str, *values):
        return self.get_client(read=False).sadd(name, *values)

    def smembers(self, name: str):
        return self.get_client(read=True).smembers(name)

    def srem(self, name: str, *values):
        return self.get_client(read=False).srem(name, *values)

    def sismember(self, name: str, value):
        return self.get_client(read=True).sismember(name, value)

    def scard(self, name: str):
        return self.get_client(read=True).scard(name)


class LazyRedisClient(StringProxy, ListProxy, HashProxy, ABC):

    @staticmethod
    def from_url(url: Union[str, List[str]]) -> "LazyRedisClient":
        url_list = [
            ParsedURL.from_string(item)
            for item in (url if isinstance(url, List) else url.split("\n"))
            if item
        ]
        if len(url_list) == 1 and ("service_name" not in url_list[0].params):
            url = url_list[0]
            return LazyRedisStandalone(
                database=url.database,
                hostname=url.hostname,
                port=url.port,
                username=url.username,
                password=url.password
            )
        else:
            sentinels = []
            service_name = None
            database = None
            username = None
            password = None
            for item in url_list:
                assert item.scheme in {"redis"}

                sentinels.append((item.hostname, item.port))
                if "service_name" in item.params:
                    service_name = item.params["service_name"]
                if item.database is not None:
                    database = item.database
                if item.username:
                    username = item.username
                if item.password:
                    password = item.password

            return LazyRedisSentinel(
                sentinels=sentinels,
                service_name=service_name,
                database=database,
                username=username,
                password=password
            )

    def keys(self, pattern: str = "*"):
        return self.get_client(read=True).keys(pattern)

    def exists(self, *names: str):
        return self.get_client(read=True).exists(*names)

    def type(self, name: str):
        return self.get_client(read=True).type(name)

    def pipeline(self, transaction=True):
        return self.get_client(read=False).pipeline(transaction)


class LazyRedisStandalone(LazyRedisClient, LazyClient):

    def __init__(
            self,
            database: int = None,
            hostname: str = None,
            port: int = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            connection_pool: Optional[ConnectionPool] = None,
            **kwargs
    ):
        super().__init__()
        self.database = database or 0
        self.hostname = hostname or "localhost"
        self.port = port or 6379
        self.username = username or "default"
        self.password = password
        self.kwargs = kwargs

        self._conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL
        self._conn_key = (self.hostname, self.port, self.username, self.database)

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool(DEFAULT_CONN_POOL_SIZE)

    def _connect(self):
        client = self._conn_pool.get(self._conn_key)
        if client is None:
            # noinspection PyPackageRequirements
            from redis import Redis
            client = Redis(
                host=self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                db=self.database,
                decode_responses=True,
                **self.kwargs
            )
        return client

    def _disconnect(self, client):
        client = self._conn_pool.put(self._conn_key, client)
        if client is not None:
            client.close()

    def get_client(self, read=False):
        return self.client


class LazyRedisSentinel(LazyRedisClient, LazyClient):

    def __init__(
            self,
            service_name: str,
            sentinels: List[tuple] = None,
            database: int = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            socket_timeout: int = 1,
            connection_pool: Optional[ConnectionPool] = None
    ):
        super().__init__()
        self.service_name = service_name
        self.sentinels = sentinels or [("localhost", 26379)]
        self.database = database or 0
        self.username = username or "default"
        self.password = password
        self.socket_timeout = socket_timeout

        self._conn_key = (*self.sentinels, self.username, self.database)
        self._conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool(DEFAULT_CONN_POOL_SIZE)

    def _connect(self):
        client = self._conn_pool.get(self._conn_key)
        if client is None:
            # noinspection PyPackageRequirements
            from redis import Sentinel
            sentinel = Sentinel(self.sentinels, socket_timeout=self.socket_timeout)
            kwargs = dict(
                service_name=self.service_name,
                db=self.database,
                username=self.username,
                password=self.password
            )
            master = sentinel.master_for(**kwargs)
            slave = sentinel.slave_for(**kwargs, decode_responses=True)
            client = (master, slave)
        return client

    def _disconnect(self, client):
        client = self._conn_pool.put(self._conn_key, client)
        if client is not None:
            master, slave = client
            master.close()
            slave.close()

    def get_client(self, read=False):
        return self.client[int(read)]
