#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "DocReader",
    "DocWriter",
    "LazyClient",
    "ConnectionPool",
]

import abc
import importlib
from collections import defaultdict, deque
from threading import Lock
from typing import Generic, Optional, TypeVar, Union

from libdata.url import URL

ITERATORS = {
    "mongo": "libdata.mongodb.MongoIterator",
    "mongodb": "libdata.mongodb.MongoIterator",
    "mysql": "libdata.mysql.MySQLIterator",
}


class DocIterator(abc.ABC):
    """Abstract class for document iterator."""

    @classmethod
    def from_url(cls, url: Union[str, URL]) -> "DocIterator":
        url = URL.ensure_url(url)

        if not (name := ITERATORS.get(url.scheme)):
            raise ValueError(f"Unsupported type \"{url.scheme}\".")

        try:
            module, reader_class = name.rsplit(".", 1)
            reader_class = getattr(importlib.import_module(module), reader_class)
        except ValueError or ModuleNotFoundError or AttributeError:
            raise RuntimeError(f"Failed to import `{name}`.")

        assert isinstance(reader_class, type) and issubclass(reader_class, DocIterator)
        return reader_class.from_url(url)

    def __init__(self):
        self.fields: list[str] | None = None

    @abc.abstractmethod
    def __len__(self):
        pass

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


class DocReader(abc.ABC):
    """Abstract class for document readers."""

    @classmethod
    def from_url(cls, url: Union[str, URL]) -> "DocReader":
        url = URL.ensure_url(url)

        factory = {
            "json": "libdata.json.JSONReader",
            "yaml": "libdata.json.JSONReader",
            "yml": "libdata.json.JSONReader",
            "jsondir": "libdata.json_dir.JSONDirReader",
            "jsonl": "libdata.jsonl.JSONLReader",
            "mongo": "libdata.mongodb.MongoReader",
            "mongodb": "libdata.mongodb.MongoReader",
            "mysql": "libdata.mysql.MySQLReader",
            "yamldir": "libdata.json_dir.YAMLDirReader",
            "ymldir": "libdata.json_dir.YAMLDirReader",
        }

        if url.scheme not in factory:
            raise ValueError(f"Unsupported type \"{url.scheme}\".")

        name = factory.get(url.scheme)
        try:
            module, reader_class = name.rsplit(".", 1)
            reader_class = getattr(importlib.import_module(module), reader_class)
        except ValueError or ModuleNotFoundError or AttributeError:
            raise RuntimeError(f"Failed to import `{name}`.")

        assert isinstance(reader_class, type) and issubclass(reader_class, DocReader)
        return reader_class.from_url(url)

    def __iter__(self):
        return (self[idx] for idx in range(len(self)))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass

    @abc.abstractmethod
    def __getitem__(self, idx):
        pass

    @abc.abstractmethod
    def read(self, key):
        pass


class DocWriter(abc.ABC):
    """Abstract class for document writers."""

    @classmethod
    def from_url(cls, url: Union[str, URL]) -> "DocWriter":
        url = URL.ensure_url(url)

        factory = {
            "jsondir": "libdata.json_dir.JSONDirWriter",
            "json": "libdata.jsonl.JSONLWriter",
            "jsonl": "libdata.jsonl.JSONLWriter",
            "mongo": "libdata.mongodb.MongoWriter",
            "mongodb": "libdata.mongodb.MongoWriter",
            "mysql": "libdata.mysql.MySQLWriter",
            "yamldir": "libdata.json_dir.YAMLDirWriter",
            "ymldir": "libdata.json_dir.YAMLDirWriter",
        }

        if url.scheme not in factory:
            raise ValueError(f"Unsupported type \"{url.scheme}\".")

        name = factory.get(url.scheme)
        try:
            module, writer_class = name.rsplit(".", 1)
            writer_class = getattr(importlib.import_module(module), writer_class)
        except ValueError or ModuleNotFoundError or AttributeError:
            raise RuntimeError(f"Failed to import `{name}`.")

        assert isinstance(writer_class, type) and issubclass(writer_class, DocWriter)
        return writer_class.from_url(url)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def write(self, doc):
        pass

    @abc.abstractmethod
    def close(self):
        pass


ClientType = TypeVar("ClientType")


class LazyClient(Generic[ClientType]):

    def __init__(self):
        self._client = None

    @property
    def client(self) -> ClientType:
        if self._client is None:
            self._client = self._connect()
        return self._client

    def close(self):
        if hasattr(self, "_client") and self._client is not None:
            self._disconnect(self._client)
            self._client = None

    # noinspection PyBroadException
    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _connect(self) -> ClientType:
        raise NotImplementedError()

    def _disconnect(self, client: ClientType):
        raise NotImplementedError()


ConnType = TypeVar("ConnType")


class ConnectionPool(Generic[ConnType]):

    def __init__(self, max_size: int):
        self._max_size = max_size
        self.pools = defaultdict(deque)
        self.lock = Lock()

    @property
    def max_size(self):
        return self._max_size

    @max_size.setter
    def max_size(self, value: int):
        with self.lock:
            self._max_size = value

    def get(self, key) -> Optional[ConnType]:
        with self.lock:
            pool = self.pools[key]
            if len(pool) > 0:
                return pool.pop()
            return None

    def put(self, key, conn: ConnType) -> Optional[ConnType]:
        with self.lock:
            pool = self.pools[key]
            if len(pool) < self._max_size:
                pool.append(conn)
                return None
            return conn
