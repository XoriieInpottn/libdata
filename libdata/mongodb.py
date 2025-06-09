#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMongoClient",
    "MongoReader",
    "MongoWriter",
]

from typing import Any, List, Mapping, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import DeleteResult, UpdateResult
from tqdm import tqdm

from libdata.common import ConnectionPool, DocReader, DocWriter, LazyClient, ParsedURL


class LazyMongoClient(LazyClient[MongoClient]):
    """Mongo client with a connection pool.
    The client is thread safe.
    """

    @classmethod
    def from_url(cls, url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if url.hostname is None:
            url.hostname = "localhost"
        if url.port is None:
            url.port = 27017
        if url.database is None:
            raise ValueError("Database should be given in the URL.")
        if url.table is None:
            raise ValueError("Collection name should be given in the URL.")
        return cls(
            collection=url.table,
            database=url.database,
            hostname=url.hostname,
            port=url.port,
            username=url.username,
            password=url.password,
            **url.params
        )

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool[MongoClient](DEFAULT_CONN_POOL_SIZE)

    def __init__(
            self,
            collection: str,
            *,
            database: str = "default",
            hostname: str = "localhost",
            port: int = 27017,
            username: Optional[str] = None,
            password: Optional[str] = None,
            auth_db: str = "admin",
            buffer_size: int = 1000,
            connection_pool: Optional[ConnectionPool] = None,
            **kwargs
    ):
        super().__init__()
        self.collection_name = collection
        self.database = database
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.auth_db = auth_db
        self.buffer_size = buffer_size
        self.kwargs = kwargs

        self._conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL
        self._conn_key = (self.hostname, self.port, self.username)
        self._db = None
        self._coll = None
        self.buffer = []

    # noinspection PyPackageRequirements
    def _connect(self):
        client = self._conn_pool.get(self._conn_key)
        if client is None:
            client = MongoClient(
                host=self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                authSource=self.auth_db
            )
        return client

    def _disconnect(self, client):
        client = self._conn_pool.put(self._conn_key, client)
        if client is not None:
            client.close()

    def get_database(self) -> Database:
        if self._db is None:
            self._db = self.client.get_database(self.database)
        return self._db

    def get_collection(self) -> Collection:
        if self._coll is None:
            self._coll = self.get_database().get_collection(self.collection_name)
        return self._coll

    def insert(self, docs: Union[dict, List[dict]], flush=True):
        if isinstance(docs, List):
            return self.insert_many(docs, flush)
        else:
            return self.insert_one(docs, flush)

    def insert_one(self, doc: dict, flush=True):
        coll = self.get_collection()
        if flush:
            if len(self.buffer) > 0:
                self.buffer.append(doc)
                coll.insert_many(self.buffer)
                self.buffer.clear()
            else:
                coll.insert_one(doc)
        else:
            self.buffer.append(doc)
            if len(self.buffer) > self.buffer_size:
                coll.insert_many(self.buffer)
                self.buffer.clear()

    def insert_many(self, docs: List[dict], flush: True):
        coll = self.get_collection()
        self.buffer.extend(docs)
        if flush or len(self.buffer) > self.buffer_size:
            coll.insert_many(self.buffer)
            self.buffer.clear()

    def flush(self):
        if len(self.buffer) != 0:
            coll = self.get_collection()
            coll.insert_many(self.buffer)
            self.buffer.clear()

    def close(self):
        self.flush()
        super().close()

    def count(self) -> int:
        return self.get_collection().count()

    def count_documents(self, query: Optional[Mapping[str, Any]] = None) -> int:
        return self.get_collection().count_documents(query)

    def distinct(self, key, query: Optional[Mapping[str, Any]] = None):
        return self.get_collection().distinct(key, query)

    def find(
            self,
            query: Optional[Mapping[str, Any]] = None,
            projection: Optional[Mapping[str, Any]] = None,
            skip: Optional[int] = 0,
            limit: Optional[int] = 0,
            sort: Optional[List[Tuple[str, int]]] = None
    ) -> Cursor:
        return self.get_collection().find(
            filter=query,
            projection=projection,
            skip=skip,
            limit=limit,
            sort=sort
        )

    def find_one(
            self,
            query: Optional[Mapping[str, Any]] = None,
            projection: Optional[Mapping[str, Any]] = None,
            sort: Optional[List[Tuple[str, int]]] = None
    ) -> Cursor:
        return self.get_collection().find_one(
            filter=query,
            projection=projection,
            sort=sort
        )

    def delete_one(self, query: Mapping[str, Any]) -> DeleteResult:
        return self.get_collection().delete_one(query)

    def delete_many(self, query: Mapping[str, Any]) -> DeleteResult:
        return self.get_collection().delete_many(query)

    def update_one(
            self,
            query: Mapping[str, Any],
            update: Mapping[str, Any],
            upsert: bool = False,
    ) -> UpdateResult:
        return self.get_collection().update_one(
            filter=query,
            update=update,
            upsert=upsert
        )

    def update_many(
            self,
            query: Mapping[str, Any],
            update: Mapping[str, Any],
            upsert: bool = False,
    ) -> UpdateResult:
        return self.get_collection().update_many(
            filter=query,
            update=update,
            upsert=upsert
        )

    def start_session(self) -> ClientSession:
        return self.client.start_session()


class MongoReader(DocReader):

    @staticmethod
    @DocReader.register("mongo")
    @DocReader.register("mongodb")
    def from_url(url: Union[str, ParsedURL]) -> "MongoReader":
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"mongo", "mongodb"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")
        if url.database is None or url.table is None:
            raise ValueError(f"Invalid path \"{url.path}\" for mongodb.")

        return MongoReader(
            host=url.hostname,
            port=url.port,
            username=url.username,
            password=url.password,
            database=url.database,
            collection=url.table,
            **url.params
        )

    def __init__(
            self,
            database,
            collection,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            auth_db: str = "admin",
            key_field: str = "_id",
            use_cache: bool = False
    ) -> None:
        self.client = LazyMongoClient(
            collection=collection,
            database=database,
            hostname=host,
            port=port,
            username=username,
            password=password,
            auth_db=auth_db
        )
        self.key_field = key_field
        self.use_cache = use_cache

        self.id_list = self._fetch_ids()
        self.cache = {}

    def _fetch_ids(self):
        id_list = []
        with self.client:
            cur = self.client.find({}, {self.key_field: 1})
            for doc in tqdm(cur, leave=False):
                id_list.append(doc[self.key_field])
        return id_list

    def __len__(self):
        return len(self.id_list)

    def __getitem__(self, idx: int):
        _id = self.id_list[idx]
        if self.use_cache and _id in self.cache:
            return self.cache[_id]

        doc = self.client.find_one({self.key_field: _id})

        if self.use_cache:
            self.cache[_id] = doc
        return doc

    def read(self, _key=None, **kwargs):
        query = kwargs
        if _key is not None:
            query[self.key_field] = _key

        return self.client.find_one(query)


class MongoWriter(DocWriter):

    @staticmethod
    @DocWriter.register("mongo")
    @DocWriter.register("mongodb")
    def from_url(url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"mongo", "mongodb"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")
        if url.database is None or url.table is None:
            raise ValueError(f"Invalid path \"{url.path}\" for database.")

        return MongoWriter(
            host=url.hostname,
            port=url.port,
            username=url.username,
            password=url.password,
            database=url.database,
            collection=url.table,
            **url.params
        )

    def __init__(
            self,
            database,
            collection,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            auth_db: str = "admin",
            buffer_size: int = 512
    ):
        self.client = LazyMongoClient(
            collection=collection,
            database=database,
            hostname=host,
            port=port,
            username=username,
            password=password,
            auth_db=auth_db,
            buffer_size=buffer_size
        )

    def write(self, doc):
        return self.client.insert_one(doc, flush=False)

    def flush(self):
        return self.client.flush()

    def close(self):
        return self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.client.__exit__(exc_type, exc_val, exc_tb)
