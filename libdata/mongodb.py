#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMongoClient",
    "MongoReader",
    "MongoWriter",
]

from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import DeleteResult, UpdateResult
from tqdm import tqdm

from libdata.common import ConnectionPool, DocReader, DocWriter, LazyClient
from libdata.url import URL


class LazyMongoClient(LazyClient[MongoClient]):
    """Mongo client with a connection pool.
    The client is thread safe.
    """

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        return cls(url)

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool[MongoClient](DEFAULT_CONN_POOL_SIZE)

    def __init__(
            self,
            url: Union[str, URL],
            auth_source: str = "admin",
            buffer_size: int = 1000,
            connection_pool: Optional[ConnectionPool] = None
    ):
        super().__init__()
        url = URL.ensure_url(url)

        if url.scheme not in {"mongo", "mongodb"}:
            raise ValueError("scheme should be one of {\"mongodb\", \"mongo\"}")

        self.auth_source = auth_source
        self.buffer_size = buffer_size
        if url.parameters:
            params = url.parameters
            if "auth_source" in params:
                self.auth_source = params["auth_source"]
            elif "authSource" in params:
                self.auth_source = params["authSource"]
            elif "auth_db" in params:
                self.auth_source = params["auth_db"]

            if "buffer_size" in params:
                self.buffer_size = int(params["buffer_size"])
            elif "bufferSize" in params:
                self.buffer_size = int(params["bufferSize"])

        self._conn_url = URL(
            scheme="mongodb",
            username=url.username,
            password=url.password,
            address=url.address,
            parameters={"authSource": self.auth_source}
        ).to_string()

        self.database, self.collection = url.get_database_and_table()

        self._conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL
        self._db = None
        self._coll = None
        self.buffer = []

    def _connect(self):
        client = self._conn_pool.get(self._conn_url)
        if client is None:
            client = MongoClient(self._conn_url)
        return client

    def _disconnect(self, client):
        client = self._conn_pool.put(self._conn_url, client)
        if client is not None:
            client.close()

    def get_database(self) -> Database:
        if self._db is None:
            if not self.database:
                raise RuntimeError("Database name should be given.")
            self._db = self.client.get_database(self.database)
        return self._db

    def get_collection(self) -> Collection:
        if self._coll is None:
            if not self.collection:
                raise RuntimeError("Collection name should be given.")
            self._coll = self.get_database().get_collection(self.collection)
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

    def insert_many(self, docs: List[dict], flush: bool = True):
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
        self._db = None
        self._coll = None
        super().close()

    def count_documents(self, query: Optional[Mapping[str, Any]] = None) -> int:
        return self.get_collection().count_documents(query if query is not None else {})

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
    ) -> Optional[Dict]:
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

    @classmethod
    def from_url(cls, url: Union[str, URL]) -> "MongoReader":
        return MongoReader(url)

    def __init__(
            self,
            url: Union[str, URL],
            auth_db: str = "admin",
            key_field: str = "_id",
            use_cache: bool = False
    ) -> None:
        url = URL.ensure_url(url)
        self.client = LazyMongoClient(url, auth_source=auth_db)
        if url.parameters:
            params = url.parameters
            if "key_field" in params:
                key_field = params["key_field"]
            elif "keyField" in params:
                key_field = params["keyField"]

            if "use_cache" in params:
                use_cache = params["use_cache"].lower() in {"true", "1"}
            elif "useCache" in params:
                use_cache = params["useCache"].lower() in {"true", "1"}

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

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        return MongoWriter(url)

    def __init__(
            self,
            url: Union[str, URL],
            auth_db: str = "admin",
            buffer_size: int = 512
    ):
        self.client = LazyMongoClient(
            url,
            auth_source=auth_db,
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
