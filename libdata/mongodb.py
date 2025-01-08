#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMongoClient",
    "MongoReader",
    "MongoWriter",
]

from typing import List, Optional, Union

from tqdm import tqdm

from libdata.common import ConnectionPool, DocReader, DocWriter, LazyClient, ParsedURL


class LazyMongoClient(LazyClient):

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
    DEFAULT_CONN_POOL = ConnectionPool(DEFAULT_CONN_POOL_SIZE)

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
        self.buffer = []

    # noinspection PyPackageRequirements
    def _connect(self):
        client = self._conn_pool.get(self._conn_key)
        if client is None:
            from pymongo import MongoClient
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

    def insert(self, docs: Union[dict, List[dict]], flush=True):
        return self.insert_many(docs, flush) if isinstance(docs, List) else self.insert_one(docs, flush)

    def insert_one(self, doc: dict, flush=True):
        coll = self.client[self.database][self.collection_name]
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
        coll = self.client[self.database][self.collection_name]
        self.buffer.extend(docs)
        if flush or len(self.buffer) > self.buffer_size:
            coll.insert_many(self.buffer)
            self.buffer.clear()

    def flush(self):
        if len(self.buffer) != 0:
            coll = self.client[self.database][self.collection_name]
            coll.insert_many(self.buffer)
            self.buffer.clear()

    def count(self):
        return self.client[self.database][self.collection_name].count()

    def find(self, query: Optional[dict] = None):
        coll = self.client[self.database][self.collection_name]
        return coll.find(query)

    def find_one(self, query: Optional[dict] = None):
        coll = self.client[self.database][self.collection_name]
        return coll.find_one(query)


class LazyClient:

    def __init__(
            self,
            database,
            collection,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            auth_db: str = "admin",
    ):
        self.database = database
        self.collection = collection
        self.host = host if host else "localhost"
        self.port = port if port else 27017
        self.username = username
        self.password = password
        self.auth_db = auth_db

        self._conn = None

    def get_connection(self):
        if self._conn is None:
            # noinspection PyPackageRequirements
            from pymongo import MongoClient
            self._conn = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                authSource=self.auth_db
            )
        return self._conn

    def close(self):
        if hasattr(self, "_conn") and self._conn is not None:
            # noinspection PyBroadException
            try:
                self._conn.close()
            except:
                pass
            self._conn = None

    def __del__(self):
        self.close()


class MongoReader(DocReader, LazyClient):

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
        super().__init__(
            database=database,
            collection=collection,
            host=host,
            port=port,
            username=username,
            password=password,
            auth_db=auth_db,
        )
        self.key_field = key_field
        self.use_cache = use_cache

        self.id_list = self._fetch_ids()
        self.cache = {}

    def _fetch_ids(self):
        id_list = []
        with self.get_connection() as conn:
            coll = conn[self.database][self.collection]
            for doc in tqdm(coll.find({}, {'_id': 1}), leave=False):
                id_list.append(doc["_id"])
        self._conn = None
        return id_list

    def __len__(self):
        return len(self.id_list)

    def __getitem__(self, idx: int):
        _id = self.id_list[idx]
        if self.use_cache and _id in self.cache:
            return self.cache[_id]

        conn = self.get_connection()
        coll = conn[self.database][self.collection]
        doc = coll.find_one({"_id": _id})

        if self.use_cache:
            self.cache[_id] = doc
        return doc

    def read(self, _key=None, **kwargs):
        query = kwargs
        if _key is not None:
            query[self.key_field] = _key

        conn = self.get_connection()
        coll = conn[self.database][self.collection]
        return coll.find_one(query)


class MongoWriter(DocWriter, LazyClient):

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
        super().__init__(
            database=database,
            collection=collection,
            host=host,
            port=port,
            username=username,
            password=password,
            auth_db=auth_db,
        )
        self.buffer_size = buffer_size
        self.buffer = []

    def write(self, doc):
        if self.buffer_size > 0:
            self.buffer.append(doc)
            if len(self.buffer) > self.buffer_size:
                conn = self.get_connection()
                coll = conn[self.database][self.collection]
                coll.insert_many(self.buffer)
                self.buffer.clear()
        else:
            conn = self.get_connection()
            coll = conn[self.database][self.collection]
            coll.insert_one(doc)

    def flush(self):
        if len(self.buffer) != 0:
            conn = self.get_connection()
            coll = conn[self.database][self.collection]
            coll.insert_many(self.buffer)
            self.buffer.clear()

    def close(self):
        self.flush()
        super().close()
