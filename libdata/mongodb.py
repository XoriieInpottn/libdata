#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "MongoReader",
    "MongoWriter",
]

from typing import Optional, Union

from tqdm import tqdm

from libdata.common import DocReader, DocWriter, ParsedURL


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
            self._conn.close()
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
        with self.get_connection() as conn:
            coll = conn[self.database][self.collection]
            id_list = []
            for doc in tqdm(coll.find({}, {'_id': 1}), leave=False):
                id_list.append(doc["_id"])
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
