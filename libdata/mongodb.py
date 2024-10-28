#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "MongoReader",
    "MongoWriter",
]

from typing import Union

from tqdm import tqdm

from libdata.common import DocReader, DocWriter, ParsedURL


class MongoReader(DocReader):

    @staticmethod
    @DocReader.factory.register("mongo")
    @DocReader.factory.register("mongodb")
    def from_url(url: Union[str, ParsedURL]):
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
            host: str = "127.0.0.1",
            port: int = 27017,
            username: str = "root",
            password: str = None,
            auth_db: str = "admin",
            key_field: str = "_id",
            use_cache: bool = False
    ) -> None:
        self.database = database
        self.collection = collection
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth_db = auth_db
        self.key_field = key_field
        self.use_cache = use_cache

        self.id_list = self._fetch_ids()
        self.conn = None
        self.cache = {}

    def _get_conn(self):
        from pymongo import MongoClient
        return MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            authSource=self.auth_db
        )

    def _fetch_ids(self):
        with self._get_conn() as conn:
            db = conn[self.database]
            coll = db[self.collection]
            key_list = []
            for doc in tqdm(coll.find({}, {'_id': 1}), leave=False):
                key_list.append(doc["_id"])
            return key_list

    def __len__(self):
        return len(self.id_list)

    def __getitem__(self, idx: int):
        _id = self.id_list[idx]
        if self.use_cache and _id in self.cache:
            return self.cache[_id]

        if self.conn is None:
            self.conn = self._get_conn()

        coll = self.conn[self.database][self.collection]
        doc = coll.find_one({"_id": _id})

        if self.use_cache:
            self.cache[_id] = doc
        return doc

    def read(self, key):
        coll = self.conn[self.database][self.collection]
        return coll.find_one({self.key_field: key})

    def close(self):
        if self.conn is not None:
            # self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()


class MongoWriter(DocWriter):

    @staticmethod
    @DocWriter.factory.register("mongo")
    @DocWriter.factory.register("mongodb")
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
            host: str,
            port: int,
            username: str,
            password: str,
            database: str,
            collection: str,
            buffer_size: int = 100
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.collection = collection
        self.buffer_size = buffer_size

        self._conn = None
        self.buffer = []

    def _get_conn(self):
        if self._conn is None:
            from pymongo import MongoClient
            self._conn = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
        return self._conn

    def write(self, doc):
        if self.buffer_size > 0:
            self.buffer.append(doc)
            if len(self.buffer) > 100:
                conn = self._get_conn()
                coll = conn[self.database][self.collection]
                coll.insert_many(self.buffer)
                self.buffer.clear()
        else:
            conn = self._get_conn()
            coll = conn[self.database][self.collection]
            coll.insert_one(doc)

    def flush(self):
        if len(self.buffer) != 0:
            conn = self._get_conn()
            coll = conn[self.database][self.collection]
            coll.insert_many(self.buffer)
            self.buffer.clear()

    def close(self):
        if self._conn is not None:
            self.flush()
            self._conn.close()
            self._conn = None
