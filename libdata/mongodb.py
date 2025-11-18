#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMongoClient",
    "MongoReader",
    "MongoWriter",
]

import sys
from typing import Any, Generator, List, Mapping, Optional, Tuple, Union

from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import DeleteResult, UpdateResult
from tqdm import tqdm

from libdata.common import ConnectionPool, DocIterator, DocReader, DocWriter, LazyClient
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

    def get_collection(self, collection: str | None = None) -> Collection:
        if collection:
            return self.get_database().get_collection(collection)

        if self._coll is None:
            if not self.collection:
                raise RuntimeError("Collection name should be given.")
            self._coll = self.get_database().get_collection(self.collection)
        return self._coll

    def insert(self, docs: Union[dict, List[dict]], flush=True):
        if isinstance(docs, List):
            self.buffer.extend(docs)
        else:
            self.buffer.append(docs)

        if len(self.buffer) > self.buffer_size:
            coll = self.get_collection()
            coll.insert_many(self.buffer)
            self.buffer.clear()
        elif flush:
            self.flush()

    def insert_one(self, doc: dict, collection: str | None = None):
        coll = self.get_collection(collection)
        return coll.insert_one(doc)

    def insert_many(self, docs: List[dict], collection: str | None = None):
        coll = self.get_collection(collection)
        return coll.insert_many(docs)

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

    def count_documents(self, query: Optional[Mapping[str, Any]] = None, collection: str | None = None) -> int:
        coll = self.get_collection(collection)
        return coll.count_documents(query if query is not None else {})

    def distinct(self, key, query: Optional[Mapping[str, Any]] = None, collection: str | None = None):
        return self.get_collection(collection).distinct(key, query)

    def find(
            self,
            query: Optional[Mapping[str, Any]] = None,
            projection: Optional[Mapping[str, Any] | type[BaseModel]] = None,
            skip: Optional[int] = 0,
            limit: Optional[int] = 0,
            sort: Optional[List[Tuple[str, int]]] = None,
            collection: str | None = None
    ) -> Cursor | Generator[BaseModel, None, None]:
        coll = self.get_collection() if collection is None else self.get_database()[collection]
        if isinstance(projection, type) and issubclass(projection, BaseModel):
            pipeline = []
            if query:
                pipeline.append({"$match": query})
            pipeline.append({"$project": self.create_projection(projection)})
            if sort:
                pipeline.append({"$sort": dict(sort)})
            if skip:
                pipeline.append({"$skip": skip})
            if limit:
                pipeline.append({"$limit": limit})

            cur = coll.aggregate(pipeline)

            def _iter_objs():
                with cur:
                    for doc in cur:
                        yield projection.model_validate(doc)

            return _iter_objs()
        else:
            return coll.find(
                filter=query,
                projection=projection,
                skip=skip,
                limit=limit,
                sort=sort
            )

    def find_one(
            self,
            query: Optional[Mapping[str, Any]] = None,
            projection: Optional[Mapping[str, Any] | type[BaseModel]] = None,
            sort: Optional[List[Tuple[str, int]]] = None,
            collection: str | None = None
    ) -> dict | BaseModel | None:
        for result in self.find(query, projection, limit=1, sort=sort, collection=collection):
            return result
        return None

    @staticmethod
    def create_projection(model_type: type[BaseModel]) -> dict:
        result = {}
        for field_name, field_info in model_type.model_fields.items():
            extra = field_info.json_schema_extra or {}
            original_name = extra.get("original") or extra.get("origin") or extra.get("raw")
            result[field_name] = ("$" + original_name) if original_name else 1
        return result

    def delete_one(self, query: Mapping[str, Any], collection: str | None = None) -> DeleteResult:
        return self.get_collection(collection).delete_one(query)

    def delete_many(self, query: Mapping[str, Any], collection: str | None = None) -> DeleteResult:
        return self.get_collection(collection).delete_many(query)

    def update_one(
            self,
            query: Mapping[str, Any],
            update: Mapping[str, Any],
            upsert: bool = False,
            collection: str | None = None
    ) -> UpdateResult:
        return self.get_collection(collection).update_one(
            filter=query,
            update=update,
            upsert=upsert
        )

    def update_many(
            self,
            query: Mapping[str, Any],
            update: Mapping[str, Any],
            upsert: bool = False,
            collection: str | None = None
    ) -> UpdateResult:
        return self.get_collection(collection).update_many(
            filter=query,
            update=update,
            upsert=upsert
        )

    def start_session(self) -> ClientSession:
        return self.client.start_session()


class MongoIterator(DocIterator):
    """Iterator for MongoDB collections."""

    @classmethod
    def from_url(cls, url: Union[str, URL], auth_db: str = "admin"):
        url = URL.ensure_url(url)
        if url.scheme not in {"mongodb", "mongo"}:
            raise ValueError(f"Unsupported scheme '{url.scheme}'.")
        return cls(url, auth_db=auth_db)

    def __init__(self, url: Union[str, URL], auth_db: str = "admin"):
        super().__init__()

        url = URL.ensure_url(url)
        self.client = LazyMongoClient(url, auth_source=auth_db)

        self._cursor = None
        self._exhausted = False
        self._count = None

    def __len__(self):
        if self._count is None:
            self._count = self.client.count_documents()
        return self._count

    def __iter__(self):
        if self._cursor is None:
            self._cursor = self.client.find(
                projection={f: 1 for f in self.fields} if self.fields else None
            )
            self._exhausted = False
        return self

    def __next__(self):
        if self._exhausted:
            raise StopIteration()

        try:
            doc = next(self._cursor)
            return doc
        except StopIteration:
            self._exhausted = True
            self.close()
            raise
        except Exception as e:
            print(f"Error reading from MongoDB: {e}", file=sys.stderr)
            self._exhausted = True
            self.close()
            raise StopIteration()

    def close(self):
        if getattr(self, "_cursor", None) is not None:
            try:
                self._cursor.close()
            except Exception as e:
                print(e, file=sys.stderr)
            self._cursor = None

        if getattr(self, "client", None) is not None:
            try:
                self.client.close()
            except Exception as e:
                print(e, file=sys.stderr)
            self.client = None


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
        return self.client.insert(doc, flush=False)

    def flush(self):
        return self.client.flush()

    def close(self):
        return self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.client.__exit__(exc_type, exc_val, exc_tb)
