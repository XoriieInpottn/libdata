#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMilvusClient",
]

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union

import numpy as np
from pymilvus import MilvusClient
from scipy.sparse import csr_array

from libdata.common import ConnectionPool, LazyClient
from libdata.url import URL

DEFAULT_VARCHAR_LENGTH = 65535
DEFAULT_ID_LENGTH = 256
DEFAULT_INDEX_CONFIG = {
    "AUTOINDEX": {
        "index_type": "AUTOINDEX",
        "metric_type": "IP",
        "params": {}
    },
    "HNSW": {
        "index_type": "HNSW",
        "metric_type": "IP",
        "params": {"M": 8, "efConstruction": 64}
    }
}

DenseVector = Union[np.ndarray, list]
SparseVector = Union[csr_array, dict]


class LazyMilvusClient(LazyClient[MilvusClient]):

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        return cls(url)

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool[MilvusClient](DEFAULT_CONN_POOL_SIZE)

    def __init__(
            self,
            url: Union[str, URL],
            collection: Optional[str] = None,
            database: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            connection_pool: Optional[ConnectionPool] = None,
    ):
        super().__init__()
        url = URL.ensure_url(url)

        allowed_schemes = {"milvus", "http", "https", "unix", "tcp"}
        if url.scheme not in allowed_schemes:
            raise ValueError(f"scheme should be one of {allowed_schemes}.")
        scheme = url.scheme
        if scheme == "milvus":
            scheme = "http"

        if username is None:
            username = url.username

        if password is None:
            password = url.password

        db_from_url, coll_from_url = url.get_database_and_table()
        if database is None:
            database = db_from_url
        if collection is None:
            collection = coll_from_url
        if not database:
            database = "default"
        if not collection:
            raise ValueError("Collection should be given.")

        self._conn_url = URL(
            scheme=scheme,
            address=url.address,
            path=f"/{database}"
        ).to_string()

        self.username = username
        self.password = password
        self.database = database
        self.collection = collection

        self._conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL

    def _connect(self):
        client = self._conn_pool.get(self._conn_url)
        if client is None:
            client = MilvusClient(
                self._conn_url,
                user=self.username,
                password=self.password,
                db_name=self.database
            )
        return client

    def _disconnect(self, client):
        client = self._conn_pool.put(self._conn_url, client)
        if client is not None:
            client.close()

    def exists(self, timeout: Optional[float] = None) -> bool:
        return self.client.has_collection(self.collection, timeout=timeout)

    def flush(self, timeout: Optional[float] = None):
        self.client.flush(self.collection, timeout=timeout)

    def create(
            self,
            ref_doc: Dict[str, Any],
            id_field: str = "id",
            dynamic_field: bool = True,
            vector_index: Union[str, Dict] = "AUTOINDEX",
            sparse_vector_index: Union[str, Dict] = "AUTOINDEX",
            timeout: Optional[float] = None
    ):
        from pymilvus import DataType

        if id_field not in ref_doc:
            # ID field not given in the ref_doc means the collection need to use auto_id mode.
            schema = self.client.create_schema(auto_id=True, enable_dynamic_field=dynamic_field)
            schema.add_field(field_name=id_field, datatype=DataType.INT64, is_primary=True)
        else:
            schema = self.client.create_schema(auto_id=False, enable_dynamic_field=dynamic_field)
            id_value = ref_doc[id_field]
            field_kwargs: Dict[str, Any] = {"is_primary": True}
            if isinstance(id_value, int):
                field_kwargs["datatype"] = DataType.INT64
            elif isinstance(id_value, str):
                field_kwargs["datatype"] = DataType.VARCHAR
                field_kwargs["max_length"] = DEFAULT_ID_LENGTH
            else:
                raise TypeError(f"Invalid id type \"{type(id_value)}\". Should be one of int or str.")
            schema.add_field(field_name=id_field, **field_kwargs)

        index_params = self.client.prepare_index_params()
        index_params.add_index(id_field)

        for field, value in ref_doc.items():
            if field == id_field:
                # ID field is already added.
                continue

            field_kwargs = self._infer_dtype(value)
            if field_kwargs["datatype"] is DataType.FLOAT_VECTOR:
                if isinstance(vector_index, str):
                    vector_index = DEFAULT_INDEX_CONFIG[vector_index]
                index_params.add_index(
                    field_name=field,
                    index_type=vector_index["index_type"],
                    metric_type=vector_index["metric_type"],
                    **vector_index["params"]
                )
            elif field_kwargs["datatype"] is DataType.SPARSE_FLOAT_VECTOR:
                if isinstance(sparse_vector_index, str):
                    sparse_vector_index = DEFAULT_INDEX_CONFIG[sparse_vector_index]
                index_params.add_index(
                    field_name=field,
                    index_type=sparse_vector_index["index_type"],
                    metric_type=sparse_vector_index["metric_type"],
                    **sparse_vector_index["params"]
                )
            schema.add_field(field_name=field, **field_kwargs)

        self.client.create_collection(
            collection_name=self.collection,
            schema=schema,
            index_params=index_params,
            timeout=timeout
        )

    @staticmethod
    def _infer_dtype(value):
        from pymilvus import DataType

        if isinstance(value, str):
            return {"datatype": DataType.VARCHAR, "max_length": DEFAULT_VARCHAR_LENGTH}
        elif isinstance(value, int):
            return {"datatype": DataType.INT64}
        elif isinstance(value, float):
            return {"datatype": DataType.FLOAT}
        elif isinstance(value, np.ndarray):
            shape = value.shape
            if len(shape) == 1 or (len(shape) == 2 and shape[0] == 1):
                return {"datatype": DataType.FLOAT_VECTOR, "dim": shape[-1]}
            raise ValueError(f"Invalid vector shape {shape}.")
        elif isinstance(value, csr_array):
            return {"datatype": DataType.SPARSE_FLOAT_VECTOR}
        elif isinstance(value, datetime):
            return {"datatype": DataType.VARCHAR, "max_length": 24}
        elif isinstance(value, List):
            if len(value) > 0:
                if all(isinstance(v, (float, int)) for v in value):
                    return {"datatype": DataType.FLOAT_VECTOR, "dim": len(value)}
                elif all(
                        isinstance(v, (tuple, list))
                        and len(v) == 2
                        and isinstance(v[0], int)
                        and isinstance(v[1], (float, int))
                        for v in value
                ):
                    return {"datatype": DataType.SPARSE_FLOAT_VECTOR}
            return {"datatype": DataType.JSON}
        elif isinstance(value, Mapping):
            if len(value) > 0:
                if all(isinstance(k, int) and isinstance(v, (float, int)) for k, v in value.items()):
                    return {"datatype": DataType.SPARSE_FLOAT_VECTOR}
            return {"datatype": DataType.JSON}
        else:
            raise TypeError(f"Unsupported data type {type(value)}.")

    @staticmethod
    def _prepare_doc_for_insert(doc: dict):
        converted_doc = {}
        for name, value in doc.items():
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, np.ndarray):
                shape = value.shape
                if len(shape) == 1:
                    value = value.tolist()
                elif len(shape) == 2 and shape[0] == 1:
                    value = value[0].tolist()
                else:
                    raise ValueError("Only one vector can be used.")
            elif isinstance(value, csr_array):
                shape = value.shape
                if len(shape) == 1:
                    value = [{i: v for i, v in zip(value.indices, value.data)}]
                elif len(shape) == 2 and shape[0] == 1:
                    start, end = value.indptr[0], value.indptr[1]
                    value = {i: v for i, v in zip(value.indices[start:end], value.data[start:end])}
                else:
                    raise ValueError("Only one vector can be used.")
            converted_doc[name] = value
        return converted_doc

    def drop(self, timeout: Optional[float] = None):
        self.client.drop_collection(self.collection, timeout=timeout)

    def insert(self, docs: Union[dict, List[dict]], timeout: Optional[float] = None) -> dict:
        if not isinstance(docs, List):
            docs = [docs]

        if len(docs) == 0:
            return {"insert_count": 0, "ids": []}

        if not self.exists(timeout=timeout):
            self.create(docs[0])

        docs = [self._prepare_doc_for_insert(doc) for doc in docs]

        return self.client.insert(
            self.collection,
            data=docs,
            timeout=timeout
        )

    def upsert(self, docs: Union[Dict, List[Dict]], timeout: Optional[float] = None) -> dict:
        if not isinstance(docs, List):
            docs = [docs]

        if len(docs) == 0:
            return {"insert_count": 0, "ids": []}

        if not self.exists(timeout=timeout):
            self.create(docs[0])

        docs = [self._prepare_doc_for_insert(doc) for doc in docs]

        return self.client.upsert(
            self.collection,
            data=docs,
            timeout=timeout
        )

    def delete(
            self,
            ids: Optional[Union[list, str, int]] = None,
            expr: Optional[str] = None,
            timeout: Optional[float] = None,
    ) -> dict:
        return self.client.delete(self.collection, ids=ids, filter=expr, timeout=timeout)

    def query(
            self,
            expr: str = "",
            ids: Optional[Union[List, str, int]] = None,
            output_fields: Optional[List[str]] = None,
            timeout: Optional[float] = None,
    ) -> List[dict]:
        return self.client.query(
            self.collection,
            filter=expr,
            output_fields=output_fields,
            timeout=timeout,
            ids=ids,
        )

    @staticmethod
    def _prepare_vector_for_search(vector):
        if isinstance(vector, np.ndarray):
            shape = vector.shape
            if len(shape) == 1:
                return [vector.tolist()]
            elif len(shape) == 2:
                return vector.tolist()
            else:
                raise ValueError("Vector shape cannot be larger than 2.")
        elif isinstance(vector, csr_array):
            shape = vector.shape
            if len(shape) == 1:
                return [{i: v for i, v in zip(vector.indices, vector.data)}]
            elif len(shape) == 2:
                return [
                    {i: v for i, v in zip(
                        vector.indices[vector.indptr[row]:vector.indptr[row + 1]],
                        vector.data[vector.indptr[row]:vector.indptr[row + 1]]
                    )}
                    for row in range(shape[0])
                ]
            else:
                raise ValueError("Vector shape cannot be larger than 2.")
        elif isinstance(vector, dict):
            return [vector]
        elif isinstance(vector, list):
            if len(vector) == 0:
                raise ValueError("Vector cannot be an empty list.")
            if not isinstance(vector[0], List):
                return [vector]
            return vector
        else:
            raise ValueError(f"Unsupported vector type {type(vector)}.")

    def search(
            self,
            field: str,
            data: Union[DenseVector, SparseVector, List[DenseVector], List[SparseVector]],
            expr: str = "",
            limit: int = 10,
            output_fields: Optional[List[str]] = None,
            search_params: Optional[dict] = None,
            timeout: Optional[float] = None,
    ):
        if not self.client.has_collection(self.collection):
            return []

        data = self._prepare_vector_for_search(data)
        response = self.client.search(
            self.collection,
            data,
            filter=expr,
            limit=limit,
            output_fields=output_fields,
            anns_field=field,
            search_params=search_params,
            timeout=timeout,
        )
        return response[0]

    def hybrid_search(
            self,
            data: Mapping[str, Union[DenseVector, SparseVector, List[DenseVector], List[SparseVector]]],
            expr: str = "",
            limit: int = 10,
            output_fields: Optional[List[str]] = None,
            search_params: Optional[dict] = None,
            timeout: Optional[float] = None,
    ):
        if not self.client.has_collection(self.collection):
            return []

        from pymilvus import AnnSearchRequest, RRFRanker

        reqs = []
        for ann_field, vector in data.items():
            vector = self._prepare_vector_for_search(vector)
            reqs.append(AnnSearchRequest(
                vector,
                ann_field,
                param=search_params if search_params else {},
                limit=limit,
                expr=expr
            ))

        response = self.client.hybrid_search(
            self.collection,
            reqs,
            RRFRanker(k=60),
            filter=expr,
            limit=limit,
            output_fields=output_fields,
            timeout=timeout,
        )
        return response[0]
