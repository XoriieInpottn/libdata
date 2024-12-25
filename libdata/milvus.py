#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMilvusClient",
]

from typing import Any, Dict, List, Optional, Union

import numpy as np

from libdata.common import LazyClient, ParsedURL

DEFAULT_VARCHAR_LENGTH = 65536
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


class LazyMilvusClient(LazyClient):

    @classmethod
    def from_url(cls, url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if url.hostname is None:
            url.hostname = "localhost"
        if url.port is None:
            url.port = 19530
        if url.database is None:
            url.database = "default"
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

    def __init__(
            self,
            collection: str,
            *,
            database: str = "default",
            hostname: str = "localhost",
            port: int = 19530,
            username: Optional[str] = None,
            password: Optional[str] = None,
            **kwargs
    ):
        super().__init__()
        self.collection_name = collection
        self.database = database
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.kwargs = kwargs

    # noinspection PyPackageRequirements
    def _connect(self):
        from pymilvus import MilvusClient
        return MilvusClient(
            f"http://{self.hostname}:{self.port}",
            user=self.username,
            password=self.password,
            db_name=self.database,
            **self.kwargs
        )

    def _disconnect(self, client):
        client.close()

    def exists(self, timeout: Optional[float] = None) -> bool:
        return self.client.has_collection(self.collection_name, timeout=timeout)

    # noinspection PyPackageRequirements
    def create(
            self,
            ref_doc: Dict[str, Any],
            id_field: str = "id",
            dynamic_field: bool = True,
            dense_index: Union[str, Dict] = "AUTOINDEX",
            timeout: Optional[float] = None
    ):
        from pymilvus import DataType
        from pymilvus.orm.types import infer_dtype_bydata

        if id_field not in ref_doc:
            # ID field not given in the ref_doc means the collection need to use auto_id mode.
            schema = self.client.create_schema(auto_id=True, enable_dynamic_field=dynamic_field)
            schema.add_field(field_name=id_field, datatype=DataType.INT64, is_primary=True)
        else:
            schema = self.client.create_schema(auto_id=False, enable_dynamic_field=dynamic_field)
            id_value = ref_doc[id_field]
            id_dtype = infer_dtype_bydata(id_value)
            if id_dtype is DataType.INT64:
                schema.add_field(field_name=id_field, datatype=DataType.INT64, is_primary=True)
            elif id_dtype is DataType.VARCHAR:
                schema.add_field(field_name=id_field, datatype=DataType.VARCHAR, is_primary=True, max_length=128)
            else:
                raise TypeError(f"Invalid id dtype \"{id_dtype}\". Should be one of INT64 or VARCHAR().")

        index_params = self.client.prepare_index_params()
        index_params.add_index(id_field)

        for field, value in ref_doc.items():
            if field == id_field:
                # ID field is already added.
                continue
            dtype = infer_dtype_bydata(value)
            kwargs = {}
            if dtype is DataType.UNKNOWN:
                raise TypeError(f"Unsupported data type {type(value)}.")
            elif dtype is DataType.VARCHAR:
                kwargs["max_length"] = 65535
            elif dtype in {DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR}:
                kwargs["dim"] = len(value)
                if isinstance(dense_index, str):
                    dense_index = DEFAULT_INDEX_CONFIG[dense_index]
                index_params.add_index(
                    field_name=field,
                    index_type=dense_index["index_type"],
                    metric_type=dense_index["metric_type"],
                    **dense_index["params"]
                )
            elif dtype is DataType.SPARSE_FLOAT_VECTOR:
                raise NotImplementedError()
            schema.add_field(field_name=field, datatype=dtype, **kwargs)

        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            index_params=index_params,
            timeout=timeout
        )

    def drop(self, timeout: Optional[float] = None):
        self.client.drop_collection(self.collection_name, timeout=timeout)

    def insert(self, docs: Union[dict, List[dict]], timeout: Optional[float] = None) -> dict:
        if not self.exists(timeout=timeout):
            if isinstance(docs, List):
                if len(docs) == 0:
                    return {"insert_count": 0, "ids": []}
                ref_doc = docs[0]
            else:
                ref_doc = docs
            self.create(ref_doc)
        return self.client.insert(self.collection_name, docs)

    def upsert(self, docs: Union[Dict, List[Dict]], timeout: Optional[float] = None) -> dict:
        if not self.exists(timeout=timeout):
            if isinstance(docs, List):
                if len(docs) == 0:
                    return {"upsert_count": 0, "ids": []}
                ref_doc = docs[0]
            else:
                ref_doc = docs
            self.create(ref_doc)
        return self.client.upsert(
            collection_name=self.collection_name,
            data=docs,
            timeout=timeout
        )

    # noinspection PyShadowingBuiltins
    def delete(
            self,
            ids: Optional[Union[list, str, int]] = None,
            filter: Optional[str] = None,
            timeout: Optional[float] = None,
    ) -> dict:
        return self.client.delete(self.collection_name, ids=ids, filter=filter, timeout=timeout)

    # noinspection PyShadowingBuiltins
    def query(
            self,
            filter: str = "",
            ids: Optional[Union[List, str, int]] = None,
            output_fields: Optional[List[str]] = None,
            timeout: Optional[float] = None,
    ) -> List[dict]:
        return self.client.query(
            self.collection_name,
            filter=filter,
            output_fields=output_fields,
            timeout=timeout,
            ids=ids,
        )

    # noinspection PyShadowingBuiltins
    def search(
            self,
            field: str,
            vector: Union[List[list], list, np.ndarray],
            filter: str = "",
            limit: int = 10,
            output_fields: Optional[List[str]] = None,
            search_params: Optional[dict] = None,
            timeout: Optional[float] = None,
    ):
        if not self.client.has_collection(self.collection_name):
            return []

        if isinstance(vector, np.ndarray):
            if len(vector.shape) == 1:
                vector = [vector.tolist()]
            else:
                vector = vector.tolist()
        elif isinstance(vector, list):
            if not isinstance(vector[0], list):
                vector = [vector]

        response = self.client.search(
            self.collection_name,
            vector,
            filter=filter,
            limit=limit,
            output_fields=output_fields,
            anns_field=field,
            search_params=search_params,
            timeout=timeout,
        )
        return response[0]
