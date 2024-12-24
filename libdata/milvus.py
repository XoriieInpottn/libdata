#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMilvusClient",
]

from typing import Any, Dict, List, Optional, Union

import numpy as np

from libdata.common import LazyClient, ParsedURL


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

    def create(
            self,
            ref_doc: Dict[str, Any],
            id_field: str = "id",
            auto_id: bool = True,
            dynamic_field: bool = True,
            dense_index: Union[str, Dict] = "AUTOINDEX",
    ):
        from pymilvus import DataType
        from pymilvus.orm.types import infer_dtype_bydata

        if self.client.has_collection(self.collection_name):
            return

        default_index_params = {
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
        schema = self.client.create_schema(auto_id=auto_id, enable_dynamic_field=dynamic_field)
        index_params = self.client.prepare_index_params()

        schema.add_field(field_name=id_field, datatype=DataType.INT64, is_primary=True)
        index_params.add_index(id_field)

        for k, v in ref_doc.items():
            dtype = infer_dtype_bydata(v)
            kwargs = {}
            if dtype is DataType.UNKNOWN:
                raise TypeError(f"Unsupported data type {type(v)}.")
            elif dtype is DataType.VARCHAR:
                kwargs["max_length"] = 65535
            elif dtype in {DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR}:
                kwargs["dim"] = len(v)
                if isinstance(dense_index, str):
                    dense_index = default_index_params[dense_index]
                index_params.add_index(
                    field_name=k,
                    index_type=dense_index["index_type"],
                    metric_type=dense_index["metric_type"],
                    **dense_index["params"]
                )
            elif dtype is DataType.SPARSE_FLOAT_VECTOR:
                raise NotImplementedError()
            schema.add_field(field_name=k, datatype=dtype, **kwargs)

        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            index_params=index_params,
        )

    def drop(self):
        self.client.drop_collection(self.collection_name)

    def insert(self, doc: Dict[str, Any]):
        self.create(doc)
        return self.client.insert(self.collection_name, doc)

    def delete(self, ids):
        self.client.delete(self.collection_name, ids)

    def search(
            self,
            field: str,
            data: Union[List[list], list, np.ndarray],
            filter: str = "",
            limit: int = 10,
            output_fields: Optional[List[str]] = None,
            search_params: Optional[dict] = None,
            timeout: Optional[float] = None,
    ):
        if not self.client.has_collection(self.collection_name):
            return []

        if isinstance(data, np.ndarray):
            if len(data.shape) == 1:
                data = [data.tolist()]
            else:
                data = data.tolist()
        elif isinstance(data, list):
            if not isinstance(data[0], list):
                data = [data]

        response = self.client.search(
            self.collection_name,
            data,
            filter=filter,
            limit=limit,
            output_fields=output_fields,
            anns_field=field,
            search_params=search_params,
            timeout=timeout,
        )
        return response[0]
