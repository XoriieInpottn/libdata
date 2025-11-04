#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "LazyMySQLClient",
    "MySQLReader",
    "MySQLWriter",
]

import sys
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union

from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from tqdm import tqdm

from libdata.common import ConnectionPool, DocIterator, DocReader, DocWriter, LazyClient
from libdata.url import Address, URL


class LazyMySQLClient(LazyClient[MySQLConnection]):
    """MySQL client with a connection pool.
    The client is thread safe.
    """

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        return cls(url)

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool[MySQLConnection](DEFAULT_CONN_POOL_SIZE)

    def __init__(
            self,
            url: Union[str, URL],
            charset: str | None = None,
            autocommit: bool | None = None,
            connect_timeout: int | None = None,
            connection_pool: Optional[ConnectionPool] = None
    ):
        super().__init__()
        url = URL.ensure_url(url)

        if url.scheme != "mysql":
            raise ValueError("scheme should be `mysql`.")

        input_params = url.parameters or {}
        valid_params = {}

        charset = (
            charset if charset is not None else
            input_params.get("charset")
        )
        if charset is not None:
            valid_params["charset"] = charset

        autocommit = (
            str(autocommit).lower()
            if autocommit is not None else
            input_params.get("autocommit", "true")
        )
        valid_params["autocommit"] = autocommit

        connect_timeout = (
            str(connect_timeout) if connect_timeout is not None else
            input_params.get("connect_timeout")
        )
        if connect_timeout is not None:
            valid_params["connect_timeout"] = connect_timeout

        self._conn_url = URL(
            scheme="mysql",
            username=url.username,
            password=url.password,
            address=url.address,
            parameters=valid_params
        ).to_string()

        self.database, self.table = url.get_database_and_table()

        self._conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL

    def _connect(self):
        client = self._conn_pool.get(self._conn_url)
        if client is None:
            return self._create_connection()
        elif not client.is_connected():
            client.close()
            return self._create_connection()
        else:
            return client

    def _create_connection(self):
        conn_url = URL.ensure_url(self._conn_url)
        assert isinstance(conn_url.address, Address)

        kwargs: dict[str, Any] = conn_url.parameters or {}
        if "autocommit" in kwargs:
            kwargs["autocommit"] = kwargs["autocommit"].lower() in {"true", "1"}
        if "connect_timeout" in kwargs:
            kwargs["connect_timeout"] = float(kwargs["connect_timeout"])

        return MySQLConnection(
            host=conn_url.address.host,
            port=conn_url.address.port or 3306,
            user=conn_url.username,
            password=conn_url.password,
            database=self.database,
            **kwargs
        )

    def _disconnect(self, client):
        client = self._conn_pool.put(self._conn_url, client)
        if client is not None:
            client.close()

    def cursor(
            self,
            buffered: Optional[bool] = None,
            raw: Optional[bool] = None,
            prepared: Optional[bool] = None,
            dictionary: Optional[bool] = None
    ) -> MySQLCursor:
        return self.client.cursor(
            buffered=buffered,
            raw=raw,
            prepared=prepared,
            dictionary=dictionary
        )

    def execute(
            self,
            sql: str,
            params=None,
            buffered: Optional[bool] = None,
            raw: Optional[bool] = None,
            prepared: Optional[bool] = None,
            dictionary: Optional[bool] = None
    ) -> MySQLCursor:
        cur = self.client.cursor(
            buffered=buffered,
            raw=raw,
            prepared=prepared,
            dictionary=dictionary
        )
        cur.execute(sql, params=params)
        return cur

    def start_transaction(
            self,
            consistent_snapshot: bool = False,
            isolation_level: Optional[str] = None,
            readonly: Optional[bool] = None,
    ) -> None:
        self.client.start_transaction(
            consistent_snapshot=consistent_snapshot,
            isolation_level=isolation_level,
            readonly=readonly
        )

    def commit(self):
        return self.client.commit()

    def rollback(self):
        return self.client.rollback()

    def table_exists(self, table: Optional[str] = None) -> bool:
        if not table:
            table = self.table
        if not table:
            raise ValueError("Table should be given.")

        sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \"%s\";"
        with self.execute(sql, params=(table,), buffered=True) as cur:
            return cur.fetchone()[0] == 1

    def find(
            self,
            where: Optional[str] = None,
            projection: Union[List[str], str] = "*",
            table: Optional[str] = None
    ):
        if not table:
            table = self.table
        if not table:
            raise ValueError("Table should be given.")

        sql = f"SELECT {projection} FROM {table}"
        if where:
            sql += " WHERE " + where
        sql += ";"
        with self.execute(sql, dictionary=True) as cur:
            for doc in cur:
                yield doc
            ret = cur.close()
            return ret

    def insert(self, doc_or_docs: dict | list[dict], table: Optional[str] = None):
        if isinstance(doc_or_docs, list):
            return self.insert_many(doc_or_docs, table)
        else:
            return self.insert_one(doc_or_docs, table)

    def insert_one(self, doc: Dict[str, Any], table: Optional[str] = None):
        if not table:
            table = self.table
        if not table:
            raise ValueError("Table should be given.")

        fields = []
        placeholders = []
        values = []
        for k, v in doc.items():
            fields.append(k)
            placeholders.append("%s")
            values.append(v)
        fields = ", ".join(fields)
        placeholders = ", ".join(placeholders)

        sql = f"INSERT INTO {table} ({fields}) VALUES ({placeholders});"
        cur = self.execute(sql, params=values)
        return cur.close()

    def insert_many(self, docs: List[Dict[str, Any]], table: Optional[str] = None):
        if not docs:
            return True

        if not table:
            table = self.table
        if not table:
            raise ValueError("Table should be given.")

        fields = list(docs[0].keys())
        field_list = ", ".join(fields)

        # (%s, %s, ...), (%s, %s, ...) ...
        placeholders_per_row = "(" + ", ".join(["%s"] * len(fields)) + ")"
        placeholders_all = ", ".join([placeholders_per_row] * len(docs))

        sql = f"INSERT INTO {table} ({field_list}) VALUES {placeholders_all};"

        params = []
        for doc in docs:
            params.extend([doc.get(f) for f in fields])

        cur = self.execute(sql, params=params)
        return cur.close()

    # noinspection PyShadowingBuiltins
    def update(self, set: str, where: str, table: Optional[str] = None):
        if not table:
            table = self.table
        if not table:
            raise ValueError("Table should be given.")

        sql = f"UPDATE {table} SET {set} WHERE {where};"
        cur = self.execute(sql)
        return cur.close()

    def delete(self, where: str, table: Optional[str] = None):
        if not table:
            table = self.table
        if not table:
            raise ValueError("Table should be given.")

        sql = f"DELETE FROM {table} WHERE {where};"
        cur = self.execute(sql)
        return cur.close()


class MySQLIterator(DocIterator):

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        url = URL.ensure_url(url)
        if url.scheme != "mysql":
            raise ValueError(f"Unsupported scheme '{url.scheme}'.")
        return cls(url)

    def __init__(self, url: Union[str, URL]):
        super().__init__()

        url = URL.ensure_url(url)
        self.client = LazyMySQLClient.from_url(url)
        _, self.table = url.get_database_and_table()

        self._cursor = None
        self._exhausted = False
        self._count = None

    def __len__(self):
        if self._count is None:
            sql = f"SELECT COUNT(*) FROM {self.table};"
            with self.client.execute(sql) as cur:
                self._count = cur.fetchone()[0]
        return self._count

    def __iter__(self):
        if self._cursor is None:
            proj = ",".join(self.fields) if self.fields else "*"
            sql = f"SELECT {proj} FROM {self.table};"
            self._cursor = self.client.execute(sql, dictionary=True)
            self._exhausted = False
        return self

    def __next__(self):
        if self._exhausted:
            raise StopIteration()

        doc = self._cursor.fetchone()
        if doc is None:
            self._exhausted = True
            self.close()
            raise StopIteration()
        return doc

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


class MySQLReader(DocReader):

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        url = URL.ensure_url(url)

        if not url.scheme in {"mysql"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        assert isinstance(url.address, Address)

        return MySQLReader(url)

    def __init__(
            self,
            url: Union[str, URL],
            key_field="id"
    ) -> None:
        url = URL.ensure_url(url)
        self.client = LazyMySQLClient.from_url(url)
        _, self.table = url.get_database_and_table()

        if url.parameters:
            params = url.parameters
            if "key_field" in params:
                key_field = params["key_field"]
            elif "keyField" in params:
                key_field = params["keyField"]

        self.key_field = key_field

        self.key_list = self._fetch_keys()

    def _fetch_keys(self):
        sql = f"SELECT {self.key_field} FROM {self.table};"
        with self.client.execute(sql) as cur, self.client:
            key_list = [row[0] for row in tqdm(cur, leave=False)]
        return key_list

    def __len__(self):
        return len(self.key_list)

    def __getitem__(self, idx: int):
        key = self.key_list[idx]
        sql = f"SELECT * FROM {self.table} WHERE {self.key_field}='{key}';"
        with self.client.execute(sql, dictionary=True, buffered=True) as cur:
            return cur.fetchone()

    def close(self):
        if hasattr(self, "client"):
            self.client.close()

    def __del__(self):
        self.close()

    def read(self, key):
        sql = f"SELECT * FROM {self.table} WHERE {self.key_field}='{key}';"
        with self.client.execute(sql, dictionary=True, buffered=True) as cur:
            return cur.fetchone()


class MySQLWriter(DocWriter):

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        url = URL.ensure_url(url)

        if not url.scheme in {"mysql"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        assert isinstance(url.address, Address)

        return MySQLWriter(url)

    def __init__(
            self,
            url: Union[str, URL],
            verbose: bool = True
    ):
        url = URL.ensure_url(url)
        self.client = LazyMySQLClient.from_url(url)
        _, self.table = url.get_database_and_table()

        self.verbose = verbose

        self._table_exists = None

    def write(self, doc: Mapping[str, Any]):
        if not self.client.table_exists(self.table):
            self.create_table_from_doc(doc)

        fields = []
        placeholders = []
        values = []
        for k, v in doc.items():
            fields.append(k)
            placeholders.append("%s")
            values.append(v)
        fields = ", ".join(fields)
        placeholders = ", ".join(placeholders)

        sql = f"INSERT INTO {self.table} ({fields}) VALUES ({placeholders});"
        cur = self.client.execute(sql, params=values)
        return cur.close()

    def create_table_from_doc(self, doc: Mapping[str, Any]):
        fields = []
        for field, value in doc.items():
            _type = "TEXT"
            if isinstance(value, int):
                _type = "BIGINT"
            elif isinstance(value, float):
                _type = "DOUBLE"
            elif isinstance(value, bool):
                _type = "BOOLEAN"
            elif isinstance(value, datetime):
                _type = "DATETIME"
            fields.append((field, _type))
        fields = ", ".join(f"`{field}` {_type}" for field, _type in fields)

        sql = (
            f"CREATE TABLE IF NOT EXISTS `{self.table}` ("
            f"`id` INT NOT NULL AUTO_INCREMENT, "
            f"{fields}, "
            f"PRIMARY KEY (`id`)"
            f");"
        )
        cur = self.client.execute(sql)
        return cur.close()

    def close(self):
        if hasattr(self, "client"):
            self.client.close()

    def __del__(self):
        self.close()
