#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "MySQLReader",
]

from datetime import datetime
from typing import Any, Mapping, Optional, Union

from tqdm import tqdm

from libdata.common import DocReader, DocWriter, ParsedURL


class MySQLReader(DocReader):

    @staticmethod
    @DocReader.register("mysql")
    def from_url(url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"mysql"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")
        if url.database is None or url.table is None:
            raise ValueError(f"Invalid path \"{url.path}\" for database.")

        return MySQLReader(
            host=url.hostname,
            port=url.port,
            user=url.username,
            password=url.password,
            database=url.database,
            table=url.table,
            **url.params
        )

    def __init__(
            self,
            database: str,
            table: str,
            host: Optional[str] = "127.0.0.1",
            port: Optional[int] = 3306,
            user: str = "root",
            password: str = None,
            key_field="id"
    ) -> None:
        self.database = database
        self.table = table
        self.host = host if host is not None else "127.0.0.1"
        self.port = port if port is not None else 3306
        self.user = user
        self.password = password
        self.key_field = key_field

        self.key_list = self._fetch_keys()
        self.conn = None

    def _get_conn(self):
        from mysql.connector import MySQLConnection
        return MySQLConnection(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def _fetch_keys(self):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {self.key_field} FROM {self.table};")
                return [row[0] for row in tqdm(cur, leave=False)]

    def __len__(self):
        return len(self.key_list)

    def __getitem__(self, idx: int):
        key = self.key_list[idx]

        if self.conn is None:
            self.conn = self._get_conn()

        with self.conn.cursor(dictionary=True) as cur:
            cur.execute(f"SELECT * FROM {self.table} WHERE {self.key_field}='{key}';")
            return cur.fetchone()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()

    def read(self, key):
        if self.conn is None:
            self.conn = self._get_conn()

        with self.conn.cursor(dictionary=True) as cur:
            cur.execute(f"SELECT * FROM {self.table} WHERE {self.key_field}='{key}';")
            return cur.fetchone()


class MySQLWriter(DocWriter):

    @staticmethod
    @DocWriter.register("mysql")
    def from_url(url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"mysql"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")
        if url.database is None or url.table is None:
            raise ValueError(f"Invalid path \"{url.path}\" for database.")

        return MySQLWriter(
            host=url.hostname,
            port=url.port,
            user=url.username,
            password=url.password,
            database=url.database,
            table=url.table,
            **url.params
        )

    def __init__(
            self,
            database: str,
            table: str,
            host: Optional[str] = "127.0.0.1",
            port: Optional[int] = 3306,
            user: str = "root",
            password: str = None,
            verbose: bool = True
    ):
        self.database = database
        self.table = table
        self.host = host if host is not None else "127.0.0.1"
        self.port = port if port is not None else 3306
        self.user = user
        self.password = password
        self.verbose = verbose

        self._conn = None
        self._table_exists = None

    def get_connection(self):
        if self._conn is None:
            from mysql.connector import MySQLConnection
            self._conn = MySQLConnection(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        return self._conn

    def write(self, doc: Mapping[str, Any]):
        if not self.table_exists():
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

        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {self.table} ({fields}) VALUES ({placeholders});", values)
        conn.commit()

    def table_exists(self):
        if self._table_exists is None:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s", (self.table,))
                self._table_exists = cur.fetchone()[0] == 1
        return self._table_exists

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

        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{self.table}` ("
                f"`id` INT NOT NULL AUTO_INCREMENT, "
                f"{fields}, "
                f"PRIMARY KEY (`id`)"
                f");"
            )
        conn.commit()

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()
