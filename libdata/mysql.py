#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "MySQLReader",
]

from typing import Union

from tqdm import tqdm

from libdata.common import DocReader, ParsedURL


class MySQLReader(DocReader):

    @staticmethod
    @DocReader.factory.register("mysql")
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
            database,
            table,
            host: str = "127.0.0.1",
            port: int = 3306,
            user: str = "root",
            password: str = None,
            primary_key="id"
    ) -> None:
        self.database = database
        self.table = table
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.primary_key = primary_key

        self.key_list = self._fetch_keys()
        self.conn = None

    def _fetch_keys(self):
        from mysql.connector import MySQLConnection
        with MySQLConnection(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {self.primary_key} FROM {self.table};")
                return [row[0] for row in tqdm(cur, leave=False)]

    def __len__(self):
        return len(self.key_list)

    def __getitem__(self, idx: int):
        key = self.key_list[idx]

        if self.conn is None:
            from mysql.connector import MySQLConnection
            self.conn = MySQLConnection(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )

        with self.conn.cursor(dictionary=True) as cur:
            cur.execute(f"SELECT * FROM {self.table} WHERE {self.primary_key}='{key}';")
            return cur.fetchone()

    def close(self):
        if self.conn is not None:
            # self.conn.close()
            self.conn = None

    def __del__(self):
        self.close()
