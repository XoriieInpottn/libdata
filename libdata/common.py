#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "ParsedURL",
    "DocReader",
    "DocWriter",
]

import abc
import re
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

from libentry import Factory, literal_eval
from pydantic import BaseModel, Field


class ParsedURL(BaseModel):
    scheme: Optional[str] = Field(default=None)
    hostname: Optional[str] = Field(default=None)
    port: Optional[int] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    path: Optional[str] = Field(default=None)
    query: Optional[str] = Field(default=None)
    database: Optional[str] = Field(default=None)
    table: Optional[str] = Field(default=None)
    params: Optional[Dict[str, Any]] = Field(default=None)

    @classmethod
    def from_string(cls, url: str) -> "ParsedURL":
        parsed = urlparse(url)

        # database and table
        matched = re.search(r"^/(\w+)/(.+)$", parsed.path)
        database, table = None, None
        if matched is not None:
            database, table = matched.groups()

        # params
        params = {}
        for param in parsed.query.split("&"):
            if not param:
                continue
            try:
                name, value = param.split("=")
            except ValueError:
                raise ValueError(f"Invalid url parameter \"{param}\".")
            params[name] = literal_eval(value)

        return ParsedURL(
            scheme=parsed.scheme,
            hostname=parsed.hostname,
            port=parsed.port,
            username=parsed.username,
            password=parsed.password,
            path=parsed.path,
            query=parsed.query,
            database=database,
            table=table,
            params=params,
        )


class DocReader(abc.ABC):
    """Abstract class for document readers."""

    factory = Factory("DocReader")

    @staticmethod
    def from_url(url: Union[str, ParsedURL]) -> "DocReader":
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if url.scheme not in DocReader.factory:
            raise ValueError(f"Unsupported type \"{url.scheme}\".")

        return DocReader.factory.build(url.scheme, url=url)

    def __iter__(self):
        return (self[idx] for idx in range(len(self)))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass

    @abc.abstractmethod
    def __getitem__(self, idx):
        pass

    @abc.abstractmethod
    def read(self, key):
        pass


class DocWriter(abc.ABC):
    """Abstract class for document writers."""

    factory = Factory("DocWriter")

    @staticmethod
    def from_url(url: Union[str, ParsedURL]) -> "DocWriter":
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if url.scheme not in DocWriter.factory:
            raise ValueError(f"Unsupported type \"{url.scheme}\".")

        return DocWriter.factory.build(url.scheme, url=url)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def write(self, doc):
        pass

    @abc.abstractmethod
    def close(self):
        pass
