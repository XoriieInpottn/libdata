#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "filesystem",
    "LazyFSClient",
    "listdir",
    "open",
    "rm",
    "mv",
    "read_bytes",
    "write_bytes",
    "read_text",
    "write_text",
]

from typing import Optional, Union

import fsspec
from fsspec import AbstractFileSystem

from libdata.common import ConnectionPool, LazyClient
from libdata.url import URL


def filesystem(url: Union[str, URL]) -> AbstractFileSystem:
    url = URL.ensure_url(url)

    schemes = url.split_scheme()
    if schemes:
        fs_protocol = schemes[0]
        backend_protocol = schemes[-1]
    else:
        fs_protocol = "local"
        backend_protocol = None

    key = url.username
    secret = url.password
    kwargs = {}
    if url.parameters:
        for name, value in url.parameters.items():
            kwargs[name] = value
    if url.address and backend_protocol:
        kwargs["endpoint_url"] = URL(scheme=backend_protocol, address=url.address).to_string()

    config_kwargs = {}
    if "signature_version" in kwargs:
        config_kwargs["signature_version"] = kwargs["signature_version"]
        del kwargs["signature_version"]

    if "verify" in kwargs:
        kwargs["verify"] = kwargs["verify"].lower() in {"true", "1"}

    return fsspec.filesystem(
        fs_protocol,
        key=key,
        secret=secret,
        client_kwargs=kwargs,
        config_kwargs=config_kwargs
    )


class LazyFSClient(LazyClient[AbstractFileSystem]):

    @staticmethod
    def from_url(url: Union[str, URL]) -> "LazyFSClient":
        return LazyFSClient(url)

    DEFAULT_CONN_POOL_SIZE = 16
    DEFAULT_CONN_POOL = ConnectionPool[AbstractFileSystem](DEFAULT_CONN_POOL_SIZE)

    def __init__(
            self,
            url: Union[str, URL],
            connection_pool: Optional[ConnectionPool] = None
    ):
        super().__init__()
        url = URL.ensure_url(url)
        self.conn_url = URL(
            scheme=url.scheme,
            username=url.username,
            password=url.password,
            address=url.address,
            path=None,
            parameters=url.parameters
        ).to_string()

        if url.path:
            self.base_path = url.path
            if url.address:
                # remote path
                self.base_path = self.base_path.strip("/")
            else:
                self.base_path = self.base_path.rstrip("/")
        else:
            self.base_path = ""

        self.conn_pool = connection_pool if connection_pool else self.DEFAULT_CONN_POOL

    def _connect(self) -> AbstractFileSystem:
        client = self.conn_pool.get(self.conn_url)
        if client is None:
            client = filesystem(self.conn_url)
        return client

    def _disconnect(self, client: AbstractFileSystem):
        client = self.conn_pool.put(self.conn_url, client)
        if client is not None:
            if hasattr(client, "close"):
                client.close()
            elif hasattr(client, "disconnect"):
                client.disconnect()

    def join_path(self, path: Optional[str] = None):
        if path:
            if self.base_path:
                return self.base_path + "/" + path.lstrip("/")
            else:
                return path.lstrip("/")
        else:
            return self.base_path

    def listdir(self, path: Optional[str] = None):
        for item in self.client.listdir(self.join_path(path), detail=False):
            yield item

    def open(self, path: Optional[str] = None, mode: str = "rb"):
        return self.client.open(self.join_path(path), mode=mode)

    def rm(self, path: Optional[str] = None, recursive=False):
        return self.client.rm(self.join_path(path), recursive=recursive)

    def mv(self, path1: str, path2: str, recursive=False):
        return self.client.mv(
            self.join_path(path1),
            self.join_path(path2),
            recursive=recursive
        )


def listdir(url, path: Optional[str] = None):
    return LazyFSClient(url).listdir(path=path)


# noinspection PyShadowingBuiltins
def open(url, mode="rb", path: Optional[str] = None):
    return LazyFSClient(url).open(path=path, mode=mode)


def rm(url, recursive=False, path: Optional[str] = None):
    return LazyFSClient(url).rm(path=path, recursive=recursive)


def mv(url, path1: str, path2: str, recursive=False):
    return LazyFSClient(url).mv(path1=path1, path2=path2, recursive=recursive)


def read_bytes(url: Union[str, URL]) -> bytes:
    client = LazyFSClient.from_url(url)
    with client.open(mode="rb") as f, client:
        return f.read()


def write_bytes(url: Union[str, URL], data: bytes, append: bool = False):
    client = LazyFSClient.from_url(url)
    mode = "ab" if append else "wb"
    with client.open(mode=mode) as f, client:
        return f.write(data)


def read_text(url: Union[str, URL]) -> bytes:
    client = LazyFSClient.from_url(url)
    with client.open(mode="rt") as f, client:
        return f.read()


def write_text(url: Union[str, URL], content: str, append: bool = False):
    client = LazyFSClient.from_url(url)
    mode = "at" if append else "wt"
    with client.open(mode=mode) as f, client:
        return f.write(content)
