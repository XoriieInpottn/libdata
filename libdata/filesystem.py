#!/usr/bin/env python3

__author__ = "xi"

import json
from typing import Union

import fsspec
from fsspec import AbstractFileSystem

from libdata.url import URL


class LazyFileSystem:

    @classmethod
    def __new__(cls, url: Union[str, URL]) -> AbstractFileSystem:
        return filesystem(url)

    @staticmethod
    def from_url(url: Union[str, URL]) -> AbstractFileSystem:
        return filesystem(url)


def filesystem(url: Union[str, URL]) -> AbstractFileSystem:
    url = URL.ensure_url(url)

    schemes = url.split_scheme()
    fs_protocol = schemes[0]
    backend_protocol = schemes[-1]

    key = url.username
    secret = url.password
    kwargs = {}
    if url.parameters:
        for name, value in url.parameters.items():
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
            kwargs[name] = value
    if url.address:
        kwargs["endpoint_url"] = URL(scheme=backend_protocol, address=url.address).to_string()

    return fsspec.filesystem(
        fs_protocol,
        key=key,
        secret=secret,
        client_kwargs=kwargs,
    )


def listdir(url):
    url = URL.ensure_url(url)
    print(repr(url))
    exit()

    fs = filesystem(url)
    path = url.path if url.path else ""
    if url.address:
        path = path.lstrip("/")
    for item in fs.listdir(path, detail=False):
        yield item


def open(url, mode="rb"):
    url = URL.ensure_url(url)
    fs = filesystem(url)
    path = url.path.lstrip("/") if url.path else ""
    return fs.open(path, mode=mode)
