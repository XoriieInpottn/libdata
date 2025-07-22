#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "Resource",
    "JSONResource",
    "MongoResource",
]

import abc
import time
from threading import Lock
from typing import Dict, MutableMapping, Union

import yaml

from libdata import LazyMongoClient
from libdata.url import URL


class Resource(MutableMapping, abc.ABC):

    @classmethod
    def from_url(cls, url: Union[str, URL], **kwargs) -> "Resource":
        url = URL.ensure_url(url)
        if url.scheme in {None, "", "json", "yaml", "yml"}:
            return JSONResource(url.path)
        elif url.scheme in {"mongo", "mongodb"}:
            return MongoResource(url, **kwargs)
        else:
            raise NotImplementedError(f"Scheme \"{url.scheme}\" is not supported.")


class JSONResource(Resource):

    def __init__(self, path: str):
        self.path = path

        with open(self.path, "r") as f:
            self.doc = yaml.load(f, yaml.SafeLoader)

        if not isinstance(self.doc, Dict):
            raise RuntimeError(
                f"Expect a JSON object, "
                f"got {type(self.doc)}."
            )

        self.lock = Lock()

    def __setitem__(self, name, value):
        raise NotImplementedError(f"Cannot set values to a JSON resource `{self.path}`.")

    def __delitem__(self, name):
        raise NotImplementedError(f"Cannot delete from a JSON resource `{self.path}`.")

    def __getitem__(self, name):
        with self.lock:
            return self.doc[name]

    def __len__(self):
        with self.lock:
            return len(self.doc)

    def __iter__(self):
        with self.lock:
            return iter(self.doc)


class MongoResource(Resource):

    def __init__(self, url: Union[str, URL], cache_timeout: float = 60):
        self.url = url
        self.cache_timeout = cache_timeout

        mongo = LazyMongoClient.from_url(url)
        doc = mongo.find_one()
        if doc is not None:
            if not ("name" in doc and "value" in doc):
                raise RuntimeError(f"`{url}` should contain `name` and `value`.")
        mongo.get_collection().create_index("name")

        self.lock = Lock()
        self.cache = {}

    def __setitem__(self, name: str, value):
        with self.lock:
            mongo = LazyMongoClient.from_url(self.url)
            mongo.update_one(
                {"name": name},
                {"$set": {"name": name, "value": value}},
                upsert=True
            )
            if name in self.cache:
                del self.cache[name]

    def __delitem__(self, name: str):
        with self.lock:
            mongo = LazyMongoClient.from_url(self.url)
            mongo.delete_one({"name": name})
            if name in self.cache:
                del self.cache[name]

    def __getitem__(self, name: str):
        with self.lock:
            if name not in self.cache:
                self.cache[name] = (self._find_item(name), time.time())
            else:
                value, last_update = self.cache[name]
                if time.time() - last_update > self.cache_timeout:
                    self.cache[name] = (self._find_item(name), time.time())
            return self.cache[name][0]

    def _find_item(self, name: str):
        mongo = LazyMongoClient.from_url(self.url)
        doc = mongo.find_one({"name": name})
        if doc is None:
            raise KeyError(name)
        return doc["value"]

    def __len__(self):
        mongo = LazyMongoClient.from_url(self.url)
        return mongo.count_documents()

    def __iter__(self):
        mongo = LazyMongoClient.from_url(self.url)
        for doc in mongo.find():
            yield doc["name"]
