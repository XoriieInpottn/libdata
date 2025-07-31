#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "JSONLReader",
    "JSONLWriter",
]

import json
import os
from typing import Union

from tqdm import tqdm

from libdata.common import DocReader, DocWriter
from libdata.url import URL


class JSONLReader(DocReader):

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        url = URL.ensure_url(url)

        if not url.scheme in {"json", "jsonl"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        return JSONLReader(path=url.path, **url.parameters)

    def __init__(
            self,
            path: str,
            encoding: str = "UTF-8",
            key_field: str = "id",
            verbose: bool = True
    ) -> None:
        self.path = path
        self.encoding = encoding
        self.key_field = key_field
        self.verbose = verbose

        with open(self.path, "rt", encoding=self.encoding) as f:
            it = tqdm(f, leave=False) if self.verbose else f
            self.doc_list = [
                json.loads(line)
                for line in it
                if line.strip()
            ]
        self.index = None

    def __len__(self):
        return len(self.doc_list)

    def __getitem__(self, idx: int):
        return self.doc_list[idx]

    def read(self, key):
        if self.index is None:
            self.index = {
                doc[self.key_field]: doc
                for doc in self.doc_list
            }
        return self.index[key]


class JSONLWriter(DocWriter):

    @classmethod
    def from_url(cls, url: Union[str, URL]):
        url = URL.ensure_url(url)

        if not url.scheme in {"json", "jsonl"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        return JSONLWriter(path=url.path, **url.parameters)

    def __init__(self, path: str, replace: bool = False):
        self.path = path
        self.replace = replace

        self._fp = None

    def write(self, doc):
        if self._fp is None:
            if os.path.exists(self.path):
                if self.replace:
                    os.remove(self.path)
                else:
                    raise FileExistsError(self.path)
            self._fp = open(self.path, "wt")

        self._fp.write(json.dumps(doc, indent=None))
        self._fp.write("\n")

    def close(self):
        if hasattr(self, "_fp") and self._fp is not None:
            self._fp.close()
            self._fp = None
