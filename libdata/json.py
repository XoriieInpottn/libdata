#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "JSONReader",
]

import json
from typing import Union

import yaml

from libdata.common import DocReader, ParsedURL


class JSONReader(DocReader):

    @staticmethod
    @DocReader.register("json")
    @DocReader.register("yaml")
    @DocReader.register("yml")
    def from_url(url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"json", "yaml", "yml"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        return JSONReader(path=url.path, **url.params)

    def __init__(
            self,
            path: str,
            encoding: str = "UTF-8",
            key_field: str = "id"
    ) -> None:
        self.path = path
        self.encoding = encoding
        self.key_field = key_field

        with open(self.path, "rt", encoding=self.encoding) as f:
            self.doc_list = json.load(f) if path.endswith(".json") else yaml.safe_load(f)
        if not isinstance(self.doc_list, list):
            raise ValueError(
                f"The content should be a list of documents. "
                f"Expect \"list\", got \"{type(self.doc_list)}\"."
            )
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
