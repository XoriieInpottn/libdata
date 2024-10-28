#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "JSONReader",
    "JSONWriter",
]

import json
import os
from typing import Union

from tqdm import tqdm

from libdata.common import DocReader, DocWriter, ParsedURL


class JSONReader(DocReader):

    @staticmethod
    @DocReader.factory.register("json")
    @DocReader.factory.register("jsonl")
    @DocReader.factory.register("yaml")
    @DocReader.factory.register("yml")
    def from_url(url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"json", "jsonl", "yaml", "yml"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        return JSONReader(path=url.path, **url.params)

    def __init__(self, path: str, encoding: str = "UTF-8", verbose: bool = True):
        self.path = path
        self.encoding = encoding
        self.verbose = verbose
        try:
            self.doc_list = self._load_as_standard()
        except json.JSONDecodeError:
            self.doc_list = self._load_as_jsonl()

    def _load_as_standard(self):
        with open(self.path, "rt", encoding=self.encoding) as f:
            return json.load(f)

    def _load_as_jsonl(self):
        with open(self.path, "rt", encoding=self.encoding) as f:
            it = tqdm(f, leave=False) if self.verbose else f
            return [json.loads(line) for line in it if line.strip()]

    def __len__(self):
        return len(self.doc_list)

    def __getitem__(self, idx: int):
        return self.doc_list[idx]


class JSONWriter(DocWriter):

    @staticmethod
    @DocWriter.factory.register("json")
    @DocWriter.factory.register("jsonl")
    def from_url(url: Union[str, ParsedURL]):
        if not isinstance(url, ParsedURL):
            url = ParsedURL.from_string(url)

        if not url.scheme in {"json", "jsonl", "yaml", "yml"}:
            raise ValueError(f"Unsupported scheme \"{url.scheme}\".")

        return JSONWriter(path=url.path, **url.params)

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
