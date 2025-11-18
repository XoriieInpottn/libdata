#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "Address",
    "URL",
]

import io
import re
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import quote
from urllib.parse import unquote

from pydantic import BaseModel
from pydantic import Field

RE_SCHEME = re.compile("^(?P<value>[A-Za-z0-9+.]+)://")
RE_AUTH = re.compile("(?P<value>[^@]+)@")
RE_ADDRESS = re.compile("(?P<value>[^/?#]+)")
RE_PATH = re.compile("(?P<value>/?[^?#]*)")
RE_PARAMS = re.compile("\?(?P<value>[^#]+)")
RE_FRAGS = re.compile("(#(?P<value>.+))")


class Address(BaseModel):
    """Address with a host name (or IP) and a port number.
    An address object represents a single server.
    A cluster is represented as a list of address objects.
    """
    host: str = Field(
        title="Host name (or IP)",
        description="Host name or IP address of the server."
    )
    port: Optional[int] = Field(
        title="Port number",
        description=(
            "The port number of the service. "
            "Leaving the port number empty means using the pre-defined default port of the service"
        ),
        default=None
    )


class URL(BaseModel):
    """Universal Resource Locator."""

    scheme: Optional[str] = Field(
        title="Scheme",
        description="The scheme of the resource. Empty scheme means local file(s).",
        default=None
    )
    username: Optional[str] = Field(
        title="User name",
        description="The user name used to access the resource.",
        default=None
    )
    password: Optional[str] = Field(
        title="Password",
        description="The password of the user.",
        default=None
    )
    address: Optional[Union[Address, List[Address]]] = Field(
        title="Address(es)",
        description=(
            "The address(es) of the resource. "
            "Empty address means local resource. "
            "List of addresses means the resource is served by a cluster."
        ),
        default=None
    )
    path: Optional[str] = Field(
        title="Path",
        description="The path to locate the resource.",
        default=None
    )
    parameters: Optional[Dict[str, str]] = Field(
        title="Parameters",
        description="The parameters to access the resource.",
        default=None
    )
    fragments: Optional[str] = Field(
        title="Fragments",
        description="The fragments fo the resource.",
        default=None
    )

    @classmethod
    def from_string(cls, url_str: str):
        scheme = None
        auth = None
        address = None
        path = None
        params = None
        fragments = None

        pos = 0
        if m := RE_SCHEME.match(url_str, pos):
            scheme = m.group("value")
            pos = m.span()[1]

        if scheme and scheme not in {"file", "local"}:
            if m := RE_AUTH.match(url_str, pos):
                auth = m.group("value")
                pos = m.span()[1]

            if m := RE_ADDRESS.match(url_str, pos):
                address = m.group("value")
                pos = m.span()[1]

            if m := RE_PATH.match(url_str, pos):
                path = m.group("value")
                pos = m.span()[1]

            if m := RE_PARAMS.match(url_str, pos):
                params = m.group("value")
                pos = m.span()[1]

            if m := RE_FRAGS.match(url_str, pos):
                fragments = m.group("value")
        else:
            if m := RE_PATH.match(url_str, pos):
                path = m.group("value")
                pos = m.span()[1]

            if m := RE_PARAMS.match(url_str, pos):
                params = m.group("value")
                pos = m.span()[1]

            if m := RE_FRAGS.match(url_str, pos):
                fragments = m.group("value")

        if scheme:
            scheme = unquote(scheme)

        username = None
        password = None
        if auth:
            i = auth.find(":")
            if i > 0:
                username = unquote(auth[:i])
                password = unquote(auth[i + 1:])
            else:
                username = unquote(auth)

        if address:
            address_list = []
            for a in address.split(","):
                i = a.find(":")
                if i > 0:
                    host = unquote(a[:i])
                    port = int(a[i + 1:])
                else:
                    host = unquote(a)
                    port = None
                address_list.append(Address(host=host, port=port))
            address = address_list[0] if len(address_list) == 1 else address_list

        if path:
            path = unquote(path)

        parameters = {}
        if params:
            for p in params.split("&"):
                i = p.find("=")
                if i > 0:
                    name = unquote(p[:i])
                    value = unquote(p[i + 1:])
                else:
                    name = unquote(p)
                    value = ""
                parameters[name] = value

        if fragments:
            fragments = unquote(fragments)

        return cls(
            scheme=scheme,
            username=username,
            password=password,
            address=address,
            path=path,
            parameters=parameters,
            fragments=fragments
        )

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        return super().__repr__()

    def to_string(self):
        buffer = io.StringIO()

        if self.scheme:
            buffer.write(self.scheme)
            buffer.write("://")

        if self.username:
            buffer.write(quote(self.username, safe=""))
            if self.password:
                buffer.write(":")
                buffer.write(quote(self.password, safe=""))
            buffer.write("@")

        if self.address:
            address_list: List[Address] = (
                self.address
                if isinstance(self.address, List)
                else [self.address]
            )
            for i, address in enumerate(address_list):
                if i != 0:
                    buffer.write(",")
                if address.host:
                    buffer.write(quote(address.host, safe=""))
                    if address.port:
                        buffer.write(":")
                        buffer.write(str(address.port))

        if self.path:
            if not self.path.startswith("/"):
                buffer.write("/")
            buffer.write(quote(self.path))

        if self.parameters:
            buffer.write("?")
            for i, (name, value) in enumerate(self.parameters.items()):
                if i != 0:
                    buffer.write("&")
                buffer.write(quote(name, safe=""))
                buffer.write("=")
                buffer.write(quote(value, safe=""))

        if self.fragments:
            buffer.write("#")
            buffer.write(quote(self.fragments, safe=""))

        return buffer.getvalue()

    @staticmethod
    def ensure_url(url: Union["URL", str, bytes]) -> "URL":
        if isinstance(url, URL):
            return url
        elif isinstance(url, str):
            return URL.from_string(url)
        elif isinstance(url, bytes):
            return URL.from_string(url.decode())

    def split_scheme(self) -> List[str]:
        if self.scheme:
            return self.scheme.split("+")
        return []

    def split_path(self) -> List[str]:
        if self.path:
            return self.path.strip("/").split("/")
        return []

    def get_database_and_table(self) -> Tuple[str, str]:
        database = None
        table = None

        if self.path:
            path_list = self.split_path()
            if len(path_list) == 1:
                database = path_list[0] or None
                table = None
            elif len(path_list) == 2:
                database = path_list[0] or None
                table = path_list[1] or None
            else:
                raise ValueError(
                    "\"path\" should only contains database and collection. "
                    "Expect \"/{database_name}/{table_name}\", "
                    f"got \"{self.path}\"."
                )

        return database, table

    def update_parameters(self, params: Dict[str, str]):
        if self.parameters:
            self.parameters.update(params)
        else:
            self.parameters = params
        return self

    def append_path(self, path: str):
        if self.path:
            self.path = self.path.rstrip("/") + "/" + path.lstrip("/")
        else:
            self.path = path
        return self

    def __add__(self, address: str):
        assert isinstance(address, str)

        m = RE_ADDRESS.match(address)
        if not m:
            raise ValueError(f"Invalid address `{address}`.")

        address = m.group("value")
        address_list = []
        for a in address.split(","):
            i = a.find(":")
            if i > 0:
                host = unquote(a[:i])
                port = int(a[i + 1:])
            else:
                host = unquote(a)
                port = None
            address_list.append(Address(host=host, port=port))

        if self.address is None:
            address = address_list[0] if len(address_list) == 1 else address_list
        elif isinstance(self.address, Address):
            address = [self.address, *address_list]
        elif isinstance(self.address, list):
            address = self.address + address_list

        return self.model_copy(update={"address": address}, deep=True)

    def __truediv__(self, path: str):
        assert isinstance(path, str)

        if self.path:
            path = self.path.rstrip("/") + "/" + path.lstrip("/")

        return self.model_copy(update={"path": path}, deep=True)

    def __rtruediv__(self, path: str):
        assert isinstance(path, str)

        if self.path:
            path = "/" + path.strip() + "/" + self.path.lstrip("/")

        return self.model_copy(update={"path": path}, deep=True)
