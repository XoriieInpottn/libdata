#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "Address",
    "URL",
]

import io
import re
from typing import Dict, List, Optional, Union
from urllib.parse import quote, unquote

from pydantic import BaseModel, Field

URL_REGEXP = re.compile(
    r"^((?P<scheme>[A-Za-z0-9]+)://)?"
    r"((?P<auth>[^@]+)@)?"
    r"(?P<address>[^/?#]+)?"
    r"(?P<path>/[^?#]+)?"
    r"(\?(?P<params>[^#]+))?"
    r"(#(?P<frags>.+))?"
)


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
        matched = URL_REGEXP.match(url_str)
        if not matched:
            raise ValueError(f"\"{url_str}\" is not a valid URL string.")

        matched_groups = matched.groupdict()
        scheme = matched_groups["scheme"]
        auth = matched_groups["auth"]
        address = matched_groups["address"]
        path = matched_groups["path"]
        params = matched_groups["params"]
        fragments = matched_groups["frags"]

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

        parameters = None
        if params:
            parameters = {}
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

    def to_string(self):
        buffer = io.StringIO()

        if self.scheme:
            buffer.write(quote(self.scheme))
            buffer.write("://")

        if self.username:
            buffer.write(quote(self.username))
            if self.password:
                buffer.write(":")
                buffer.write(quote(self.password))
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
                    buffer.write(quote(address.host))
                    if address.port:
                        buffer.write(":")
                        buffer.write(str(address.port))

        if self.path:
            if not self.path.startswith("/"):
                buffer.write("/")
            buffer.write(quote(self.path))

        if self.parameters:
            buffer.write("?")
            for name, value in self.parameters.items():
                buffer.write(quote(name))
                buffer.write("=")
                buffer.write(quote(value))

        if self.fragments:
            buffer.write("#")
            buffer.write(quote(self.fragments))

        return buffer.getvalue()
