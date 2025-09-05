# !/usr/bin/env python3

__author__ = "xi"

from argparse import ArgumentParser

from libdata.redis import LazyRedisClient


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--url",
        required=True,
        help="URL example: \"redis://default:password@host1:port1,host2:port2,host3:port3/?service_name=xxx\""
    )
    args = parser.parse_args()

    url = args.url
    print(url)

    client = LazyRedisClient.from_url(url)

    print("> List keys")
    for i, k in enumerate(client.scan_iter("*")):
        print(k)
        if i == 5:
            print("...")
            break
    print()

    print("> Set a string value.")
    client.set("__aloha__", url[::-1].upper())
    print()

    print("> Check if the sample exists.")
    print("Exists." if client.exists("__aloha__") else "Doesn't exist.")
    print()

    print("> Get the value.")
    value = client.get("__aloha__")
    print(value)
    print()

    print("> Delete the sample.")
    client.delete("__aloha__")
    print()

    print("> Check if the sample exists.")
    print("Exists." if client.exists("__aloha__") else "Doesn't exist.")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
