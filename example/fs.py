#!/usr/bin/env python3

__author__ = "xi"

from argparse import ArgumentParser

from libdata.fs import LazyFSClient


def main():
    parser = ArgumentParser()
    parser.add_argument("--dir_url", required=True, help="URL example: \"s3+http://key:secret@host:port/dir_path\"")
    args = parser.parse_args()

    dir_url = args.dir_url
    print(dir_url)

    print("> Get the filelist of the remote dir.")
    fs = LazyFSClient.from_url(dir_url)
    for name in fs.listdir():
        print(name)
    print()

    print("> Open the remote file and write content to it.")
    with fs.open("test.txt", mode="w") as f:
        f.write("The first line")
        f.write("\n")
        f.write("The second line")
    print()

    print("> Check if there is a new file named `test.txt`.")
    fs = LazyFSClient.from_url(dir_url)
    for name in fs.listdir():
        print(name)
    print()

    print("> Check the file content.")
    with fs.open("test.txt", mode="r") as f:
        print(f.read())
    print()

    print("> Append a new line to that file with mode=\"a\".")
    with fs.open("test.txt", mode="a") as f:
        f.write("\n")
        f.write("The third line")
    print()

    print("> Check the file content.")
    with fs.open("test.txt", mode="r") as f:
        print(f.read())
    print()

    print("> Remove the file.")
    fs.rm("test.txt")
    print()

    print("> Check if the file `test.txt` has been removed.")
    fs = LazyFSClient.from_url(dir_url)
    for name in fs.listdir():
        print(name)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
