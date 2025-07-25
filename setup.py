#!/usr/bin/env python3

__author__ = "xi"

from setuptools import setup

if __name__ == "__main__":
    with open("README.md") as file:
        long_description = file.read()
    setup(
        name="libdata",
        packages=[
            "libdata",
        ],
        version="0.22",
        description="Unified interface to access data.",
        long_description_content_type="text/markdown",
        long_description=long_description,
        license="MIT",
        author="xi",
        author_email="gylv@mail.ustc.edu.cn",
        url="https://github.com/XoriieInpottn/libdata",
        platforms="any",
        classifiers=[
            "Programming Language :: Python :: 3",
        ],
        include_package_data=True,
        zip_safe=True,
        install_requires=[
            "pydantic",
            "tqdm",
            "PyYAML",
            "numpy",
            "scipy",
            "pymongo",
            "pymilvus",
            "redis",
        ]
    )
