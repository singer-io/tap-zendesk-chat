#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="tap-zendesk-chat",
    version="0.5.1",
    description="Singer.io tap for extracting data from the Zendesk Chat API",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_zendesk_chat"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.32.3",
    ],
    extras_require={"dev": ["pylint", "ipdb", "nose"]},
    entry_points="""
    [console_scripts]
    tap-zendesk-chat=tap_zendesk_chat:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data={"schemas": ["tap_zendesk_chat/schemas/*.json"]},
    include_package_data=True,
)