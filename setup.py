#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="tap-zendesk-chat",
      version="0.1.1",
      description="Singer.io tap for extracting data from the Zendesk Chat API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_zendesk_chat"],
      install_requires=[
          "singer-python>=3.2.0",
          "requests",
          "backoff",
          "attrs",
      ],
      entry_points="""
          [console_scripts]
          tap-zendesk-chat=tap_zendesk_chat:main
      """,
      packages=["tap_zendesk_chat"],
      package_data = {
          "schemas": ["tap_zendesk_chat/schemas/*.json"]
      },
      include_package_data=True,
)
