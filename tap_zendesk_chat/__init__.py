#!/usr/bin/env python3
import os
import singer
from singer.utils import parse_args
from singer.catalog import Catalog
from .context import Context
from .discover import discover
from .sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()

def main_impl():
    args = parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover(args.config).dump()
        print()
    else:
        catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(args.config)
        ctx = Context(args.config, args.state, catalog)
        sync(ctx)

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
