#!/usr/bin/env python3
import os
import singer
from singer.utils import parse_args,handle_top_exception
from singer.catalog import Catalog
from .context import Context
from .discover import discover
from .sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


@handle_top_exception(LOGGER)
def main():
    """performs sync and discovery."""
    args = parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover(args.config).dump()
    else:
        ctx = Context(args.config, args.state, args.catalog or discover(args.config))
        sync(ctx,args.catalog or discover(args.config),args.state)


if __name__ == "__main__":
    main()