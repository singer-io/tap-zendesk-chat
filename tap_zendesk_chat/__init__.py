#!/usr/bin/env python3
import singer
from singer.utils import handle_top_exception, parse_args

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
        sync(ctx)


if __name__ == "__main__":
    main()
