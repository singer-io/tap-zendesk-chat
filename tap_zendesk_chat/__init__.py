#!/usr/bin/env python3
import os
import singer
from singer import metrics, utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from requests.exceptions import HTTPError
from . import streams as streams_
from .streams import STREAM_OBJECTS
from .context import Context
from .http import Client
from .sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def ensure_credentials_are_authorized(client):
    # The request will throw an exception if the credentials are not authorized
    client.request(STREAM_OBJECTS['departments'].tap_stream_id)


def is_account_endpoint_authorized(client):
    # The account endpoint is restricted to zopim accounts, meaning integrated
    # Zendesk accounts will get a 403 for this endpoint.
    try:
        client.request(STREAM_OBJECTS['account'].tap_stream_id)
    except HTTPError as e:
        if e.response.status_code == 403:
            LOGGER.info(
                "Ignoring 403 from account endpoint - this must be an "
                "integrated Zendesk account. This endpoint will be excluded "
                "from discovery."
            )
            return False
        raise
    return True


def discover(config):
    client = Client(config)
    ensure_credentials_are_authorized(client)
    include_account_stream = is_account_endpoint_authorized(client)
    streams = []
    for _, stream in STREAM_OBJECTS.items():
        if (not include_account_stream
            and stream.tap_stream_id == STREAM_OBJECTS['account'].tap_stream_id):
            continue
        raw_schema = load_schema(stream.tap_stream_id)
        schema = Schema.from_dict(raw_schema)
        streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
            metadata=metadata.get_standard_metadata(
                schema=raw_schema,
                schema_name=stream.tap_stream_id,
                key_properties=stream.pk_fields)
        ))
    return Catalog(streams)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover(args.config).dump()
    elif args.catalog:
        ctx = Context(args.config, args.state, args.catalog)
        sync(ctx)
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
