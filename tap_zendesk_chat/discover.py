import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from requests.exceptions import HTTPError
from . import streams as streams_
from .http import Client
from .utils import load_schema

LOGGER = singer.get_logger()




def ensure_credentials_are_authorized(client):
    # The request will throw an exception if the credentials are not authorized
    client.request(streams_.DEPARTMENTS.tap_stream_id)


def is_account_endpoint_authorized(client):
    # The account endpoint is restricted to zopim accounts, meaning integrated
    # Zendesk accounts will get a 403 for this endpoint.
    try:
        client.request(streams_.ACCOUNT.tap_stream_id)
    except HTTPError as e:
        if e.response.status_code == 403:
            LOGGER.info(
                "Ignoring 403 from account endpoint - this must be an "
                "integrated Zendesk account. This endpoint will be excluded "
                "from discovery."
            )
            return False
        else:
            raise
    return True


def discover(config):
    client = Client(config)
    ensure_credentials_are_authorized(client)
    include_account_stream = is_account_endpoint_authorized(client)
    catalog = Catalog([])
    for stream in streams_.all_streams:
        if (not include_account_stream
            and stream.tap_stream_id == streams_.ACCOUNT.tap_stream_id):
            continue
        raw_schema = load_schema(stream.tap_stream_id)
        mdata = build_metadata(raw_schema, stream)
        schema = Schema.from_dict(raw_schema)
        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))
    return catalog

def build_metadata(raw_schema, stream):

    mdata = metadata.new()
    metadata.write(mdata, (), 'valid-replication-keys', list(stream.replication_key))
    metadata.write(mdata, (), 'table-key-properties', list(stream.pk_fields))
    for prop in raw_schema['properties'].keys():
        if prop in stream.replication_key or prop in stream.pk_fields:
            metadata.write(mdata, ('properties', prop), 'inclusion', 'automatic')
        else:
            metadata.write(mdata, ('properties', prop), 'inclusion', 'available')

    return mdata





