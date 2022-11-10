import singer
from requests.exceptions import HTTPError
from singer import metadata
from singer.catalog import Catalog

from .http import Client
from .streams import STREAMS
from .utils import load_schema

LOGGER = singer.get_logger()


def account_not_authorized(client):
    # The account endpoint is restricted to zopim accounts, meaning integrated
    # Zendesk accounts will get a 403 for this endpoint.
    try:
        client.request(STREAMS["account"].tap_stream_id)
    except HTTPError as err:
        if err.response.status_code == 403:
            LOGGER.info(
                "Ignoring 403 from account endpoint - this must be an \
                integrated Zendesk account. This endpoint will be excluded \
                from discovery"
            )
            return True
        raise
    return False


def build_metadata(raw_schema: dict, stream):
    mdata = metadata.new()
    metadata.write(mdata, (), "valid-replication-keys", list(stream.replication_key))
    metadata.write(mdata, (), "table-key-properties", list(stream.pk_fields))
    metadata.write(mdata, (), "forced-replication-method", stream.forced_replication_method)
    for prop in raw_schema["properties"].keys():
        if (prop in stream.replication_key) or (prop in stream.pk_fields):
            metadata.write(mdata, ("properties", prop), "inclusion", "automatic")
        else:
            metadata.write(mdata, ("properties", prop), "inclusion", "available")
    return metadata.to_list(mdata)


def discover(config: dict) -> Catalog:
    """discover function for tap-zendesk-chat."""
    if config:
        client = Client(config)
        client.request(STREAMS["chats"].tap_stream_id)
        if account_not_authorized(client):
            STREAMS.pop("account")
    streams = []
    for stream_name, stream in STREAMS.items():
        schema = load_schema(stream.tap_stream_id)
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream.tap_stream_id,
                "schema": schema,
                "metadata": build_metadata(schema, stream),
            }
        )
    return Catalog.from_dict({"streams": streams})
