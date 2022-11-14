import singer
from requests.exceptions import HTTPError
from singer.metadata import write,to_list,to_map,get_standard_metadata
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


def get_metadata(schema: dict, stream):
    """
    tweaked inbuilt singer method to also mark the replication keys as automatic fields
    """
    stream_metadata = get_standard_metadata(
        **{
            "schema": schema,
            "key_properties": list(stream.key_properties),
            "valid_replication_keys": list(stream.valid_replication_keys),
            "replication_method": stream.forced_replication_method,
        }
    )
    stream_metadata = to_map(stream_metadata)
    if stream.valid_replication_keys is not None:
        for key in stream.valid_replication_keys:
            stream_metadata = write(stream_metadata, ("properties", key), "inclusion", "automatic")
    stream_metadata = to_list(stream_metadata)
    return stream_metadata

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
                "metadata": get_metadata(schema, stream),
            }
        )
    return Catalog.from_dict({"streams": streams})
