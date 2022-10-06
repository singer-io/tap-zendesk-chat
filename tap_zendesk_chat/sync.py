import singer
from . import streams
LOGGER = singer.get_logger()


def sync(ctx, catalog: singer.Catalog, state):
    """performs sync for selected streams."""
    with singer.Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()
            stream_metadata = singer.metadata.to_map(stream.metadata)
            stream_obj = streams.STREAMS[tap_stream_id]
            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            ctx.state = singer.set_currently_syncing(ctx.state, tap_stream_id)
            singer.write_state(state)
            singer.write_schema(tap_stream_id, stream_schema, stream_obj.pk_fields, stream.replication_key)
            stream_obj.sync(ctx)
            state = stream_obj.sync(
                state=state, schema=stream_schema, stream_metadata=stream_metadata, transformer=transformer
            )
            singer.write_state(ctx.state)

    ctx.state = singer.set_currently_syncing(ctx.state, None)
    singer.write_state(ctx.state)