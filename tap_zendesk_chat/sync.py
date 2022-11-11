from singer import (
    Transformer,
    get_logger,
    metadata,
    set_currently_syncing,
    write_schema,
    write_state,
)

from .streams import STREAMS

LOGGER = get_logger()


def sync(ctx):
    """performs sync for selected streams."""
    with Transformer() as transformer:
        for stream in ctx.catalog.get_selected_streams(ctx.state):
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)
            stream_obj = STREAMS[tap_stream_id]()
            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            ctx.state = set_currently_syncing(ctx.state, tap_stream_id)
            ctx.write_state()
            write_schema(tap_stream_id, stream_schema, stream_obj.pk_fields, stream.replication_key)
            stream_obj.sync(ctx, schema=stream_schema, stream_metadata=stream_metadata, transformer=transformer)
            ctx.write_state()

    ctx.state = set_currently_syncing(ctx.state, None)
    write_state(ctx.state)
