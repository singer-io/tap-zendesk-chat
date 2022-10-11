from singer import Transformer,metadata,Catalog,write_state,write_schema,set_currently_syncing,get_logger
from . import streams
from .http import Client
LOGGER = get_logger()

def sync(ctx,catalog: Catalog):
    """performs sync for selected streams."""
    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(ctx.state):
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)
            stream_obj = streams.STREAMS[tap_stream_id]
            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            ctx.state = set_currently_syncing(ctx.state, tap_stream_id)
            ctx.write_state()
            write_schema(tap_stream_id, stream_schema, stream_obj.pk_fields, stream.replication_key)
            try:
                stream_obj.sync(ctx,schema=stream_schema, stream_metadata=stream_metadata, transformer=transformer)        
            except Exception as err:
                LOGGER.info("%s",err)
                stream_obj.sync(ctx)
            ctx.write_state()

    ctx.state = set_currently_syncing(ctx.state, None)
    write_state(ctx.state)