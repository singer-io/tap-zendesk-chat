import singer

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def sync(ctx):
    selected_streams = ctx.catalog.get_selected_streams(ctx.state)

    for stream in selected_streams:
        stream_name = stream.tap_stream_id
        stream_object = STREAM_OBJECTS.get(stream_name)()

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_name))

        singer.write_schema(
            stream_name,
            stream.schema.to_dict(),
            stream_object.pk_fields,
        )

        LOGGER.info("Syncing stream: " + stream_name)
        ctx.state["currently_syncing"] = stream_name

        ctx.write_state()
        stream_object.sync(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()
