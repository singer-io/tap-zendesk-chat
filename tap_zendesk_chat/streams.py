from datetime import datetime, timedelta
from typing import Dict

import singer
from singer import Transformer, metrics
from singer.utils import strptime_to_utc

from .utils import break_into_intervals

LOGGER = singer.get_logger()


class Stream:
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields
    """

    replication_key = set()
    forced_replication_method = "FULL_TABLE"

    def __init__(self, tap_stream_id, pk_fields):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def format_response(self, response):
        return [response] if isinstance(response, list) else response

    def write_page(self, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)


class Everything(Stream):
    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        response = ctx.client.request(self.tap_stream_id)
        page = [transformer.transform(rec, schema, metadata=stream_metadata) for rec in response]
        self.write_page(page)


class Agents(Stream):
    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        since_id_offset = [self.tap_stream_id, "offset", "id"]
        since_id = ctx.bookmark(since_id_offset) or 0
        while True:
            params = {
                "since_id": since_id,
                "limit": ctx.config.get("agents_page_limit", 100),
            }
            page = ctx.client.request(self.tap_stream_id, params)
            if not page:
                break
            self.write_page([transformer.transform(rec, schema, metadata=stream_metadata) for rec in page])
            since_id = page[-1]["id"] + 1
            ctx.set_bookmark(since_id_offset, since_id)
            ctx.write_state()
        ctx.set_bookmark(since_id_offset, None)
        ctx.write_state()


class Chats(Stream):
    replication_key = {"timestamp", "end_timestamp"}
    forced_replication_method = "INCREMENTAL"

    def _bulk_chats(self, ctx, chat_ids):
        if not chat_ids:
            return []
        params = {"ids": ",".join(chat_ids)}
        body = ctx.client.request(self.tap_stream_id, params=params)
        return list(body["docs"].values())

    def _search(self, ctx, chat_type, ts_field, start_dt: datetime, end_dt: datetime):
        params = {"q": f"type:{chat_type} AND {ts_field}:[{start_dt.isoformat()} TO {end_dt.isoformat()}]"}
        return ctx.client.request(self.tap_stream_id, params=params, url_extra="/search")

    def _pull(
        self, ctx, chat_type, ts_field, *, full_sync, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ):
        """Pulls and writes pages of data for the given chat_type, where
        chat_type can be either "chat" or "offline_msg".

        ts_field determines the property of the chat objects that is used as
        the bookmark for the chat.

        full_sync is a boolean indicating whether or not to pull all chats
        based on the "start_date" in the config. When this is true, all
        bookmarks for this chat type will be ignored.
        """
        ts_bookmark_key = [self.tap_stream_id, chat_type + "." + ts_field]
        url_offset_key = [self.tap_stream_id, "offset", chat_type + ".next_url"]
        if full_sync:
            ctx.set_bookmark(ts_bookmark_key, None)
            ctx.set_bookmark(url_offset_key, None)
        start_time = ctx.update_start_date_bookmark(ts_bookmark_key)
        next_url = ctx.bookmark(url_offset_key)
        max_bookmark = start_time

        interval_days = 14
        interval_days_str = ctx.config.get("chat_search_interval_days")
        if interval_days_str is not None:
            interval_days = int(interval_days_str)
        LOGGER.info("Using chat_search_interval_days: %s", interval_days)

        intervals = break_into_intervals(interval_days, start_time, ctx.now)

        for start_dt, end_dt in intervals:
            while True:
                if next_url:
                    search_resp = ctx.client.request(self.tap_stream_id, url=next_url)
                else:
                    search_resp = self._search(ctx, chat_type, ts_field, start_dt, end_dt)
                next_url = search_resp["next_url"]
                ctx.set_bookmark(url_offset_key, next_url)
                ctx.write_state()
                chat_ids = [r["id"] for r in search_resp["results"]]
                chats = self._bulk_chats(ctx, chat_ids)
                if chats:
                    chats = [transformer.transform(rec, schema, metadata=stream_metadata) for rec in chats]
                    self.write_page(chats)
                    max_bookmark = max(max_bookmark, *[c[ts_field] for c in chats])
                if not next_url:
                    break
            ctx.set_bookmark(ts_bookmark_key, max_bookmark)
            ctx.write_state()

    def _should_run_full_sync(self, ctx) -> bool:
        sync_days = ctx.config.get("chats_full_sync_days")
        if sync_days:
            last_sync = ctx.state.get("chats_last_full_sync")
            if not last_sync:
                LOGGER.info("Running full sync of chats: no last sync time")
                return True
            next_sync = strptime_to_utc(last_sync) + timedelta(days=int(sync_days))
            if next_sync <= ctx.now:
                LOGGER.info(
                    "Running full sync of chats: last sync was %s, configured to run every %s days",
                    last_sync,
                    sync_days,
                )
                return True
        return False

    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        full_sync = self._should_run_full_sync(ctx)
        self._pull(
            ctx,
            "chat",
            "end_timestamp",
            full_sync=full_sync,
            schema=schema,
            stream_metadata=stream_metadata,
            transformer=transformer,
        )
        self._pull(
            ctx,
            "offline_msg",
            "timestamp",
            full_sync=full_sync,
            schema=schema,
            stream_metadata=stream_metadata,
            transformer=transformer,
        )
        if full_sync:
            ctx.state["chats_last_full_sync"] = ctx.now.isoformat()
            ctx.write_state()


class Bans(Stream):
    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        since_id_offset = [self.tap_stream_id, "offset", "id"]
        since_id = ctx.bookmark(since_id_offset) or 0
        while True:

            params = {
                "since_id": since_id,
                "limit": ctx.config.get("bans_page_limit", 100),
                # TODO: Add Additional advanced property in connection_properties
            }
            response = ctx.client.request(self.tap_stream_id, params)
            page = response.get("visitor", []) + response.get("ip_address", [])
            if not page:
                break
            page = response["visitor"] + response["ip_address"]
            self.write_page([transformer.transform(rec, schema, metadata=stream_metadata) for rec in page])
            since_id = page[-1]["id"] + 1
            ctx.set_bookmark(since_id_offset, since_id)
            ctx.write_state()
        ctx.set_bookmark(since_id_offset, None)
        ctx.write_state()


class Account(Stream):
    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        # The account endpoint returns a single item, so we have to wrap it in
        # a list to write a "page"
        response = ctx.client.request(self.tap_stream_id)
        page = transformer.transform(response, schema, metadata=stream_metadata)
        self.write_page([page])


all_streams = [
    Account("account", ["account_key"]),
    Agents("agents", ["id"]),
    Bans("bans", ["id"]),
    Chats("chats", ["id"]),
    Everything("departments", ["id"]),
    Everything("goals", ["id"]),
    Everything("shortcuts", ["name"]),
    Everything("triggers", ["id"]),
]
STREAMS = {s.tap_stream_id: s for s in all_streams}
