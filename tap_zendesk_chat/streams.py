from datetime import timedelta
from typing import Dict, List

import singer
from singer import Transformer, metrics
from singer.utils import strptime_to_utc

from .utils import break_into_intervals

LOGGER = singer.get_logger()


class BaseStream:
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields
    """

    valid_replication_keys = set()
    tap_stream_id = None

    def metrics(self, page):
        "updates the metrics counter for the current stream"
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def write_page(self, page: List):
        """Formats a list of records in place and outputs the data to
        stdout."""
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)

    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        response = ctx.client.request(self.tap_stream_id)
        page = [transformer.transform(rec, schema, metadata=stream_metadata) for rec in response]
        self.write_page(page)


class Account(BaseStream):

    tap_stream_id = "account"
    key_properties = ["account_key"]
    forced_replication_method = "FULL_TABLE"

    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        response = ctx.client.request(self.tap_stream_id)
        page = transformer.transform(response, schema, metadata=stream_metadata)
        self.write_page([page])


class Agents(BaseStream):

    tap_stream_id = "agents"
    key_properties = ["id"]
    forced_replication_method = "FULL_TABLE"

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


class Bans(BaseStream):

    tap_stream_id = "bans"
    key_properties = ["id"]
    forced_replication_method = "FULL_TABLE"

    def sync(self, ctx, schema: Dict, stream_metadata: Dict, transformer: Transformer):
        since_id_offset = [self.tap_stream_id, "offset", "id"]
        since_id = ctx.bookmark(since_id_offset) or 0

        while True:
            params = {
                "since_id": since_id,
                "limit": ctx.config.get("bans_page_limit", 100),
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


class Chats(BaseStream):

    tap_stream_id = "chats"
    key_properties = ["id"]
    forced_replication_method = "INCREMENTAL"
    valid_replication_keys = {"timestamp", "end_timestamp"}

    def _bulk_chats(self, ctx, chat_ids: List):
        if not chat_ids:
            return []
        params = {"ids": ",".join(chat_ids)}
        body = ctx.client.request(self.tap_stream_id, params=params)
        return list(body["docs"].values())

    # pylint: disable=too-many-positional-arguments
    def _pull(self, ctx, chat_type, ts_field, full_sync, schema: Dict, stream_metadata: Dict, transformer: Transformer):
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

        interval_days = int(ctx.config.get("chat_search_interval_days", "14"))
        LOGGER.info("Using chat_search_interval_days: %s", interval_days)

        for start_dt, end_dt in break_into_intervals(interval_days, start_time, ctx.now):
            while True:
                if next_url:
                    search_resp = ctx.client.request(self.tap_stream_id, url=next_url)
                else:
                    params = {"q": f"type:{chat_type} AND {ts_field}:[{start_dt.isoformat()} TO {end_dt.isoformat()}]"}
                    search_resp = ctx.client.request(self.tap_stream_id, params=params, url_extra="/search")

                next_url = search_resp["next_url"]
                ctx.set_bookmark(url_offset_key, next_url)
                ctx.write_state()
                chats = self._bulk_chats(ctx, [r["id"] for r in search_resp["results"]])
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


class Departments(BaseStream):
    tap_stream_id = "departments"
    key_properties = ["id"]
    forced_replication_method = "FULL_TABLE"


class Goals(BaseStream):
    tap_stream_id = "goals"
    key_properties = ["id"]
    forced_replication_method = "FULL_TABLE"


class Shortcuts(BaseStream):
    tap_stream_id = "shortcuts"
    key_properties = ["name"]
    forced_replication_method = "FULL_TABLE"


class Triggers(BaseStream):
    tap_stream_id = "triggers"
    key_properties = ["id"]
    forced_replication_method = "FULL_TABLE"


STREAMS = {
    Account.tap_stream_id: Account,
    Agents.tap_stream_id: Agents,
    Bans.tap_stream_id: Bans,
    Chats.tap_stream_id: Chats,
    Departments.tap_stream_id: Departments,
    Goals.tap_stream_id: Goals,
    Shortcuts.tap_stream_id: Shortcuts,
    Triggers.tap_stream_id: Triggers,
}
