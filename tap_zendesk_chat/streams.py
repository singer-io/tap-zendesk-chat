from datetime import datetime, timedelta
import inspect
import json
import singer
from singer import metrics, Transformer, metadata, utils

LOGGER = singer.get_logger()


def break_into_intervals(days, start_time: str, now: datetime):
    delta = timedelta(days=days)
    start_dt = utils.strptime_to_utc(start_time)
    while start_dt < now:
        end_dt = min(start_dt + delta, now)
        yield start_dt, end_dt
        start_dt = end_dt


class Stream:
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields"""
    tap_stream_id = None
    pk_fields = None

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def format_response(self, response):
        return [response] if not isinstance(response, list) else response

    def write_page(self, ctx, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        stream = ctx.catalog.get_stream(self.tap_stream_id)
        with Transformer() as transformer:
            for rec in page:
                singer.write_record(
                    self.tap_stream_id,
                    transformer.transform(
                        rec, stream.schema.to_dict(), metadata.to_map(stream.metadata),
                    )
                )
        self.metrics(page)


class Everything(Stream):
    def sync(self, ctx):
        self.write_page(ctx, ctx.client.request(self.tap_stream_id))


class Agents(Stream):
    tap_stream_id = 'agents'
    pk_fields = ["id"]

    def sync(self, ctx):
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
            self.write_page(ctx, page)
            since_id = page[-1]["id"] + 1
            ctx.set_bookmark(since_id_offset, since_id)
            ctx.write_state()
        ctx.set_bookmark(since_id_offset, None)
        ctx.write_state()


class Chats(Stream):
    tap_stream_id = 'chats'
    pk_fields = ["id"]

    def _bulk_chats(self, ctx, chat_ids):
        if not chat_ids:
            return []
        params = {"ids": ",".join(chat_ids)}
        body = ctx.client.request(self.tap_stream_id, params=params)
        return list(body["docs"].values())

    def _search(self, ctx, chat_type, ts_field,
                start_dt: datetime, end_dt: datetime):
        params = {
            "q": "type:{} AND {}:[{} TO {}]"
            .format(chat_type, ts_field, start_dt.isoformat(), end_dt.isoformat())
        }
        return ctx.client.request(
            self.tap_stream_id, params=params, url_extra="/search")

    def _pull(self, ctx, chat_type, ts_field, *, full_sync):
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
        LOGGER.info("Using chat_search_interval_days: {}".format(interval_days))

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
                    self.write_page(ctx, chats)
                    max_bookmark = max(max_bookmark, *[c[ts_field] for c in chats])
                if not next_url:
                    break
            ctx.set_bookmark(ts_bookmark_key, max_bookmark)
            ctx.write_state()

    def _should_run_full_sync(self, ctx):
        sync_days = ctx.config.get("chats_full_sync_days")
        if sync_days:
            last_sync = ctx.state.get("chats_last_full_sync")
            if not last_sync:
                LOGGER.info("Running full sync of chats: no last sync time")
                return True
            next_sync = utils.strptime_to_utc(last_sync) + timedelta(days=int(sync_days))
            if next_sync <= ctx.now:
                LOGGER.info("Running full sync of chats: "
                            "last sync was {}, configured to run every {} days"
                            .format(last_sync, sync_days))
                return True
        return False

    def sync(self, ctx):
        full_sync = self._should_run_full_sync(ctx)
        self._pull(ctx, "chat", "end_timestamp", full_sync=full_sync)
        self._pull(ctx, "offline_msg", "timestamp", full_sync=full_sync)
        if full_sync:
            ctx.state["chats_last_full_sync"] = ctx.now.isoformat()
            ctx.write_state()


class Shortcuts(Everything):
    tap_stream_id = 'shortcuts'
    pk_fields = ["name"]


class Triggers(Stream):
    tap_stream_id = 'triggers'
    pk_fields = ["id"]

    def sync(self, ctx):
        page = ctx.client.request(self.tap_stream_id)
        for trigger in page:
            definition = trigger["definition"]
            for k in ["condition", "actions"]:
                definition[k] = json.dumps(definition[k])
        self.write_page(ctx, page)


class Bans(Stream):
    tap_stream_id = 'bans'
    pk_fields = ['id']

    def sync(self, ctx):
        response = ctx.client.request(self.tap_stream_id)
        page = response["visitor"] + response["ip_address"]
        self.write_page(ctx, page)


class Departments(Everything):
    tap_stream_id = 'departments'
    pk_fields = ["id"]


class Goals(Everything):
    tap_stream_id = 'goals'
    pk_fields = ["id"]


class Account(Stream):
    tap_stream_id = 'account'
    pk_fields = ['account_key']

    def sync(self, ctx):
        # The account endpoint returns a single item, so we have to wrap it in
        # a list to write a "page"
        self.write_page(ctx, [ctx.client.request(self.tap_stream_id)])


STREAM_OBJECTS = {
    cls.tap_stream_id: cls
    for cls in globals().values()
    if inspect.isclass(cls) and issubclass(cls, Stream) and cls.tap_stream_id
}
