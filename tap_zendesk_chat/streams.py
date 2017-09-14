from singer import metrics
from pendulum import parse as dt_parse
import time
from datetime import datetime, timedelta
from requests.exceptions import HTTPError
import attr
import json
import singer

LOGGER = singer.get_logger()


class Stream(object):
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields"""
    def __init__(self, tap_stream_id, pk_fields):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def format_response(self, response):
        return [response] if type(response) != list else response

    def write_page(self, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)


class Everything(Stream):
    def sync(self, ctx):
        self.write_page(ctx.client.request(self.tap_stream_id))


class Agents(Stream):
    def sync(self, ctx):
        since_id_offset = [self.tap_stream_id, "offset", "id"]
        since_id = ctx.bookmark(since_id_offset) or 0
        while True:
            params = {
                "since_id": since_id,
                "limit": ctx.config.get("agents_page_limit", 500),
            }
            page = ctx.client.request(self.tap_stream_id, params)
            if not page:
                break
            self.write_page(page)
            since_id = page[-1]["id"] + 1
            ctx.set_bookmark(since_id_offset, since_id)
            ctx.write_state()
        ctx.set_bookmark(since_id_offset, None)
        ctx.write_state()


class Chats(Stream):
    def _bulk_chats(self, ctx, chat_ids):
        if not chat_ids:
            return []
        params = {"ids": ",".join(chat_ids)}
        body = ctx.client.request(self.tap_stream_id, params=params)
        return list(body["docs"].values())

    def _search(self, ctx, chat_type, ts_field, dt):
        params = {
            "q": "type:{} AND {}:[{} TO *]".format(chat_type, ts_field, dt)
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
        while True:
            if next_url:
                search = ctx.client.request(self.tap_stream_id, url=next_url)
            else:
                search = self._search(ctx, chat_type, ts_field, start_time)
            next_url = search["next_url"]
            ctx.set_bookmark(url_offset_key, next_url)
            ctx.write_state()
            chat_ids = [r["id"] for r in search["results"]]
            chats = self._bulk_chats(ctx, chat_ids)
            if chats:
                self.write_page(chats)
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
            next_sync = dt_parse(last_sync) + timedelta(days=sync_days)
            if next_sync <= datetime.utcnow():
                LOGGER.info("Running full sync of chats: "
                            "last sync was {}, configured to run every {} days"
                            .format(last_sync, sync_days))
                return True
        return False

    def sync(self, ctx):
        full_sync = self._should_run_full_sync(ctx)
        self._pull(ctx, "chat", "end_timestamp", full_sync=full_sync),
        self._pull(ctx, "offline_msg", "timestamp", full_sync=full_sync)
        if full_sync:
            ctx.state["chats_last_full_sync"] = datetime.utcnow().isoformat()
            ctx.write_state()


class Triggers(Stream):
    def sync(self, ctx):
        page = ctx.client.request(self.tap_stream_id)
        for trigger in page:
            definition = trigger["definition"]
            for k in ["condition", "actions"]:
                definition[k] = json.dumps(definition[k])
        self.write_page(page)


class Bans(Stream):
    def sync(self, ctx):
        response = ctx.client.request(self.tap_stream_id)
        page = response["visitor"] + response["ip_address"]
        self.write_page(page)


class Account(Stream):
    def sync(self, ctx):
        # The account endpoint is restricted to zopim accounts, meaning
        # integrated Zendesk accounts will get a 403 for this endpoint. As a
        # result, we will have to just ignore a 403 and not output any data.
        try:
            self.write_page([ctx.client.request(self.tap_stream_id)])
        except HTTPError as e:
            if e.response.status_code == 403:
                LOGGER.info("Ignoring 403 from accounts endpoint - I assume "
                            "this must be an integrated Zendesk account")
            else:
                raise

all_streams = [
    Agents("agents", ["id"]),
    Chats("chats", ["id"]),
    Everything("shortcuts", ["name"]),
    Triggers("triggers", ["id"]),
    Bans("bans", ["id"]),
    Everything("departments", ["id"]),
    Everything("goals", ["id"]),
    # Account stream is last due to the 403 issue described in Account above -
    # if we ever reach the Account stream it means the token is valid but we're
    # just not able to access the endpoint.
    Account("account", ["account_key"]),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
