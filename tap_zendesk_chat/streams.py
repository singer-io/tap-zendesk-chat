from singer import metrics
import pendulum
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

    def format_and_write(self, response):
        """Formats a list of records in place and outputs the data to
        stdout."""
        page = self.format_response(response)
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)


class Everything(Stream):
    def sync(self, ctx):
        self.format_and_write(ctx.client.request(self.tap_stream_id))


class Agents(Stream):
    def sync(self, ctx):
        since_id_bookmark = [self.tap_stream_id, "since_id"]
        since_id = ctx.bookmark(since_id_bookmark) or 0
        while True:
            params = {
                "since_id": since_id,
                "limit": 500,
            }
            page = ctx.client.request(self.tap_stream_id, params)
            if not page:
                ctx.set_bookmark(since_id_bookmark, None)
                ctx.write_state()
                break
            self.format_and_write(page)
            since_id = page[-1]["id"] + 1
            ctx.set_bookmark(since_id_bookmark, since_id)
            ctx.write_state()


class Chats(Stream):
    def _bulk_chats(self, ctx, chat_ids):
        if not chat_ids:
            return []
        params = {"ids": ",".join(chat_ids)}
        body = ctx.client.request(self.tap_stream_id, params=params)
        return body["docs"].values()

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
        ts_bookmark_key = [self.tap_stream_id, chat_type + "_ts"]
        url_bookmark_key = [self.tap_stream_id, chat_type + "_next_url"]
        if full_sync:
            ctx.set_bookmark(ts_bookmark_key, None)
            ctx.set_bookmark(url_bookmark_key, None)
        ts = ctx.update_start_date_bookmark(ts_bookmark_key)
        next_url = ctx.bookmark(url_bookmark_key)
        max_ts = ts
        while True:
            if next_url:
                search = ctx.client.request(self.tap_stream_id, url=next_url)
            else:
                search = self._search(ctx, chat_type, ts_field, ts)
            next_url = search["next_url"]
            ctx.set_bookmark(url_bookmark_key, next_url)
            ctx.write_state()
            chat_ids = [r["id"] for r in search["results"]]
            chats = self._bulk_chats(ctx, chat_ids)
            max_ts = max(max_ts, *[c[ts_field] for c in chats])
            self.format_and_write(chats)
            if not next_url:
                break
        ctx.set_bookmark(ts_bookmark_key, max_ts)
        ctx.write_state()

    def sync(self, ctx):
        full_sync_days = timedelta(days=ctx.config.get("chats_full_sync_days", 7))
        last_sync_bookmark = [self.tap_stream_id, "chats_last_full_sync"]
        last_full_sync = ctx.bookmark(last_sync_bookmark)
        full_sync = not last_full_sync or \
            pendulum.parse(last_full_sync) + full_sync_days <= datetime.utcnow()
        self._pull(ctx, "chat", "end_timestamp", full_sync=full_sync),
        self._pull(ctx, "offline_msg", "timestamp", full_sync=full_sync)
        if full_sync:
            ctx.set_bookmark(last_sync_bookmark, datetime.utcnow().isoformat())
            ctx.write_state()


class Triggers(Everything):
    def format_response(self, response):
        for trigger in response:
            definition = trigger["definition"]
            for k in ["condition", "actions"]:
                definition[k] = json.dumps(definition[k])
        return response


class Bans(Everything):
    def format_response(self, response):
        return response["visitor"] + response["ip_address"]


class Account(Everything):
    def sync(self, ctx):
        # The account endpoint is restricted to zopim accounts, meaning
        # integrated Zendesk accounts will get a 403 for this endpoint. As a
        # result, we will have to just ignore a 403 and not output any data.
        try:
            super().sync(ctx)
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
