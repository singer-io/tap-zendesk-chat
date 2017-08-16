from singer import metrics
import pendulum
import time
import datetime
from requests.exceptions import HTTPError
from singer.utils import strftime
import attr
import json
from itertools import chain
from .http import ZendeskChatClient


class Puller(object):
    def prepare(self, config, state, tap_stream_id):
        self.config = config
        self.state = state
        self.tap_stream_id = tap_stream_id
        self.client = ZendeskChatClient(config)
        return self

    @property
    def _bookmark(self):
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        if self.tap_stream_id not in self.state["bookmarks"]:
            self.state["bookmarks"][self.tap_stream_id] = {}
        return self.state["bookmarks"][self.tap_stream_id]

    def _set_last_updated(self, key, updated_at):
        if isinstance(updated_at, datetime.datetime):
            updated_at = updated_at.isoformat()
        self._bookmark[key] = updated_at

    def _update_start_state(self, key):
        if not self._bookmark.get(key):
            self._set_last_updated(key, self.config["start_date"])
        return self._bookmark[key]


class Everything(Puller):
    def yield_pages(self):
        page = self.client.request(self.tap_stream_id)
        if page:
            yield page if type(page) == list else [page]


class Agents(Puller):
    def yield_pages(self):
        since_id = self._bookmark.get("since_id") or 0
        while True:
            params = {
                "since_id": since_id,
                "limit": 500,
            }
            page = self.client.request(self.tap_stream_id, params)
            if not page:
                self._bookmark.pop("since_id")
                break
            since_id = page[-1]["id"] + 1
            self._bookmark["since_id"] = since_id
            yield page


class Chats(Puller):
    def _bulk_chats(self, chat_ids):
        if not chat_ids:
            return []
        params = {"ids": ",".join(chat_ids)}
        body = self.client.request(self.tap_stream_id, params=params)
        return body["docs"].values()

    def _search(self, chat_type, ts_field, dt):
        params = {
            "q": "type:{} AND {}:[{} TO *]".format(chat_type, ts_field, dt)
        }
        return self.client.request(
            self.tap_stream_id, params=params, url_extra="/search")

    def pull(self, chat_type, ts_field):
        ts_bookmark_key = chat_type + "_ts"
        url_bookmark_key = chat_type + "_next_url"
        ts = self._update_start_state(ts_bookmark_key)
        max_ts = ts
        next_url = self._bookmark.get(url_bookmark_key)
        while True:
            if next_url:
                search = self.client.request(self.tap_stream_id, url=next_url)
            else:
                search = self._search(chat_type, ts_field, ts)
            next_url = search["next_url"]
            self._bookmark[url_bookmark_key] = next_url
            chat_ids = [r["id"] for r in search["results"]]
            chats = self._bulk_chats(chat_ids)
            max_ts = max(max_ts, *[c[ts_field] for c in chats])
            yield chats
            if not next_url:
                break
        self._set_last_updated(ts_bookmark_key, max_ts)

    def yield_pages(self):
        for page in chain(self.pull("chat", "end_timestamp"),
                          self.pull("offline_msg", "timestamp")):
            yield page


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    pk_fields = attr.ib()
    puller = attr.ib()
    formatter = attr.ib(default=None)

    def format_page(self, page):
        if self.formatter:
            return self.formatter(page)
        return page


def format_triggers(page):
    for trigger in page:
        definition = trigger["definition"]
        for k in ["condition", "actions"]:
            definition[k] = json.dumps(definition[k])
    return page


def format_bans(page):
    return page[0]["visitor"] + page[0]["ip_address"]

STREAMS = [
    Stream("account", ["account_key"], Everything()),
    Stream("agents", ["id"], Agents()),
    Stream("chats", ["id"], Chats()),
    Stream("shortcuts", ["name"], Everything()),
    Stream("triggers", ["id"], Everything(), format_triggers),
    Stream("bans", ["id"], Everything(), format_bans),
    Stream("departments", ["id"], Everything()),
    Stream("goals", ["id"], Everything()),
]
