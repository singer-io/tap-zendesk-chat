from datetime import datetime
from typing import Dict, List

from singer import Catalog, write_state
from singer.utils import now

from .http import Client


class Context:
    """Wrapper Class Around state bookmarking."""

    def __init__(self, config: Dict, state: Dict, catalog: Catalog):
        self.config = config
        self.state = state
        self.catalog = catalog
        self.client = Client(config)
        self.now = now()

    @property
    def bookmarks(self):
        """Provides read-only access to bookmarks, creates one if does not
        exist."""
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        return self.state["bookmarks"]

    def bookmark(self, path: List):
        """checks the state[file] for a nested path of bookmarks and returns
        value."""
        bookmark = self.bookmarks
        for key in path:
            if key not in bookmark:
                bookmark[key] = {}
            bookmark = bookmark[key]
        return bookmark

    def set_bookmark(self, path, val):
        if isinstance(val, datetime):
            val = val.isoformat()
        self.bookmark(path[:-1])[path[-1]] = val

    def update_start_date_bookmark(self, path):
        val = self.bookmark(path)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark(path, val)
        return val

    def write_state(self):
        write_state(self.state)
