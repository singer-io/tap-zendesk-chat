import unittest

from tap_zendesk_chat.context import Context


class TestContextFunctions(unittest.TestCase):
    config = {"start_date": "2022-01-01", "access_token": ""}
    state = {}
    catalog = {}

    context_client = Context(config, state, catalog)

    def test_bookmarks(self):
        """tests bookmarks property for context module returns {} with
        bookmarks key in state file."""
        self.assertEqual({}, self.context_client.bookmarks)

        self.context_client.state = {"bookmarks": {"account": {"start_date": self.config.get("start_date")}}}

        self.assertEqual(1, len(self.context_client.bookmarks))

    def test_get_bookmark(self):
        """tests bookmark fn in context.py."""
        self.context_client.state = {
            "bookmarks": {
                "account": {"last_created": "2022-06-01"},
                "chats": {"chat.end_timestamp": "2022-06-01T15:00:00", "offline_msg.timestamp": "2022-06-01T18:00:00"},
            }
        }

        output = self.context_client.bookmark([])

        self.assertEqual("2022-06-01T18:00:00", output["chats"]["offline_msg.timestamp"])
        self.assertEqual({}, output["chats"].get("offline_msg.end_timestamp", {}))
        self.assertEqual("2022-06-01T15:00:00", output["chats"]["chat.end_timestamp"])

    def test_set_bookmark(self):
        """tests set_bookmark fn in context.py set the bookmark using
        set_bookmark fn and assert the bookmark for stream in state json."""
        self.context_client.state = {
            "bookmarks": {
                "account": {"last_created": "2022-06-01"},
                "chats": {"chat.end_timestamp": "2022-06-01T15:00:00", "offline_msg.timestamp": "2022-06-01T18:00:00"},
            }
        }

        self.context_client.set_bookmark(["chats", "chat.end_timestamp"], "2022-07-01T01:00:00")
        self.assertEqual("2022-06-01T15:00:00", self.context_client.state["bookmarks"]["chats"]["chat.end_timestamp"])

        self.context_client.set_bookmark(["account"], {"last_created": "2022-07-05"})
        self.assertEqual({"last_created": "2022-07-05"}, self.context_client.state["bookmarks"]["account"])
