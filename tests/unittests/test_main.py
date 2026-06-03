import unittest
from unittest import mock

import tap_zendesk_chat
from tap_zendesk_chat.http import InvalidConfigurationError


class Args:
    discover = False
    config = {"access_token": "token", "start_date": "2022-01-01T00:00:00Z"}
    state = {}
    catalog = {}


class TestMainErrorHandling(unittest.TestCase):
    @mock.patch("tap_zendesk_chat.parse_args", return_value=Args())
    @mock.patch("tap_zendesk_chat.sync", side_effect=RuntimeError("sync failed"))
    @mock.patch("tap_zendesk_chat.Context", return_value=object())
    def test_main_sync_failure_exits_nonzero(self, _mock_context, _mock_sync, _mock_parse_args):
        with self.assertRaises(SystemExit) as err:
            tap_zendesk_chat.main()
        self.assertEqual(1, err.exception.code)

    @mock.patch("tap_zendesk_chat.parse_args")
    @mock.patch("tap_zendesk_chat.discover", side_effect=InvalidConfigurationError("bad credentials"))
    def test_main_discovery_failure_exits_nonzero(self, _mock_discover, mock_parse_args):
        args = Args()
        args.discover = True
        mock_parse_args.return_value = args

        with self.assertRaises(SystemExit) as err:
            tap_zendesk_chat.main()
        self.assertEqual(1, err.exception.code)
