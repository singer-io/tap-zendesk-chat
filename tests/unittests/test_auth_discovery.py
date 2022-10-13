from requests.exceptions import HTTPError
import tap_zendesk_chat
import unittest
from unittest import mock
from tap_zendesk_chat.http import Client


# Mock args
class Args:
    def __init__(self):
        self.discover = True
        self.properties = False
        self.config = {"access_token": "abc-def"}
        self.state = False
        self.properties = {}


class MockResponse:
    def __init__(self, resp, status_code, headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.headers = headers
        self.raise_error = raise_error

    def raise_for_status(self):
        return self.status_code

    def json(self):
        return self.json_data


def mock_200_account_endpoint_exception(*args, **kwargs):
    return MockResponse({}, 200)


class TestDiscoverMode(unittest.TestCase):

    def test_basic_auth_no_access_401(self):
        '''
            Verify exception is raised for no access(401) error code for basic auth
            do the assertions inside exception block
        '''
        args = Args()

        with self.assertRaises(HTTPError) as e:
            tap_zendesk_chat.discover(args.config)
        # Verifying the message formed for the custom exception
        expected_error_message = "401 Client Error: Unauthorized for url:"
        self.assertIn(expected_error_message, str(e.exception))

    @mock.patch('tap_zendesk_chat.utils', return_value=Args())
    @mock.patch('singer.catalog.Catalog.from_dict', return_value={"key": "value"})
    def test_discovery_no_config(self, mock_utils, mock_catalog):
        """
        tests discovery method when config is None.
        """
        expected = {"key": "value"}
        self.assertEqual(tap_zendesk_chat.discover(None), expected)

    @mock.patch('tap_zendesk_chat.utils', return_value=Args())
    @mock.patch('singer.catalog.Catalog.from_dict', return_value={"key": "value"})
    @mock.patch('tap_zendesk_chat.http.Client')
    @mock.patch('tap_zendesk_chat.http.Client.request')
    def test_discovery(self, mock_utils, mock_catalog, mock_client, mock_request):
        """
        tests discovery method.
        """
        expected = {"key": "value"}
        self.assertEqual(tap_zendesk_chat.discover(Args().config), expected)


class TestAccountEndpointAuthorized(unittest.TestCase):

    def test_is_account_not_authorized_404(self):
        """
        tests if account_not_authorized method in discover raises http 404
        """
        client = Client(Args().config)
        with self.assertRaises(HTTPError) as e:
            client.request("xxxxxxx")

        expected_error_message = "404 Client Error: Not Found for url:"
        self.assertIn(expected_error_message, str(e.exception))


