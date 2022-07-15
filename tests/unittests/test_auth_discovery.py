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


class TestBasicAuthInDiscoverMode(unittest.TestCase):

    def test_basic_auth_no_access_401(self):
        '''
            Verify exception is raised for no access(401) error code for basic auth
            do the assertions inside exception block
        '''
        args = Args()
        try:
            tap_zendesk_chat.discover(args.config)
        except HTTPError as e:
            # verify the 401 status code for wrong credentials
            self.assertEqual(e.response.status_code, 401)
            expected_error_message = "401 Client Error: Unauthorized for url:"
            # Verifying the message formed for the custom exception
            self.assertIn(expected_error_message, str(e))

    @mock.patch('tap_zendesk_chat.utils', return_value=Args())
    @mock.patch('tap_zendesk_chat.discover')
    def test_discovery_calls_on_200_access(self, mock_discover, mock_utils):
        """
        tests if discovery method is getting called after mocking required_config_keys
        """
        tap_zendesk_chat.main_impl()
        self.assertEqual(mock_discover.call_count, 1)


class TestAccountEndpointAuthorized(unittest.TestCase):

    @mock.patch("requests.Session.send")
    def test_is_account_endpoint_verified(self, mock_send):
        """
        verify if is_account_endpoint_authorized fn returns True boolean on 200 status code
        """
        args = Args()
        client = Client(args.config)
        mock_send.return_value = mock_200_account_endpoint_exception()
        resp = tap_zendesk_chat.is_account_endpoint_authorized(client)
        self.assertEqual(resp, True)


