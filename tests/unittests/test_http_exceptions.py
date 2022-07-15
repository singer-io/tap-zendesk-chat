from tap_zendesk_chat.http import RateLimitException, Client
from unittest import mock
import unittest

client = Client({"access_token": ""})


class MockResponse:
    def __init__(self, resp, status_code, headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.headers = headers
        self.raise_error = raise_error


def mock_429_rate_limit_exception_response(*args, **kwargs):
    """
    Mock the response with status code as 429
    """
    return MockResponse({}, 429, headers={}, raise_error=True)


def mock_502_bad_gateway_exception_response(*args, **kwargs):
    return MockResponse({}, 502, headers={}, raise_error=True)


class TestRateLimitExceptionRetry(unittest.TestCase):
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.send", side_effect=mock_429_rate_limit_exception_response)
    def test_rate_limit_429_error(self, mocked_send, mocked_sleep):

        try:
            """
            verify the custom RateLimitException
            Make sure API call gets retired for 10 times before raising RateLimitException
            """
            resp = client.request("departments")
        except RateLimitException:
            pass

            """
            Verifying the retry is happening 10 times for the RateLimitException exception
            """
        self.assertEquals(mocked_send.call_count, 10)


class TestBadGatewayExceptionRetry(unittest.TestCase):
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.send", side_effect=mock_502_bad_gateway_exception_response)
    def test_rate_limit_429_error(self, mocked_send, mocked_sleep):

        try:
            """
            verify the custom RateLimitException for 502 Bad Gateway exception
            Make sure API call gets retired for 10 times before raising RateLimitException
            """
            resp = client.request("departments")
        except RateLimitException:
            pass

            """
            Verifying the retry is happening 10 times for the RateLimitException exception
            """
        self.assertEquals(mocked_send.call_count, 10)


