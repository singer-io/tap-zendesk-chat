import backoff
import requests
from singer import get_logger, metrics

LOGGER = get_logger()
BASE_URL = "https://www.zopim.com"


class RateLimitException(Exception):
    pass

class ResourceDeletedError(Exception):
    pass

class InvalidConfigurationError(Exception):
    pass


class Client:
    def __init__(self, config):
        self.access_token = config["access_token"]
        self.user_agent = config.get("user_agent", "tap-zendesk-chat")
        self.headers = {}
        self.subdomain = config.get("subdomain")
        self.headers["Authorization"] = f"Bearer {self.access_token}"
        self.headers["User-Agent"] = self.user_agent
        self.base_url = self.get_base_url()
        self.session = requests.Session()

    @backoff.on_exception(backoff.expo, ResourceDeletedError, max_tries=10, factor=2)
    def get_base_url(self):
        """
        Determines the base URL to use for Zendesk API requests.

        Checks the availability of zendesk chat endpoints
        and returns the available one
        Returns:
            str: The base URL to use for subsequent API requests.

        Raises:
            InvalidConfigurationError: If neither endpoint is accessible.
        """
        urls = [
            (f"https://{self.subdomain}.zendesk.com" , "/api/v2/chat/agents"),
            (BASE_URL , "/api/v2/agents")
        ]
        if not self.subdomain:
            # return base url incase of missing subdomain
            return BASE_URL
        for domain, endpoint in urls:
            resp = requests.get(f"{domain}{endpoint}", headers=self.headers, timeout=25)
            LOGGER.info("API CHECK %s %s", resp.url, resp.status_code)
            if resp.status_code == 410:
                raise ResourceDeletedError()
            elif resp.status_code == 200:
                return domain
        raise InvalidConfigurationError("Please check the URL or reauthenticate")

    @backoff.on_exception(backoff.expo, (RateLimitException, ResourceDeletedError), max_tries=10, factor=2)
    def request(self, tap_stream_id, params=None, url=None, url_extra=""):
        with metrics.http_request_timer(tap_stream_id) as timer:
            if self.base_url == BASE_URL:
                url = url or f"{self.base_url}/api/v2/{tap_stream_id}{url_extra}"
            else:
                url = url or f"{self.base_url}/api/v2/chat/{tap_stream_id}{url_extra}"
            LOGGER.info("calling %s %s", url, params)
            response = self.session.get(url, headers=self.headers, params=params)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code in [429, 502]:
            raise RateLimitException()
        elif response.status_code == 410:
            raise ResourceDeletedError()
        elif response.status_code == 400:
            LOGGER.warning(
                "The amount of data present for in %s stream is huge,\
                The api has a pagination limit of 251 pages, please reduce the search window for this stream"
            )
        response.raise_for_status()
        return response.json()
