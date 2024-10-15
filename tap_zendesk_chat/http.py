import backoff
import requests
from singer import get_logger, metrics

LOGGER = get_logger()
BASE_URL = "https://www.zopim.com"


class RateLimitException(Exception):
    pass


class Client:
    def __init__(self, config):
        self.access_token = config["access_token"]
        self.user_agent = config.get("user_agent", "tap-zendesk-chat")
        self.headers = {}
        self.headers["Authorization"] = f"Bearer {self.access_token}"
        self.headers["User-Agent"] = self.user_agent
        if "subdomain" in config:
            self.base_url =  BASE_URL
            # self.base_url = f"https://{config['subdomain']}.zendesk.com"
        else:
            self.base_url =  BASE_URL
            LOGGER.warning("Missing Subdomain, please recheck the configuration")
        self.session = requests.Session()

    @backoff.on_exception(backoff.expo, RateLimitException, max_tries=10, factor=2)
    def request(self, tap_stream_id, params=None, url=None, url_extra=""):
        with metrics.http_request_timer(tap_stream_id) as timer:

            url = url or f"{self.base_url}/api/v2/{tap_stream_id}{url_extra}"
            LOGGER.info("calling %s %s", url, params)
            response = self.session.get(url, headers=self.headers, params=params)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code in [429, 502]:
            raise RateLimitException()
        elif response.status_code == 400:
            LOGGER.warning(
                "The amount of data present for in %s stream is huge,\
                The api has a pagination limit of 251 pages, please reduce the search window for this stream"
            )
        response.raise_for_status()
        return response.json()
