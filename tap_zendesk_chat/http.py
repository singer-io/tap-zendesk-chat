import requests
from singer import metrics, get_logger
import backoff
LOGGER = get_logger()
BASE_URL = "https://www.zopim.com"


class RateLimitException(Exception):
    pass


class Client:
    def __init__(self, config):
        # self.session = requests.Session()
        self.access_token = config["access_token"]
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()

    @backoff.on_exception(backoff.expo,RateLimitException,max_tries=10,factor=2)
    def request(self, tap_stream_id, params=None, url=None, url_extra=""):
        if not params:
            params={}
        with metrics.http_request_timer(tap_stream_id) as timer:
            url = url or BASE_URL + "/api/v2/" + tap_stream_id + url_extra
            headers = {"Authorization": "Bearer " + self.access_token}
            if self.user_agent:
                headers["User-Agent"] = self.user_agent
            LOGGER.info("calling %s %s",url,params)
            request = requests.Request("GET", url, headers=headers, params=params)
            response = self.session.send(request.prepare())
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 502]:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()
