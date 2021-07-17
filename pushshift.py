import http_utils
import log_utils

__session = None

__logger = log_utils.get_logger(__name__)


@http_utils.exponential_backoff(logger=__logger)
async def request(endpoint, **kwargs):
    global __session

    if __session is None:
        __logger.info("Initialising session...")
        __session = http_utils.ThrottledClientSession(1)
        __logger.info(f"Session created: {__session}")

    params = {
        "sort": "asc",
        "sort_type": "created_utc",
        "size": 100
    }

    for key, value in kwargs.items():
        if value is None:
            continue

        params[key] = value

    async with __session.get("https://api.pushshift.io/reddit/" + endpoint, params=params, timeout=30) as r:
        log_utils.log_response(r, __logger)

        if r.status == 200:
            response = await r.json()

            if response is None or response["data"] is None:
                raise http_utils.ResponseInvalid(f"No data returned for {r.url}")

            return response["data"]

        elif r.status == 429:
            raise http_utils.RateLimitExceeded

        raise http_utils.ResponseInvalid(f"No data returned for {r.url}")
