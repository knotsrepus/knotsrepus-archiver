import asyncio

import utils

__session = None

__logger = utils.get_logger(__name__)


async def request(endpoint, **kwargs):
    global __session

    if __session is None:
        __logger.info("Initialising session...")
        __session = utils.ThrottledClientSession(1)
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

    for i in range(5):
        # ThrottledClientSession only works per process, other workers may exist on the device if it has multiple cores.
        # Therefore, use exponential backoff if responses start hitting the rate limit.
        # Five attempts *should* be enough, but if not, an exception will be raised.
        async with __session.get("https://api.pushshift.io/reddit/" + endpoint, params=params, timeout=30) as r:
            utils.log_response(r, __logger)

            if r.status == 200:
                response = await r.json()
                return response["data"]
            elif r.status != 429:
                break

            delay = 2 ** i
            __logger.warning(f"Rate limit hit, retrying in {delay} sec.")
            await asyncio.sleep(2 ** i)

    raise Exception("The rate limit was exceeded and could not be resolved after the maximum number of retries.")
