import asyncio
import os
from datetime import datetime

import aioboto3

from src.common import log_utils, pushshift
from src.common.archiver_config import DynamoDBConfigSource, StubConfigSource, ArchiverConfigSource, ArchiverConfig
from src.common.messaging import SNSMessagingService, StubMessagingService, MessagingService


async def main(config_source: ArchiverConfigSource, messaging_service: MessagingService):
    logger = log_utils.get_logger("submission-finder")
    logger.info("Retrieving /r/superstonk submissions...")

    config = await config_source.get_config()
    after_utc = config.after_utc
    submission_count = 0

    while True:
        logger.info("Requesting submissions after %s...",
                    datetime.fromtimestamp(after_utc).isoformat())

        chunk = await pushshift.request(
            "search/submission",
            subreddit="superstonk",
            after=after_utc
        )

        if len(chunk) == 0:
            logger.info(f"{submission_count} submissions retrieved.")
            break

        last = chunk[-1]
        after_utc = last["created_utc"]

        submission_count += len(chunk)

        for submission in chunk:
            await messaging_service.send_message(submission["id"])

        await config_source.put_config(ArchiverConfig(after_utc=after_utc))


if __name__ == "__main__":
    session = aioboto3.Session()

    table_name = os.environ.get("CONFIG_TABLE_NAME")
    if table_name is not None:
        config_source = DynamoDBConfigSource(session, table_name)
    else:
        config_source = StubConfigSource()

    topic_arn = os.environ.get("ARCHIVAL_REQUESTED_TOPIC_ARN")
    if topic_arn is not None:
        messaging_service = SNSMessagingService(session, topic_arn)
    else:
        messaging_service = StubMessagingService()

    asyncio.get_event_loop().run_until_complete(main(config_source, messaging_service))
