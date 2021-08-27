import asyncio
import os
from datetime import datetime

import aioboto3
from boto3.dynamodb.conditions import Key

from src.common import log_utils, pushshift


async def get_config(session: aioboto3.Session):
    async with session.resource("dynamodb") as dynamodb:
        table = await dynamodb.Table(os.environ.get("CONFIG_TABLE_NAME"))

        response = await table.query(KeyConditionExpression=Key("key").eq("after_utc"))
        items = response["Items"]
        after_utc = int(max(items, key=lambda item: item["version"])["value"]) if len(items) > 0 else 0

        return {
            "after_utc": after_utc,
        }


async def put_config(session: aioboto3.Session, config: dict):
    async with session.resource("dynamodb") as dynamodb:
        table = await dynamodb.Table(os.environ.get("CONFIG_TABLE_NAME"))

        await table.put_item(
            Item={
                "key": "after_utc",
                "version": int(datetime.utcnow().timestamp()),
                "value": int(config["after_utc"]),
            }
        )


async def request_archival(session: aioboto3.Session, submission_id: str):
    async with session.client("sns") as sns:
        await sns.publish(
            TopicArn=os.environ.get("ARCHIVAL_REQUESTED_TOPIC_ARN"),
            Message=submission_id
        )


async def main():
    logger = log_utils.get_logger("submission-finder")
    logger.info("Retrieving /r/superstonk submissions...")

    session = aioboto3.Session()
    config = await get_config(session)
    after_utc = config["after_utc"]
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
            await request_archival(session, submission["id"])

        await put_config(session, {"after_utc": after_utc})


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
