import asyncio
import json
import os
from datetime import datetime

from src.common import log_utils, pushshift
from src.common.filesystem import S3FileSystem, StubFileSystem
from src.common.lambda_context import local_lambda_invocation


def handler(event, context):
    logger = log_utils.get_logger("archive-submission-lambda")
    submission_id = event["Records"][0]["Sns"]["Message"]

    if context is local_lambda_invocation:
        filesystem = StubFileSystem()
    else:
        filesystem = S3FileSystem(os.environ.get("ARCHIVE_DATA_BUCKET"))

    return asyncio.get_event_loop().run_until_complete(handle(submission_id, logger, filesystem))


async def handle(submission_id, logger, filesystem):
    submission = (await pushshift.request("submission/search", ids=submission_id))[0]

    logger.info(f"Archiving {submission_id}...")

    await filesystem.mkdir(submission_id)

    data = json.dumps(submission, ensure_ascii=True, indent=4)
    await filesystem.write(f"{submission_id}/post.json", data)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "submission_id": submission_id,
            "last_updated": datetime.utcnow().timestamp()
        })
    }


if __name__ == "__main__":
    with open("event.json", "r") as file:
        event = json.load(file)

    handler(event, local_lambda_invocation)
