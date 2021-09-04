import asyncio
import json
import os
from datetime import datetime

from src.common import log_utils, pushshift
from src.common.filesystem import S3FileSystem, StubFileSystem
from src.common.lambda_context import local_lambda_invocation


def handler(event, context):
    logger = log_utils.get_logger("archive-comments-lambda")
    submission_id = event["Records"][0]["Sns"]["Message"]

    if context is local_lambda_invocation:
        filesystem = StubFileSystem()
    else:
        filesystem = S3FileSystem(os.environ.get("ARCHIVE_DATA_BUCKET"))

    return asyncio.get_event_loop().run_until_complete(handle(submission_id, filesystem, logger))


async def handle(submission_id, filesystem, logger):
    logger.info(f"Archiving comments for {submission_id}...")

    comment_ids = await get_comment_ids(submission_id)

    comments = await get_comments(comment_ids)

    await filesystem.mkdir(submission_id)

    data = json.dumps(comments, ensure_ascii=True, indent=4)
    await filesystem.write(f"{submission_id}/comments.json", data)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "submission_id": submission_id,
            "last_updated": datetime.utcnow().timestamp()
        })
    }


async def get_comment_ids(submission_id):
    comment_ids = await pushshift.request(f"submission/comment_ids/{submission_id}")
    return comment_ids


async def get_comments(comment_ids):
    if comment_ids is None:
        return []

    chunk_size = 256

    id_chunks = [comment_ids[x: x + chunk_size] for x in range(0, len(comment_ids), chunk_size)]
    comments = []

    for id_chunk in id_chunks:
        ids = ",".join(id_chunk)
        chunk = await pushshift.request("search/comment", ids=ids)

        comments.extend(chunk)

    return comments


if __name__ == "__main__":
    with open("event.json", "r") as file:
        event = json.load(file)

    handler(event, local_lambda_invocation)
