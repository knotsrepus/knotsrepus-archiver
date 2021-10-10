import asyncio
import json
import os
from datetime import datetime

import aioboto3

from src.common import log_utils
from src.common.archiver_config import DynamoDBConfigSource, StubConfigSource, ArchiverConfigSource
from src.common.filesystem import S3FileSystem, StubFileSystem, FileSystem
from src.common.metadata import DynamoDBMetadataService, StubMetadataService, MetadataService


def derive_post_type(flair_text: str):
    flair_keywords = {
        "dd": "dd",
        "discussion": "discussion",
        "opinion": "discussion",
        "shitpost": "shitpost",
        "meme": "shitpost",
        "social media": "social_media",
        "data": "data",
        "hodl": "fluff",
        "fluff": "fluff",
        "news": "news",
        "daily": "daily",
    }

    if flair_text is None:
        return "unknown"

    flair_text = flair_text.lower()

    for keyword, post_type in flair_keywords.items():
        if keyword in flair_text:
            return post_type

    return "unknown"


async def main(config_source: ArchiverConfigSource, filesystem: FileSystem, metadata_service: MetadataService):
    logger = log_utils.get_logger("metadata-generator")
    logger.info("Building KNOTSREPUS archive metadata...")

    command = await config_source.get_config("metadata_control_command")
    if command == "rebuild":
        logger.info("Metadata store rebuild was requested.")
        last_generated_metadata = ""
        await config_source.put_config(metadata_control_command="resume")
    else:
        last_generated_metadata = await config_source.get_config("last_generated_metadata") or ""
        if last_generated_metadata == "":
            logger.info("Metadata generation starting from the beginning.")
        else:
            logger.info(f"Metadata generation resuming from after submission '{last_generated_metadata}'.")

    metadata_count = 0

    async for dir in filesystem.list_dirs(StartAfter=last_generated_metadata):
        submission_id = dir.replace("/", "")

        logger.info(f"Creating metadata for '{submission_id}'...")

        data = json.loads(await filesystem.read(f"{submission_id}/post.json"))

        link_flair_text = data.get("link_flair_text")
        post_type = derive_post_type(link_flair_text)

        metadata = {
            "submission_id": submission_id,
            "created_utc": data["created_utc"],
            "author": data["author"],
            "title": data["title"],
            "score": data["score"],
            "post_type": post_type,
            "subreddit": data["subreddit"],
            "last_updated": int(datetime.utcnow().timestamp()),

            # All metadata items need to have a column with a constant value that can be used as a partition key in a
            # secondary index, to enable queries that order by score or creation time in reverse order without resorting
            # to an expensive table scan.
            "dummy": "",
        }

        await metadata_service.put(submission_id, metadata)

        last_generated_metadata = submission_id

        await config_source.put_config(last_generated_metadata=last_generated_metadata)

        metadata_count += 1

    logger.info(f"Metadata generated for {metadata_count} submissions.")


if __name__ == "__main__":
    session = aioboto3.Session()

    config_table_name = os.environ.get("CONFIG_TABLE_NAME")
    if config_table_name is not None:
        config_source = DynamoDBConfigSource(session, config_table_name)
    else:
        config_source = StubConfigSource()

    metadata_table_name = os.environ.get("METADATA_TABLE_NAME")
    if metadata_table_name is not None:
        metadata_service = DynamoDBMetadataService(session, metadata_table_name)
    else:
        metadata_service = StubMetadataService()

    archive_data_bucket = os.environ.get("ARCHIVE_DATA_BUCKET")
    if archive_data_bucket is not None:
        filesystem = S3FileSystem(archive_data_bucket)
    else:
        filesystem = StubFileSystem()

    asyncio.get_event_loop().run_until_complete(main(config_source, filesystem, metadata_service))
