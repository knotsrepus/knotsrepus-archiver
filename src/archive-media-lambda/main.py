import asyncio
import json
import os
from datetime import datetime

import aiohttp as aiohttp

from src.common import log_utils, http_utils, pushshift
from src.common.filesystem import S3FileSystem, StubFileSystem
from src.common.lambda_context import local_lambda_invocation


def handler(event, context):
    logger = log_utils.get_logger("archive-media-lambda")
    submission_id = event["Records"][0]["Sns"]["Message"]

    if context is local_lambda_invocation:
        filesystem = StubFileSystem()
    else:
        filesystem = S3FileSystem(os.environ.get("ARCHIVE_DATA_BUCKET"))

    return asyncio.get_event_loop().run_until_complete(handle(submission_id, filesystem, logger))


async def handle(submission_id, filesystem, logger):
    submission = (await pushshift.request("submission/search", ids=submission_id))[0]

    async with aiohttp.ClientSession() as session:
        logger.info(f"Archiving media for {submission_id}...")

        media = await get_media(session, submission, logger)

        await filesystem.mkdir(submission_id)

        for name, data in media:
            await filesystem.write_raw(f"{submission_id}/{name}", data)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "submission_id": submission_id,
                "last_updated": datetime.utcnow().timestamp(),
                "media": [name for name, _ in media]
            })
        }


@http_utils.exponential_backoff()
async def get_video(session, submission, logger):
    media = []

    async with session.get(submission["full_link"] + ".json", timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as r:
        log_utils.log_response(r, logger)
        if r.status == 200:
            response = await r.json()

            video_details = response[0]["data"]["children"][0]["data"]["secure_media"]["reddit_video"]

            if video_details["transcoding_status"] != "completed":
                return media

            video_link = video_details["fallback_url"]
            audio_link = submission["url"] + "/DASH_audio.mp4"

            async with session.get(video_link, timeout=30) as vr:
                log_utils.log_response(vr, logger)
                if vr.status == 200:
                    media.append(("video.mp4", await vr.read()))

            async with session.get(audio_link, timeout=30) as ar:
                log_utils.log_response(ar, logger)
                if ar.status == 200:
                    media.append(("audio.mp4", await ar.read()))

    return media


@http_utils.exponential_backoff()
async def get_image(session, url, logger):
    media = []

    async with session.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as r:
        log_utils.log_response(r, logger)
        if r.status == 200:
            image_name = url.rsplit("/", 1)[-1]
            media.append((image_name, await r.read()))

    return media


async def get_image_gallery(session, submission, logger):
    media = []

    for image in submission["media_metadata"].values():
        if image["status"] != "valid":
            continue

        source_image = image.get("s", None)
        if source_image is None:
            continue

        image_url = source_image.get("u", source_image.get("gif", None))

        if image_url is None:
            continue

        image_url = image_url.split("?", 1)[0].replace("preview.redd.it", "i.redd.it")
        media.extend(await get_image(session, image_url, logger))

    return media


def infer_media_type(submission):
    if submission.get("media_metadata", None) is not None:
        return "gallery"

    post_hint = submission.get("post_hint", None)
    if post_hint == "hosted:video":
        return "video"
    elif post_hint == "image":
        return "image"


async def get_media(session, submission, logger):
    media_type = infer_media_type(submission)

    if media_type == "video":
        return await get_video(session, submission, logger)
    elif media_type == "image":
        return await get_image(session, submission["url"], logger)
    elif media_type == "gallery":
        return await get_image_gallery(session, submission, logger)

    return []


if __name__ == "__main__":
    with open("event.json", "r") as file:
        event = json.load(file)

    handler(event, local_lambda_invocation)
