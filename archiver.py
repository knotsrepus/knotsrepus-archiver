import asyncio
import datetime
import json

import aiohttp
import ray
import ray.util.queue

import http_utils
import performance
import pushshift
import ray_utils
import log_utils


class ArchiverJob:
    def __init__(self, filesystem, comments_workers=2, media_workers=2, after_utc=None, before_utc=None):
        self.logger = log_utils.get_logger(type(self).__name__)
        self.report = self.setup_report(after_utc, before_utc)

        self.filesystem = filesystem
        self.comments_workers = comments_workers
        self.media_workers = media_workers
        self.after_utc = after_utc
        self.before_utc = before_utc

    @staticmethod
    def setup_report(after_utc, before_utc) -> dict:
        return {
            "started": datetime.datetime.utcnow().timestamp(),
            "after_utc": after_utc,
            "before_utc": before_utc,
            "status": None,
            "submissions": dict(),
        }

    def execute(self):
        self.logger.info("Starting archiver job...")
        timer = performance.Timer()

        queue_gen = (ray.util.queue.Queue() for _ in range(3))
        submission_queues = list(queue_gen)

        pg = self.create_placement_group()

        retrieve_submissions = RetrieveSubmissionsStep.options(placement_group=pg).remote(submission_queues,
                                                                                          self.after_utc,
                                                                                          self.before_utc)

        actor_pools = self.get_actor_pools(pg)

        post_processors = self.get_post_processors()

        # Start the submission retrieval without waiting on it to complete, as items will be processed from the queue
        # when they are ready.
        _, _ = ray.wait([retrieve_submissions.execute.remote()], timeout=0)

        try:
            asyncio.get_event_loop().run_until_complete(asyncio.gather(*[
                self.process_submissions(pool, queue, fn)
                for (pool, queue, fn) in zip(actor_pools, submission_queues, post_processors)
            ]))
            self.report["status"] = "completed"
        except Exception:
            self.report["status"] = "failed"
            raise
        finally:
            json_str = json.dumps(self.report, ensure_ascii=True, indent=4)
            asyncio.get_event_loop().run_until_complete(self.filesystem.write(f"report-{self.report['started']}.json",
                                                                              json_str))

            self.logger.info(f"Job {self.report['status']}.")
            self.logger.info(f"Job took {timer.elapsed}.")

    def create_placement_group(self):
        total_workers = 2 + self.comments_workers + self.media_workers
        self.logger.info(f"Allocating {total_workers} workers...")
        pg = ray.util.placement_group(
            [{"CPU": 1, "pushshift-ratelimit": 60} for _ in range(total_workers)],
            strategy="SPREAD"
        )
        ray.get(pg.ready())
        self.logger.info("Allocation complete.")
        return pg

    def get_actor_pools(self, pg):
        actor_pools = [
            ray_utils.AdvancedActorPool([
                ArchiveSubmissionStep.options(placement_group=pg).remote(self.filesystem)
            ]),
            ray_utils.AdvancedActorPool([
                ArchiveCommentsStep.options(placement_group=pg).remote(self.filesystem)
                for _ in range(self.comments_workers)
            ]),
            ray_utils.AdvancedActorPool([
                ArchiveMediaStep.options(placement_group=pg).remote(self.filesystem) for _ in range(self.media_workers)
            ]),
        ]
        return actor_pools

    def get_post_processors(self):
        return [self.get_post_process_func(processor_type) for processor_type in ["submission", "comments", "media"]]

    @staticmethod
    async def process_submissions(actor_pool, queue, post_process_fn):
        async for result in actor_pool.map_unordered_queue_async(lambda a, v: a.execute.remote(v), queue):
            post_process_fn(result)

    def get_post_process_func(self, processor_type):
        def post_process(result):
            submission_id, report = result

            submission_report = self.report["submissions"].setdefault(submission_id, dict())
            submission_report[processor_type] = report

        return post_process


@ray.remote(resources={"pushshift-ratelimit": 60})
class RetrieveSubmissionsStep:
    FLAIR_DD = "DD 👨‍🔬"

    def __init__(self, submission_queues, after_utc=None, before_utc=None):
        self.logger = log_utils.get_logger(type(self).__name__)

        self.submission_queues = submission_queues
        self.after_utc = after_utc
        self.before_utc = before_utc

    async def execute(self):
        self.logger.info("Retrieving /r/superstonk submissions...")

        max_created_utc = self.after_utc
        submission_count = 0

        while True:
            chunk = await pushshift.request(
                "search/submission",
                subreddit="superstonk",
                after=max_created_utc,
                before=self.before_utc
            )

            if len(chunk) == 0:
                self.logger.info(f"{submission_count} submissions retrieved.")
                break

            chunk = list(filter(
                lambda submission: "link_flair_text" in submission and self.FLAIR_DD in submission["link_flair_text"],
                chunk
            ))

            if len(chunk) == 0:
                # Found no DD to archive, continue to the next chunk.
                continue

            submission_count += len(chunk)

            last = chunk[-1]
            max_created_utc = last["created_utc"]

            for queue in self.submission_queues:
                queue.put_nowait_batch(chunk)


@ray.remote
class ArchiveSubmissionStep:
    def __init__(self, filesystem):
        self.logger = log_utils.get_logger(type(self).__name__)

        self.filesystem = filesystem

    async def execute(self, submission):
        submission_id = submission["id"]
        self.logger.info(f"Archiving {submission_id}...")

        await self.filesystem.mkdir(submission_id)

        data = json.dumps(submission, ensure_ascii=True, indent=4)
        await self.filesystem.write(f"{submission_id}/post.json", data)

        return submission_id, {
            "last_updated": datetime.datetime.utcnow().timestamp(),
            "created_utc": submission["created_utc"],
        }


@ray.remote(resources={"pushshift-ratelimit": 60}, max_retries=2)
class ArchiveCommentsStep:
    def __init__(self, filesystem):
        self.logger = log_utils.get_logger(type(self).__name__)

        self.filesystem = filesystem

    async def execute(self, submission):
        submission_id = submission["id"]

        self.logger.info(f"Archiving comments for {submission_id}...")

        comment_ids = await self.get_comment_ids(submission_id)

        comments = await self.get_comments(comment_ids)

        await self.filesystem.mkdir(submission_id)

        data = json.dumps(comments, ensure_ascii=True, indent=4)
        await self.filesystem.write(f"{submission_id}/comments.json", data)

        return submission_id, {"last_updated": datetime.datetime.utcnow().timestamp()}

    @staticmethod
    async def get_comment_ids(submission_id):
        comment_ids = await pushshift.request(f"submission/comment_ids/{submission_id}")
        return comment_ids

    @staticmethod
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


@ray.remote(max_retries=2)
class ArchiveMediaStep:
    def __init__(self, filesystem):
        self.logger = log_utils.get_logger(type(self).__name__)
        self.session = aiohttp.ClientSession()

        self.filesystem = filesystem

    async def execute(self, submission):
        submission_id = submission["id"]

        self.logger.info(f"Archiving media for {submission_id}...")

        media = await self.get_media(submission)

        await self.filesystem.mkdir(submission_id)

        for name, data in media:
            await self.filesystem.write_raw(f"{submission_id}/{name}", data)

        return submission_id, {
            "last_updated": datetime.datetime.utcnow().timestamp(),
            "media": [name for name, _ in media],
        }

    def get(self, url, **kwargs):
        return self.session.get(url, **kwargs)

    @http_utils.exponential_backoff()
    async def get_video(self, submission):
        media = []

        async with self.get(submission["full_link"] + ".json", timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as r:
            log_utils.log_response(r, self.logger)
            if r.status == 200:
                response = await r.json()

                video_details = response[0]["data"]["children"][0]["data"]["secure_media"]["reddit_video"]

                if video_details["transcoding_status"] != "completed":
                    return media

                video_link = video_details["fallback_url"]
                audio_link = submission["url"] + "/DASH_audio.mp4"

                async with self.get(video_link, timeout=30) as vr:
                    log_utils.log_response(vr, self.logger)
                    if vr.status == 200:
                        media.append(("video.mp4", await vr.read()))

                async with self.get(audio_link, timeout=30) as ar:
                    log_utils.log_response(ar, self.logger)
                    if ar.status == 200:
                        media.append(("audio.mp4", await ar.read()))

        return media

    @http_utils.exponential_backoff()
    async def get_image(self, url):
        media = []

        async with self.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as r:
            log_utils.log_response(r, self.logger)
            if r.status == 200:
                image_name = url.rsplit("/", 1)[-1]
                media.append((image_name, await r.read()))

        return media

    async def get_image_gallery(self, submission):
        media = []

        for image in submission["media_metadata"].values():
            if image["status"] != "valid":
                continue

            source_image = image["s"]
            image_url = source_image.get("u", source_image.get("gif", None))

            if image_url is None:
                continue

            image_url = image_url.split("?", 1)[0].replace("preview.redd.it", "i.redd.it")
            media.extend(await self.get_image(image_url))

        return media

    @staticmethod
    def infer_media_type(submission):
        if submission.get("media_metadata", None) is not None:
            return "gallery"

        post_hint = submission.get("post_hint", None)
        if post_hint == "hosted:video":
            return "video"
        elif post_hint == "image":
            return "image"

    async def get_media(self, submission):
        media_type = self.infer_media_type(submission)

        if media_type == "video":
            return await self.get_video(submission)
        elif media_type == "image":
            return await self.get_image(submission["url"])
        elif media_type == "gallery":
            return await self.get_image_gallery(submission)

        return []
