import base64
import json
import mimetypes

from boto3.dynamodb.conditions import Key, Attr

import rest
from src.common.filesystem import FileSystem
from src.common.metadata import MetadataService
from src.common.syncio import run_synchronously, iterate_synchronously


class ApiController:
    def __init__(self, filesystem: FileSystem, metadata_service: MetadataService):
        self.filesystem = filesystem
        self.metadata_service = metadata_service

    @rest.route(path="/submission")
    def get_submissions(self, author=None, post_type=None, sort="created_utc", sort_order="asc", after_id=None, count=100, **kwargs):
        count = min(count, 100)

        sort_key = Key(sort).gte(0)
        if author is None and post_type is None:
            coroutine = self.metadata_service.query(
                sort_key,
                after_id=after_id,
                limit=count,
                sort_order=sort_order
            )
        elif author is not None:
            key_condition = Key("author").eq(author) & sort_key
            if post_type is None:
                coroutine = self.metadata_service.query(
                    key_condition,
                    after_id=after_id,
                    limit=count,
                    sort=sort,
                    sort_order=sort_order
                )
            else:
                coroutine = self.metadata_service.query(
                    key_condition,
                    Attr("post_type").eq(post_type),
                    after_id=after_id,
                    limit=count,
                    sort=sort,
                    sort_order=sort_order
                )
        else:
            coroutine = self.metadata_service.query(
                Key("post_type").eq(post_type) & sort_key,
                after_id=after_id,
                limit=count,
                sort=sort,
                sort_order=sort_order
            )

        data = run_synchronously(coroutine)

        if data is not None:
            return "application/json", data

        return None

    @rest.route(path="/submission/{submission_id}")
    def get_submission(self, submission_id, **kwargs):
        data = run_synchronously(self.filesystem.read(f"{submission_id}/post.json"))

        if data is not None:
            return "application/json", json.loads(data)

        return None

    @rest.route(path="/submission/{submission_id}/comments")
    def get_comments(self, submission_id, **kwargs):
        data = run_synchronously(self.filesystem.read(f"{submission_id}/comments.json"))

        if data is not None:
            return "application/json", json.loads(data)

        return None

    @rest.route(path="/submission/{submission_id}/media")
    def get_media_list(self, submission_id, **kwargs):
        media = iterate_synchronously(self.filesystem.list_files(submission_id))

        return "application/json", list(media)

    @rest.route(path="/submission/{submission_id}/media/{filename}")
    def get_media_object(self, submission_id, filename, **kwargs):
        data = run_synchronously(self.filesystem.read(f"{submission_id}/{filename}"))

        if data is not None:
            content_type, _ = mimetypes.guess_type(filename)
            return content_type, base64.b64encode(data).decode("utf-8")

        return None
