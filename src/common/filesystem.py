import json
from abc import ABC, abstractmethod

import aioboto3

from src.common import log_utils


class FileSystem(ABC):
    @abstractmethod
    async def mkdir(self, path):
        pass

    @abstractmethod
    async def write(self, path, data):
        pass

    @abstractmethod
    async def write_raw(self, path, data):
        pass

    # noinspection PyUnreachableCode
    @abstractmethod
    async def list_dirs(self, **kwargs):
        if False:
            yield

    @abstractmethod
    async def read(self, path):
        pass


class StubFileSystem(FileSystem):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def mkdir(self, path):
        self.logger.info(f"Stubbed: mkdir {path}")

    async def write(self, path, data):
        self.logger.info(f"Stubbed: write {len(data)} bytes to {path}")

    async def write_raw(self, path, data):
        self.logger.info(f"Stubbed: write_raw {len(data)} bytes to {path}")

    async def list_dirs(self, **kwargs):
        for dir in ["testid", "testic", "testib", "testia", "testi9"]:
            yield f"{dir}/"

    async def read(self, path):
        return json.dumps({
            "link_flair_text": "DD",
            "created_utc": 1630450800,
            "author": "VoxUmbra",
            "title": path,
            "score": 1,
        })


class S3FileSystem(FileSystem):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.session = aioboto3.Session()

    async def mkdir(self, path):
        # Not required as folders aren't distinct objects in S3.
        pass

    async def write(self, path, data):
        async with self.session.resource("s3") as s3:
            bucket = await s3.Bucket(self.bucket_name)
            await bucket.put_object(Body=data, Bucket=self.bucket_name, Key=str(path))

    async def write_raw(self, path, data):
        await self.write(path, data)

    async def list_dirs(self, **kwargs):
        async with self.session.client("s3") as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for result in paginator.paginate(Bucket=self.bucket_name, Delimiter="/", **kwargs):
                for prefix in result["CommonPrefixes"]:
                    yield prefix["Prefix"]

    async def read(self, path):
        async with self.session.client("s3") as s3:
            response = await s3.get_object(Bucket=self.bucket_name, Key=path)
            body = await response["Body"]

            return await body.read()
