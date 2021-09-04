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


class StubFileSystem(FileSystem):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def mkdir(self, path):
        self.logger.info(f"Stubbed: mkdir {path}")

    async def write(self, path, data):
        self.logger.info(f"Stubbed: write {len(data)} bytes to {path}")

    async def write_raw(self, path, data):
        self.logger.info(f"Stubbed: write_raw {len(data)} bytes to {path}")


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
