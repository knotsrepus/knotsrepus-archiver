import asyncio
import pathlib

import aiofiles.os
import aioboto3
import botocore.exceptions


class FileSystem:
    async def mkdir(self, path):
        pass

    async def write(self, path, data):
        pass

    async def write_raw(self, path, data):
        pass


class LocalFileSystem(FileSystem):
    def __init__(self, base_path):
        self.base_path = pathlib.Path(base_path)
        asyncio.get_event_loop().run_until_complete(self.mkdir(""))

    async def mkdir(self, path):
        full_path = self.base_path / path
        if not full_path.exists():
            try:
                await aiofiles.os.mkdir(full_path)
            except FileExistsError:  # The directory may have been created by another remote worker in the meantime
                pass

    async def write(self, path, data):
        async with aiofiles.open(self.base_path / path, mode="w") as file:
            await file.write(data)

    async def write_raw(self, path, data):
        async with aiofiles.open(self.base_path / path, mode="wb") as file:
            await file.write(data)


class S3FileSystem(FileSystem):
    def __init__(self, bucket_name, region_name=None, access_key_id=None, secret_access_key=None):
        self.bucket_name = bucket_name
        self.session = aioboto3.Session(region_name=region_name,
                                        aws_access_key_id=access_key_id,
                                        aws_secret_access_key=secret_access_key)

    async def mkdir(self, path):
        # Not required as folders aren't distinct objects in S3.
        pass

    async def write(self, path, data):
        async with self.session.resource("s3") as s3:
            bucket = await self.get_or_create_bucket(s3)
            await bucket.put_object(Body=data, Bucket=self.bucket_name, Key=str(path))

    async def write_raw(self, path, data):
        await self.write(path, data)

    async def get_or_create_bucket(self, s3):
        bucket = await s3.Bucket(self.bucket_name)

        if bucket.creation_date is None:
            try:
                await bucket.create(CreateBucketConfiguration={"LocationConstraint": self.session.region_name})
            except botocore.exceptions.ClientError as error:
                code = error.response["Error"]["Code"]
                message = error.response["Error"]["Message"]

                if code in ["BucketAlreadyExists", "BucketAlreadyOwnedByYou"]:
                    # Treat this operation as idempotent.
                    pass
                elif code == "OperationAborted" and "A conflicting conditional operation" in message:
                    # Treat this operation as idempotent.
                    pass
                else:
                    raise error

        return bucket
