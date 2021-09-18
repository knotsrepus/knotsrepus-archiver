from abc import ABC, abstractmethod
from datetime import datetime

import aioboto3
from boto3.dynamodb.conditions import Key

from src.common import log_utils


class ArchiverConfigSource(ABC):
    @abstractmethod
    async def get_config(self, key: str):
        pass

    @abstractmethod
    async def put_config(self, **kwargs):
        pass


class StubConfigSource(ArchiverConfigSource):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def get_config(self, key: str):
        if key == "after_utc":
            return 1623196800
        else:
            return "testid"

    async def put_config(self, **kwargs):
        self.logger.info(f"Stubbed: put_config {kwargs}")


class DynamoDBConfigSource(ArchiverConfigSource):
    def __init__(self, session: aioboto3.Session, table_name: str):
        self.session = session
        self.table_name = table_name

    async def get_config(self, key):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            response = await table.query(KeyConditionExpression=Key("key").eq(key))
            items = response["Items"]
            return max(items, key=lambda item: item["version"])["value"] if len(items) > 0 else None

    async def put_config(self, **kwargs):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            version = int(datetime.utcnow().timestamp())

            async with table.batch_writer() as batch:
                for key, value in kwargs.items():
                    await batch.put_item(
                        Item={
                            "key": key,
                            "version": version,
                            "value": value,
                        }
                    )
