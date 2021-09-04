from abc import ABC, abstractmethod
from datetime import datetime

import aioboto3
from boto3.dynamodb.conditions import Key
from pydantic import BaseModel

from src.common import log_utils


class ArchiverConfig(BaseModel):
    after_utc: int


class ArchiverConfigSource(ABC):
    @abstractmethod
    async def get_config(self):
        pass

    @abstractmethod
    async def put_config(self, config: ArchiverConfig):
        pass


class StubConfigSource(ArchiverConfigSource):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def get_config(self):
        return ArchiverConfig(after_utc=1623196800)

    async def put_config(self, config: ArchiverConfig):
        self.logger.info(f"Stubbed: put_config {config}")


class DynamoDBConfigSource(ArchiverConfigSource):
    def __init__(self, session: aioboto3.Session, table_name: str):
        self.session = session
        self.table_name = table_name

    async def get_config(self):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            response = await table.query(KeyConditionExpression=Key("key").eq("after_utc"))
            items = response["Items"]
            after_utc = int(max(items, key=lambda item: item["version"])["value"]) if len(items) > 0 else 0

            return ArchiverConfig(after_utc=after_utc)

    async def put_config(self, config: ArchiverConfig):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            version = int(datetime.utcnow().timestamp())

            await table.put_item(
                Item={
                    "key": "after_utc",
                    "version": version,
                    "value": config.after_utc,
                }
            )
