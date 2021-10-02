from abc import ABC, abstractmethod

import aioboto3
from boto3.dynamodb.conditions import ConditionBase

from src.common import log_utils


class MetadataService(ABC):
    @abstractmethod
    async def list(self, after_id=None, limit=100, sort=None, sort_order="asc"):
        pass

    @abstractmethod
    async def get(self, submission_id: str):
        pass

    @abstractmethod
    async def put(self, submission_id: str, metadata):
        pass

    @abstractmethod
    async def query(self, key_condition, filter_condition=None, after_id=None, limit=100, sort=None, sort_order="asc"):
        pass


class DynamoDBMetadataService(MetadataService):
    index_for_key_and_sort = {
        ("author", "created_utc"): "ArchiveMetadataByAuthorChronological",
        ("author", "score"): "ArchiveMetadataByAuthorScore",
        ("created_utc", None): "ArchiveMetadataByCreatedUtc",
        ("post_type", "created_utc"): "ArchiveMetadataByPostTypeChronological",
        ("post_type", "score"): "ArchiveMetadataByPostTypeScore",
        ("score", None): "ArchiveMetadataByScore",
    }

    def __init__(self, session: aioboto3.Session, table_name: str):
        self.session = session
        self.table_name = table_name

    async def list(self, after_id=None, limit=100, sort=None, sort_order="asc"):
        if sort_order not in ["asc", "desc"]:
            raise Exception("The sort order must be either 'asc' or 'desc'.")

        limit = min(limit, 100)
        if sort is None:
            index_name = None
        else:
            index_name = DynamoDBMetadataService.index_for_key_and_sort[(sort, None)]

        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            if after_id is None:
                start_key = None
            else:
                start_key = {
                    "submission_id": after_id,
                }
                item = (await table.get_item(Key={"submission_id": after_id})).get("Item")
                if sort is not None:
                    start_key[sort] = item.get(sort)

            response = await table.scan(
                IndexName=index_name,
                ScanIndexForward=sort_order == "asc",
                ExclusiveStartKey=start_key,
                Limit=limit,
            )

            return response["Items"]

    async def get(self, submission_id: str):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            response = await table.get_item(Key={"submission_id": submission_id})

            return response.get("Item")

    async def put(self, submission_id: str, metadata: dict):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            await table.put_item(
                Item={
                    "submission_id": submission_id,
                    **metadata
                }
            )

    async def query(self, key_condition, filter_condition=None, after_id=None, limit=100, sort=None, sort_order="asc"):
        if not isinstance(key_condition, ConditionBase):
            raise Exception("The key condition must be a boto3 condition object.")

        if filter_condition is not None and not isinstance(filter_condition, ConditionBase):
            raise Exception("The filter condition must be None or a boto3 condition object.")

        if sort not in [None, "created_utc", "score"]:
            raise Exception("The sort attribute must be None, 'created_utc', or 'score'.")

        if sort_order not in ["asc", "desc"]:
            raise Exception("The sort order must be either 'asc' or 'desc'.")

        limit = min(limit, 100)
        key_name = key_condition.get_expression().values()[0].name
        index_name = DynamoDBMetadataService.index_for_key_and_sort[(key_name, sort)]

        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            if after_id is None:
                start_key = None
            else:
                start_key = {
                    "submission_id": after_id,
                }
                item = (await table.get_item(Key={"submission_id": after_id})).get("Item")
                start_key[key_name] = item.get(key_name)
                if sort is not None:
                    start_key[sort] = item.get(sort)

            response = await table.query(
                IndexName=index_name,
                KeyConditionExpression=key_condition,
                FilterExpression=filter_condition,
                ScanIndexForward=sort_order == "asc",
                ExclusiveStartKey=start_key,
                Limit=limit,
            )

            return response["Items"]


class StubMetadataService(MetadataService):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def list(self, after_id=None, limit=100, sort=None, sort_order="asc"):
        return [await self.get(submission_id) for submission_id in ["testid", "testic", "testib", "testia", "testi9"]]

    async def get(self, submission_id: str):
        return {
            "submission_id": submission_id,
            "created_utc": 1630450800,
            "author": "VoxUmbra",
            "title": submission_id,
            "score": 1,
            "post_type": "DD",
        }

    async def put(self, submission_id: str, metadata):
        self.logger.info(f"Stubbed: put {submission_id} {metadata}")

    async def query(self, key_condition, filter_condition=None, after_id=None, limit=100, sort=None, sort_order="asc"):
        return await self.list()
