from abc import ABC, abstractmethod
from typing import Union

import aioboto3
from boto3.dynamodb.conditions import ConditionBase, AttributeBase

from src.common import log_utils


class MetadataService(ABC):
    @abstractmethod
    async def list(self, after_id=None, limit=100):
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

    async def list(self, after_id=None, limit=100):
        limit = min(limit, 100)

        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            kwargs = {
                "Limit": limit,
            }

            if after_id is not None:
                kwargs["ExclusiveStartKey"] = {
                    "submission_id": after_id,
                }

            response = await table.scan(**kwargs)

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

        key_name = DynamoDBMetadataService.get_key_name(key_condition)
        index_name = DynamoDBMetadataService.index_for_key_and_sort[(key_name, sort)]

        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            kwargs = {
                "IndexName": index_name,
                "KeyConditionExpression": key_condition,
                "ScanIndexForward": sort_order == "asc",
                "Limit": limit,
            }

            if filter_condition is not None:
                kwargs["FilterExpression"] = filter_condition

            if after_id is not None:
                start_key = {
                    "submission_id": after_id,
                }
                item = (await table.get_item(Key={"submission_id": after_id})).get("Item")
                start_key[key_name] = item.get(key_name)
                if sort is not None:
                    start_key[sort] = item.get(sort)

                kwargs["ExclusiveStartKey"] = start_key

            response = await table.query(**kwargs)

            return response["Items"]

    @staticmethod
    def get_key_name(key_condition: Union[ConditionBase, AttributeBase]):
        expr_values = key_condition.get_expression()["values"]
        sub_expr = expr_values[0]

        if isinstance(sub_expr, AttributeBase):
            return sub_expr.name

        return DynamoDBMetadataService.get_key_name(sub_expr)


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
            "subreddit": "Superstonk",
            "last_updated": 1630460000,
            "dummy": "",
        }

    async def put(self, submission_id: str, metadata):
        self.logger.info(f"Stubbed: put {submission_id} {metadata}")

    async def query(self, key_condition, filter_condition=None, after_id=None, limit=100, sort=None, sort_order="asc"):
        return await self.list()
