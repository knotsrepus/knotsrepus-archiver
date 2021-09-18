from abc import ABC, abstractmethod

import aioboto3

from src.common import log_utils


class MetadataService(ABC):
    @abstractmethod
    async def list(self):
        pass

    @abstractmethod
    async def get(self, submission_id: str):
        pass

    @abstractmethod
    async def put(self, submission_id: str, metadata):
        pass


class DynamoDBMetadataService(MetadataService):
    def __init__(self, session: aioboto3.Session, table_name: str):
        self.session = session
        self.table_name = table_name

    async def list(self):
        async with self.session.resource("dynamodb") as dynamodb:
            table = await dynamodb.Table(self.table_name)

            return await self.__paginate(table.scan)

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

    async def __paginate(self, query_func, **kwargs):
        result = []
        last_evaluated_key = None

        while True:
            if last_evaluated_key is not None:
                kwargs = {**kwargs, "ExclusiveStartKey": last_evaluated_key}

            response = await query_func(**kwargs)

            result.extend(response["Items"])

            last_evaluated_key = response.get("LastEvaluatedKey")
            if last_evaluated_key is None:
                break

        return result


class StubMetadataService(MetadataService):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def list(self):
        return [self.get(submission_id) for submission_id in ["testid", "testic", "testib", "testia", "testi9"]]

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

