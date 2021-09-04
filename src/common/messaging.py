from abc import ABC, abstractmethod

import aioboto3

from src.common import log_utils


class MessagingService(ABC):
    @abstractmethod
    async def send_message(self, message: str):
        pass


class StubMessagingService(MessagingService):
    def __init__(self):
        self.logger = log_utils.get_logger(__name__)

    async def send_message(self, message: str):
        self.logger.info(f"Stubbed: send_message {message}")


class SNSMessagingService(MessagingService):
    def __init__(self, session: aioboto3.Session, topic_arn: str):
        self.session = session
        self.topic_arn = topic_arn

    async def send_message(self, message: str):
        async with self.session.client("sns") as sns:
            await sns.publish(
                TopicArn=self.topic_arn,
                Message=message
            )
