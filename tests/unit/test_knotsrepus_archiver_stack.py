import json
import pytest

from aws_cdk import core
from knotsrepus_archiver.knotsrepus_archiver_stack import KnotsrepusArchiverStack


def get_template():
    app = core.App()
    KnotsrepusArchiverStack(app, "knotsrepus-archiver")
    return json.dumps(app.synth().get_stack("knotsrepus-archiver").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
