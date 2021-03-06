import simplejson as json
import os

import aioboto3

from src.common.filesystem import StubFileSystem, S3FileSystem
from src.common.lambda_context import local_lambda_invocation

import api_controller
import rest
from src.common.metadata import StubMetadataService, DynamoDBMetadataService


def get_api_controller(context):
    if context is local_lambda_invocation:
        filesystem = StubFileSystem()
        metadata_service = StubMetadataService()
    else:
        session = aioboto3.Session()

        bucket_name = os.environ.get("ARCHIVE_DATA_BUCKET")
        metadata_table_name = os.environ.get("METADATA_TABLE_NAME")

        filesystem = S3FileSystem(bucket_name)
        metadata_service = DynamoDBMetadataService(session, metadata_table_name)

    return api_controller.ApiController(filesystem, metadata_service)


def format_response(status_code, headers, content_type, body):
    is_binary = any(prefix in content_type for prefix in ["image", "video", "audio"])

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": content_type,
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            **headers
        },
        "isBase64Encoded": is_binary,
        "body": body if is_binary else json.dumps(body)
    }


def dispatch_event_to_api_controller(event, context):
    path = event.get("path")

    if path.endswith("/"):
        path = path[:-1]

    if not rest.route_is_defined(path):
        return format_response(400,
                               "application/json",
                               {"message": f"No controller function defined to handle path '{path}'"})

    api = get_api_controller(context)
    query_params = event.get("queryStringParameters") or dict()

    (status, headers, content_type, body) = rest.dispatch(path, api, **query_params)

    return format_response(status, headers, content_type, body)


def handler(event, context):
    return dispatch_event_to_api_controller(event, context)


if __name__ == "__main__":
    with open("event.json", "r") as file:
        event = json.load(file)

    print(handler(event, local_lambda_invocation))
