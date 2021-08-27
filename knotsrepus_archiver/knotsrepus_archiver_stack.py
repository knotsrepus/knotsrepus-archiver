import os

from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_ecs as ecs,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    core
)


class KnotsrepusArchiverStack(core.Stack):
    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        archive_data_bucket = s3.Bucket(self, "ArchiveData")

        config_table = dynamodb.Table(
            self,
            "Configuration",
            partition_key=dynamodb.Attribute(name="key", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="version", type=dynamodb.AttributeType.NUMBER),
        )

        archival_requested_topic = sns.Topic(
            self,
            "ArchivalRequested",
            display_name="Topic indicating that archival of a submission has been requested."
        )

        archive_submission_lambda = lambda_.Function(
            self,
            "ArchiveSubmission",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.handler",
            timeout=core.Duration.minutes(1),
            code=KnotsrepusArchiverStack.get_lambda_asset("./archive-submission-lambda"),
            environment={
                "ARCHIVE_DATA_BUCKET": archive_data_bucket.bucket_name
            }
        )

        archive_comments_lambda = lambda_.Function(
            self,
            "ArchiveComments",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.handler",
            timeout=core.Duration.minutes(1),
            code=KnotsrepusArchiverStack.get_lambda_asset("./archive-comments-lambda"),
            environment={
                "ARCHIVE_DATA_BUCKET": archive_data_bucket.bucket_name
            }
        )

        archive_media_lambda = lambda_.Function(
            self,
            "ArchiveMedia",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.handler",
            timeout=core.Duration.minutes(1),
            memory_size=1024,
            code=KnotsrepusArchiverStack.get_lambda_asset("./archive-media-lambda"),
            environment={
                "ARCHIVE_DATA_BUCKET": archive_data_bucket.bucket_name
            }
        )

        archive_data_bucket.grant_put(archive_submission_lambda.role)
        archive_data_bucket.grant_put(archive_comments_lambda.role)
        archive_data_bucket.grant_put(archive_media_lambda.role)

        archival_requested_topic.add_subscription(subscriptions.LambdaSubscription(archive_submission_lambda))
        archival_requested_topic.add_subscription(subscriptions.LambdaSubscription(archive_comments_lambda))
        archival_requested_topic.add_subscription(subscriptions.LambdaSubscription(archive_media_lambda))

        submission_finder_task_definition = ecs.FargateTaskDefinition(
            self,
            "SubmissionFinderDefinition",
            memory_limit_mib=512,
            cpu=256
        )

        submission_finder_task_definition.add_container(
            "SubmissionFinder",
            image=ecs.ContainerImage.from_asset(
                os.path.abspath("src"),
                file="submission-finder/Dockerfile"
            ),
            environment={
                "CONFIG_TABLE_NAME": config_table.table_name,
                "ARCHIVAL_REQUESTED_TOPIC_ARN": archival_requested_topic.topic_arn
            },
            logging=ecs.LogDriver.aws_logs(stream_prefix=core.Aws.STACK_NAME)
        )

        cluster = ecs.Cluster(
            self,
            "SubmissionFinderCluster",
            enable_fargate_capacity_providers=True
        )

        submission_finder_service = ecs.FargateService(
            self,
            "SubmissionFinderService",
            cluster=cluster,
            task_definition=submission_finder_task_definition,
            desired_count=1,
            capacity_provider_strategies=[
                ecs.CapacityProviderStrategy(capacity_provider="FARGATE", weight=1)
            ]
        )

        config_table.grant_read_write_data(submission_finder_task_definition.task_role)
        archival_requested_topic.grant_publish(submission_finder_task_definition.task_role)

    @staticmethod
    def get_lambda_asset(path: str) -> lambda_.Code:
        return lambda_.Code.from_asset(
            os.path.abspath("src"),
            bundling=core.BundlingOptions(
                image=lambda_.Runtime.PYTHON_3_8.bundling_image,
                command=[
                    "bash", "-c",
                    "pip install -r requirements.txt -t /asset-output "
                    f"&& cp -au {path}/* /asset-output"
                    "&& mkdir -p /asset-output/src/common"
                    "&& cp -au ./common/* /asset-output/src/common"
                ]
            )
        )
