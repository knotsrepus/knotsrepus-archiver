import os

from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
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

        metadata_table = dynamodb.Table(
            self,
            "ArchiveMetadata",
            partition_key=dynamodb.Attribute(name="submission_id", type=dynamodb.AttributeType.STRING)
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

        vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="SubmissionFinderSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="MetadataGeneratorSubnet",
                    subnet_type=ec2.SubnetType.ISOLATED,
                    cidr_mask=24
                )
            ]
        )

        default_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "SecurityGroup",
            vpc.vpc_default_security_group
        )

        vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            security_groups=[default_security_group]
        )
        vpc.add_interface_endpoint(
            "EcrDockerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
            security_groups=[default_security_group]
        )
        vpc.add_interface_endpoint(
            "EcrApiEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.ECR,
            security_groups=[default_security_group]
        )
        vpc.add_interface_endpoint(
            "CloudWatchEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            security_groups=[default_security_group]
        )
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3
        )
        vpc.add_gateway_endpoint(
            "DynamoDbEndpoint",
            service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
        )

        cluster = ecs.Cluster(
            self,
            "Cluster",
            enable_fargate_capacity_providers=True,
            vpc=vpc
        )

        submission_finder_service = ecs.FargateService(
            self,
            "SubmissionFinderService",
            cluster=cluster,
            task_definition=submission_finder_task_definition,
            desired_count=1,
            capacity_provider_strategies=[
                ecs.CapacityProviderStrategy(capacity_provider="FARGATE", weight=1)
            ],
            assign_public_ip=True
        )

        config_table.grant_read_write_data(submission_finder_task_definition.task_role)
        archival_requested_topic.grant_publish(submission_finder_task_definition.task_role)

        metadata_generator_task_definition = ecs.FargateTaskDefinition(
            self,
            "MetadataGeneratorDefinition",
            memory_limit_mib=512,
            cpu=256
        )

        metadata_generator_task_definition.add_container(
            "MetadataGenerator",
            image=ecs.ContainerImage.from_asset(
                os.path.abspath("src"),
                file="metadata-generator/Dockerfile"
            ),
            environment={
                "CONFIG_TABLE_NAME": config_table.table_name,
                "METADATA_TABLE_NAME": metadata_table.table_name,
                "ARCHIVE_DATA_BUCKET": archive_data_bucket.bucket_name
            },
            logging=ecs.LogDriver.aws_logs(stream_prefix=core.Aws.STACK_NAME)
        )

        metadata_generator_service = ecs.FargateService(
            self,
            "MetadataGeneratorService",
            cluster=cluster,
            task_definition=metadata_generator_task_definition,
            desired_count=1,
            capacity_provider_strategies=[
                ecs.CapacityProviderStrategy(capacity_provider="FARGATE", weight=1)
            ],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.ISOLATED)
        )

        config_table.grant_read_write_data(metadata_generator_task_definition.task_role)
        metadata_table.grant_read_write_data(metadata_generator_task_definition.task_role)
        archive_data_bucket.grant_read(metadata_generator_task_definition.task_role)

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
