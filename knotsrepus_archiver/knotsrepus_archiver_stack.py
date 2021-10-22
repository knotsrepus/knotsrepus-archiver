import os

from aws_cdk import (
    aws_apigateway as apigateway,
    aws_certificatemanager as certificatemanager,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_lambda as lambda_,
    aws_route53 as route53,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    core
)

class KnotsrepusArchiverStack(core.Stack):
    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        archive_data_bucket = s3.Bucket(self, "ArchiveData")

        config_table = self.create_config_table()

        metadata_table = self.create_metadata_table()

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

        knotsrepus_api_backend_lambda, knotsrepus_api_gateway = self.create_knotsrepus_api(
            archive_data_bucket,
            metadata_table
        )

        cluster = self.create_cluster()

        submission_finder_task_definition = self.create_submission_finder_task_definition(archival_requested_topic,
                                                                                          config_table)

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

        metadata_generator_task_definition = self.create_metadata_generator_task_definition(archive_data_bucket,
                                                                                            config_table,
                                                                                            metadata_table)

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

    def create_config_table(self):
        config_table = dynamodb.Table(
            self,
            "Configuration",
            partition_key=dynamodb.Attribute(name="key", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="version", type=dynamodb.AttributeType.NUMBER),
        )

        return config_table

    def create_metadata_table(self):
        metadata_table = dynamodb.Table(
            self,
            "ArchiveMetadata",
            partition_key=dynamodb.Attribute(name="submission_id", type=dynamodb.AttributeType.STRING)
        )

        metadata_table.auto_scale_read_capacity(
            min_capacity=5,
            max_capacity=1000
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_write_capacity(
            min_capacity=5,
            max_capacity=1000
        ).scale_on_utilization(target_utilization_percent=70)

        metadata_table.add_global_secondary_index(
            index_name="ArchiveMetadataByDummyChronological",
            partition_key=dynamodb.Attribute(name="dummy", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_utc", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )
        metadata_table.add_global_secondary_index(
            index_name="ArchiveMetadataByDummyScore",
            partition_key=dynamodb.Attribute(name="dummy", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="score", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )
        metadata_table.add_global_secondary_index(
            index_name="ArchiveMetadataByAuthorChronological",
            partition_key=dynamodb.Attribute(name="author", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_utc", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )
        metadata_table.add_global_secondary_index(
            index_name="ArchiveMetadataByAuthorScore",
            partition_key=dynamodb.Attribute(name="author", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="score", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )
        metadata_table.add_global_secondary_index(
            index_name="ArchiveMetadataByPostTypeChronological",
            partition_key=dynamodb.Attribute(name="post_type", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="created_utc", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )
        metadata_table.add_global_secondary_index(
            index_name="ArchiveMetadataByPostTypeScore",
            partition_key=dynamodb.Attribute(name="post_type", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="score", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )

        metadata_table.auto_scale_global_secondary_index_read_capacity(
            index_name="ArchiveMetadataByDummyChronological",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_write_capacity(
            index_name="ArchiveMetadataByDummyChronological",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_read_capacity(
            index_name="ArchiveMetadataByDummyScore",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_write_capacity(
            index_name="ArchiveMetadataByDummyScore",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_read_capacity(
            index_name="ArchiveMetadataByAuthorChronological",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_write_capacity(
            index_name="ArchiveMetadataByAuthorChronological",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_read_capacity(
            index_name="ArchiveMetadataByAuthorScore",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_write_capacity(
            index_name="ArchiveMetadataByAuthorScore",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_read_capacity(
            index_name="ArchiveMetadataByPostTypeChronological",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_write_capacity(
            index_name="ArchiveMetadataByPostTypeChronological",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_read_capacity(
            index_name="ArchiveMetadataByPostTypeScore",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)
        metadata_table.auto_scale_global_secondary_index_write_capacity(
            index_name="ArchiveMetadataByPostTypeScore",
            min_capacity=5,
            max_capacity=1000,
        ).scale_on_utilization(target_utilization_percent=70)

        return metadata_table

    def create_knotsrepus_api(self, archive_data_bucket: s3.Bucket, metadata_table: dynamodb.Table):
        knotsrepus_api_backend_lambda = lambda_.Function(
            self,
            "KnotsrepusApiBackend",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="main.handler",
            code=KnotsrepusArchiverStack.get_lambda_asset("./knotsrepus-api-lambda"),
            environment={
                "ARCHIVE_DATA_BUCKET": archive_data_bucket.bucket_name,
                "METADATA_TABLE_NAME": metadata_table.table_name,
            }
        )

        archive_data_bucket.grant_read(knotsrepus_api_backend_lambda.role)
        metadata_table.grant_read_data(knotsrepus_api_backend_lambda.role)

        hosted_zone = route53.PublicHostedZone.from_lookup(
            self,
            "HostedZone",
            domain_name="knotsrepus.net",
            private_zone=False
        )

        certificate = certificatemanager.Certificate(
            self,
            "Certificate",
            domain_name="*.knotsrepus.net",
            subject_alternative_names=["knotsrepus.net"],
            validation=certificatemanager.CertificateValidation.from_dns(hosted_zone=hosted_zone)
        )

        domain_name = apigateway.DomainNameOptions(
            domain_name="api.knotsrepus.net",
            certificate=certificate
        )

        knotsrepus_api_gateway = apigateway.LambdaRestApi(
            self,
            "KnotsrepusApiGateway",
            handler=knotsrepus_api_backend_lambda,
            domain_name=domain_name,
            binary_media_types=["*/*"],
        )

        return knotsrepus_api_backend_lambda, knotsrepus_api_gateway

    def create_cluster(self):
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

        return cluster

    def create_submission_finder_task_definition(self, archival_requested_topic, config_table):
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

        config_table.grant_read_write_data(submission_finder_task_definition.task_role)
        archival_requested_topic.grant_publish(submission_finder_task_definition.task_role)

        return submission_finder_task_definition

    def create_metadata_generator_task_definition(self, archive_data_bucket, config_table, metadata_table):
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

        config_table.grant_read_write_data(metadata_generator_task_definition.task_role)
        metadata_table.grant_read_write_data(metadata_generator_task_definition.task_role)
        archive_data_bucket.grant_read(metadata_generator_task_definition.task_role)

        return metadata_generator_task_definition

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
