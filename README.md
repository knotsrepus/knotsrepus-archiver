# knotsrepus-archiver

Archiver for https://reddit.com/r/Superstonk

## Getting Started

### Dependencies

- Python 3.8+
- pip
- NPM and the [AWS Cloud Development Kit (CDK)](https://github.com/aws/aws-cdk)
- Docker
- An AWS account

### Setup

```shell
git clone https://github.com/knotsrepus/knotsrepus-archiver.git
cd knotsrepus-archiver/
python -m pip install -r requirements.txt -r src/requirements.txt
```

### Running

#### Local development
The Lambda functions can be tested locally by making changes to their `event.json` file and then
invoking them as a regular Python script:

```shell
cd src/archive-comments-lambda
PYTHONPATH=../../ python main.py 
```

ECS services can be tested locally either by invoking them as a Python script or through Docker.

To invoke as a regular script:

```shell
cd src/submission-finder
PYTHONPATH=../../ python main.py
```

To use Docker:

```shell
cd src
docker build -f submission-finder/Dockerfile -t knotsrepus-archiver-submission-finder .
docker run --name submission-finder knotsrepus-archiver-submission-finder
```

#### AWS
To generate the CloudFormation template:
```shell
npx aws-cdk synth
```

To deploy to AWS:
```shell
npx aws-cdk deploy
```

In order to successfully deploy, the credentials used will require the following policy document:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutAccessPointPolicyForObjectLambda",
                "s3:PutBucketPublicAccessBlock",
                "sns:GetTopicAttributes",
                "sns:DeleteTopic",
                "sns:CreateTopic",
                "ecs:DeleteService",
                "s3:DeleteBucketPolicy",
                "s3:BypassGovernanceRetention",
                "ecs:DescribeClusters",
                "ecs:UpdateService",
                "iam:PassRole",
                "s3:ObjectOwnerOverrideToBucketOwner",
                "ecs:CreateService",
                "s3:DeleteAccessPointPolicyForObjectLambda",
                "s3:PutObjectVersionAcl",
                "s3:PutBucketAcl",
                "s3:PutBucketPolicy",
                "sns:Subscribe",
                "s3:DeleteAccessPointPolicy",
                "ecs:DescribeServices",
                "s3:PutAccessPointPolicy",
                "s3:PutObjectAcl",
                "ecs:PutClusterCapacityProviders"
            ],
            "Resource": [
                "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/*",
                "arn:aws:sns:*:<YOUR_ACCOUNT_ID>:KnotsrepusArchiver-*",
                "arn:aws:s3:::cdk-stagingbucket-*",
                "arn:aws:s3:::knotrepusarchiver-*",
                "arn:aws:s3:*:<YOUR_ACCOUNT_ID>:accesspoint/*",
                "arn:aws:s3:::*/*",
                "arn:aws:s3-object-lambda:*:<YOUR_ACCOUNT_ID>:accesspoint/*",
                "arn:aws:ecs:*:<YOUR_ACCOUNT_ID>:cluster/KnotsrepusArchiver-*",
                "arn:aws:ecs:*:<YOUR_ACCOUNT_ID>:capacity-provider/KnotsrepusArchiver-*",
                "arn:aws:ecs:*:<YOUR_ACCOUNT_ID>:service/KnotsrepusArchiver-*"
            ]
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "ecs:DeleteCluster",
            "Resource": "arn:aws:ecs:*:<YOUR_ACCOUNT_ID>:cluster/KnotsrepusArchiver-*"
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": [
                "logs:GetLogRecord",
                "sns:Unsubscribe",
                "logs:GetLogDelivery",
                "ecr:PutRegistryPolicy",
                "logs:ListLogDeliveries",
                "ecs:DeregisterTaskDefinition",
                "ec2:DescribeInternetGateways",
                "logs:DeleteResourcePolicy",
                "ecs:RegisterTaskDefinition",
                "logs:CancelExportTask",
                "logs:DeleteLogDelivery",
                "logs:DescribeQueryDefinitions",
                "logs:PutDestination",
                "logs:DescribeResourcePolicies",
                "logs:DescribeDestinations",
                "logs:DescribeQueries",
                "ecr:GetRegistryPolicy",
                "s3:PutAccountPublicAccessBlock",
                "ecs:CreateCluster",
                "logs:PutDestinationPolicy",
                "ecr:DescribeRegistry",
                "ecr:GetAuthorizationToken",
                "logs:StopQuery",
                "logs:TestMetricFilter",
                "logs:DeleteDestination",
                "logs:DeleteQueryDefinition",
                "logs:PutQueryDefinition",
                "logs:CreateLogDelivery",
                "logs:PutResourcePolicy",
                "logs:DescribeExportTasks",
                "logs:GetQueryResults",
                "logs:UpdateLogDelivery",
                "ec2:DescribeVpcs",
                "ec2:*",
                "ecr:DeleteRegistryPolicy",
                "ecr:PutReplicationConfiguration"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor3",
            "Effect": "Allow",
            "Action": "logs:*",
            "Resource": [
                "arn:aws:logs:*:<YOUR_ACCOUNT_ID>:log-group:*:log-stream:*",
                "arn:aws:logs:*:<YOUR_ACCOUNT_ID>:log-group:KnotsrepusArchiver-*"
            ]
        },
        {
            "Sid": "VisualEditor4",
            "Effect": "Allow",
            "Action": "ecr:*",
            "Resource": "arn:aws:ecr:*:<YOUR_ACCOUNT_ID>:repository/aws-cdk/assets"
        }
    ]
}
```

## Contributors

- KNOTSREPUS (aka [/u/VoxUmbra](https://reddit.com/u/VoxUmbra))

## License

This project is licensed under the Apache License 2.0.
