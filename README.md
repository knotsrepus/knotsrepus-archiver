# knotsrepus-archiver

Archiver for https://reddit.com/r/Superstonk

## Getting Started

### Dependencies

- Python 3.8+
- pip
- [AWS Cloud Development Kit (CDK)](https://github.com/aws/aws-cdk)
- Docker
- An AWS account

### Setup

```shell
git clone https://github.com/knotsrepus/knotsrepus-archiver.git
cd knotsrepus-archiver/
python -m pip install -r requirements.txt -r src/requirements.txt
```

### Running
To generate the CloudFormation template:
```shell
npx aws-cdk synth
```

To deploy to AWS:
```shell
npx aws-cdk deploy
```

## Contributors

- KNOTSREPUS (aka [/u/VoxUmbra](https://reddit.com/u/VoxUmbra))

## License

This project is licensed under the Apache License 2.0.
