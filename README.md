# knotsrepus-archiver

Archiver for https://reddit.com/r/Superstonk

## Getting Started

### Dependencies

- Python 3.8+
- pip
- [Ray](https://docs.ray.io/en/master/installation.html)
- Docker (optional, recommended for Windows)
- An AWS account (required for cluster mode)

### Installing

```shell
git clone https://github.com/knotsrepus/knotsrepus-archiver.git
cd knotsrepus-archiver/
python -m pip install -r requirements.txt
```

### Running

#### Local mode
This will run all workers on your device and output the result to the specified folder.

```shell
python main.py --local $OUTPUT_PATH -a $AFTER_UTC -b $BEFORE_UTC -c $LOCAL_COMMENTS_WORKERS -m $LOCAL_MEDIA_WORKERS
```

#### Cluster mode
This will run workers on an AWS cluster and output the result into an S3 bucket.
Note that you will need an AWS account to use this feature.

```shell
ray up --no-config-cache config.yaml
ray exec --no-config-cache config.yaml "cd ~/app/ && python main.py --cluster $CLUSTER_BUCKET $CLUSTER_REGION $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY -a $AFTER_UTC -b $BEFORE_UTC -c $CLUSTER_COMMENTS_WORKERS -m $CLUSTER_MEDIA_WORKERS"
ray down config.yaml
```

#### Docker
The Docker image supports both local and cluster mode.

```shell
docker build -t knotsrepus-archiver .
docker run -p 8265:8265 -v ./out/:/app/out/ --shm-size 2gb knotsrepus-archiver $ARGS 
```

## Help

```shell
python main.py -h
```

- `-h`, `--help`: show the help message and exit
- `--local $OUTPUT_PATH`: runs the archiver locally and outputs to the specified directory
- `--cluster $CLUSTER_BUCKET $CLUSTER_REGION $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY`:
  runs the archiver on AWS in the specified region and outputs to the specified S3 bucket
- `-a $TIME`, `--after $TIME`: archive submissions after a particular time. 
  Accepts either an ISO 8601 datetime or a UNIX timestamp.
- `-b $TIME`, `--before $TIME`: archive submissions before a particular time.
  Accepts either an ISO 8601 datetime or a UNIX timestamp.
- `-c $COMMENTS_WORKERS`, `--comments-workers $COMMENTS_WORKERS`:
  specifies the number of workers to use when archiving comments (default: 2)
- `-m $MEDIA_WORKERS`, `--media-workers $MEDIA_WORKERS`:
  specifies the number of workers to use when archiving media (default: 2)


## Contributors

- KNOTSREPUS (aka [/u/VoxUmbra](https://reddit.com/u/VoxUmbra))

## License

This project is licensed under the Apache License 2.0.
