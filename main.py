import argparse
from datetime import datetime

import dateutil.parser
import ray

import archiver
import filesystem
import log_utils


def iso_8601_or_unix_timestamp(string):
    try:
        return int(round(dateutil.parser.isoparse(string).timestamp()))
    except ValueError:
        return int(string)


def parse_args():
    arg_parser = argparse.ArgumentParser(prog="knotsrepus-archiver", description="Runs the knotsrepus archiver.")
    run_mode_group = arg_parser.add_mutually_exclusive_group(required=True)
    run_mode_group.add_argument("--local",
                                metavar="base-path",
                                default=argparse.SUPPRESS,
                                help="runs the archiver locally and outputs to the specified directory")
    run_mode_group.add_argument("--cluster",
                                metavar=("bucket", "region", "access-key-id", "secret-access-key"),
                                nargs=4,
                                default=argparse.SUPPRESS,
                                help="runs the archiver on AWS in the specified region and outputs to the specified "
                                     "S3 bucket")
    arg_parser.add_argument("-a",
                            "--after",
                            type=iso_8601_or_unix_timestamp,
                            metavar="time",
                            help="archive submissions after a particular time. Accepts either a ISO 8601 datetime or "
                                 "a UNIX timestamp.")
    arg_parser.add_argument("-b",
                            "--before",
                            type=iso_8601_or_unix_timestamp,
                            metavar="time",
                            help="archive submissions before a particular time. Accepts either a ISO 8601 datetime or "
                                 "a UNIX timestamp.")
    arg_parser.add_argument("-c",
                            "--comments-workers",
                            type=int,
                            default=2,
                            metavar="n",
                            help="specifies the number of workers to use when archiving comments (default: 2)")
    arg_parser.add_argument("-m",
                            "--media-workers",
                            type=int,
                            default=2,
                            metavar="n",
                            help="specifies the number of workers to use when archiving media (default: 2)")

    return arg_parser.parse_args()


if __name__ == "__main__":
    logger = log_utils.get_logger("knotsrepus-archiver")

    args = parse_args()

    after_utc = args.after
    before_utc = args.before
    archive_comments_workers = args.comments_workers
    archive_media_workers = args.media_workers

    use_cluster = "cluster" in args

    start_text = datetime.fromtimestamp(after_utc).isoformat() if after_utc is not None else "the beginning of time"
    end_text = datetime.fromtimestamp(before_utc).isoformat() if before_utc is not None else "right now"

    logger.info(f"""

    |================================|================================|
    |  K  N  O  T  S  R  E  P  U  S  |  S  U  P  E  R  S  T  O  N  K  |
    |================================|================================|

    /r/superstonk archiver
                                       if you could see yourself now...

    Running {'on AWS cluster' if use_cluster else 'locally'}.
    
    Workers to archive comments: {archive_comments_workers}
    Workers to archive media:    {archive_media_workers}
    
    Archiving from {start_text} to {end_text}...
    """)

    if use_cluster:
        fs = filesystem.S3FileSystem(args.cluster[0], args.cluster[1], args.cluster[2], args.cluster[3])
        ray.init(address="auto", dashboard_host="0.0.0.0")
    else:
        fs = filesystem.LocalFileSystem(args.local)
        # Use an arbitrary but large value for the 'pushshift-ratelimit' resource to allow running in local mode on a
        # single machine.
        # Using a large number of workers will likely result in the rate limit being exceeded.
        ray.init(dashboard_host="0.0.0.0", resources={"pushshift-ratelimit": 10000})

    job = archiver.ArchiverJob(fs, archive_comments_workers, archive_media_workers, after_utc, before_utc)
    job.execute()
