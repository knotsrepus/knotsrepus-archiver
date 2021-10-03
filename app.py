#!/usr/bin/env python3
import os

from aws_cdk import core

from knotsrepus_archiver.knotsrepus_archiver_stack import KnotsrepusArchiverStack


app = core.App()
KnotsrepusArchiverStack(
    app,
    "KnotsrepusArchiver",
    env=core.Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"]
    )
)

app.synth()
