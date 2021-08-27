#!/usr/bin/env python3

from aws_cdk import core

from knotsrepus_archiver.knotsrepus_archiver_stack import KnotsrepusArchiverStack


app = core.App()
KnotsrepusArchiverStack(app, "KnotsrepusArchiver")

app.synth()
