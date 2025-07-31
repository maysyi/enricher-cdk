#!/usr/bin/env python3
import aws_cdk as cdk

from enricher_cdk.enricher_cdk_stack import EnricherCdkStack

# User information (NEED TO UPDATE)
region_id = 'XXXXX'
account_id = 'XXXXX'

app = cdk.App()

parent_stack = EnricherCdkStack(app, "EnricherCdkStack",
    env=cdk.Environment(account=account_id, region=region_id)
)

app.synth()
