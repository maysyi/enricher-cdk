import boto3
import os

lambda_client = boto3.client('lambda')
uuid = "XXXXXX" # NEED TO UPDATE

def lambda_handler(event, source):
    if "eventbridge" in event:
        trigger = "eventbridge"
        response = lambda_client.update_event_source_mapping(
            UUID=uuid,
            Enabled=True
        )
        print("VT function enabled")
    else:
        trigger = "lambda"
        response = lambda_client.update_event_source_mapping(
            UUID=uuid,
            Enabled=False
        )
        print("VT function disabled")
    print(response)
    return {
        'statusCode': 200
    }
