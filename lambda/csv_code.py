import urllib.parse
import boto3
import csv
import io
import datetime
import re
import os

subnet_ids = os.environ['SUBNET_IDS'].split(',')
sg_id = os.environ['SECURITY_GROUP_ID']
db_id = os.environ['DB_ID']
ecs_cluster_id = os.environ['ECS_CLUSTER_ID']
ecs_taskdefinition_id = os.environ['ECS_TASKDEFINITION_ID']

ecs = boto3.client('ecs')
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        print(f"New file uploaded: s3://{bucket}/{key}")
    
        """ Download file from S3 and updates DynamoDB """
        obj = s3.get_object(Bucket=bucket, Key=key)
        metadata = obj['Metadata']
        body = obj['Body'].read().decode('utf-8') # Outputs a string
        reader = csv.DictReader(io.StringIO(body)) # Converts into rows for line by line input into DynamoDB
        for row in reader:
            row = {
                re.sub(r'\s+', '_', k.strip().lower()): v
                for k, v in row.items()
                if k.strip() != ''
            }
            row.update({
                'UploadFileName': key.replace("upload/", ""),
                'TimeStamp': datetime.datetime.now().strftime('%Y%m%d%H%M%S%f') # Sets sort key as time row was uploaded
            })
            row.update(metadata)

            if '\ufeffip_address' in row:
                row['ip_address'] = row.pop('\ufeffip_address')

            if 'ip_address' in row: # Next few lines of codes checks for blanks. Ignores blanks. This shouldn't be a problem unless the CSV file has blank cells.
                if row['ip_address'] == '':
                    pass
                else:
                    table.put_item(Item=row)
                    print(f"Stored to DynamoDB: {row}")
            elif 'domain' in row:
                if row['domain'] == '':
                    pass
                else:
                    table.put_item(Item=row)
                    print(f"Stored to DynamoDB: {row}")
            else:
                print(f"Missing domain/IP: {row}")
            
        if metadata['ss_status'] == "0":
            response = ecs.run_task(
                cluster=ecs_cluster_id,
                launchType='FARGATE',
                taskDefinition=ecs_taskdefinition_id,
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': subnet_ids,
                        'securityGroups': [sg_id],
                        'assignPublicIp': 'ENABLED'
                    }
                },
                enableExecuteCommand=True,
                count=10
            )  

    return {
        'statusCode': 200,
        'body': 'CSV stored in DynamoDB'
    }