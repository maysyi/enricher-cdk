import json
import urllib.request
import boto3
import time
import datetime
from slugify import slugify
import os

lambda_vt_quota_id = "XXXXX" # NEED TO UPDATE
s3_id = os.environ['S3_ID']
db_id = os.environ['DB_ID']
API_KEY = os.environ['API_KEY']
queue_url = os.environ['QUEUE_URL']
topic_arn = os.environ['TOPIC_ARN']

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)
sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')
sns = boto3.client('sns')

def lambda_handler(event, context):
    log_info = {
        'log_stream_name': context.log_stream_name,
        'log_group_name': context.log_group_name,
        'aws_request_id': context.aws_request_id
    }
    quota_flag = 0
    for record in event['Records']:
        """ Parses records from SQS event trigger """
        sqs_payload = json.loads(record['body'])
        new_image = sqs_payload.get('MessageAttributes')
        UploadFileName = new_image['UploadFileName']['Value']
        TimeStamp = new_image['TimeStamp']['Value']

        if quota_flag == 1: # To make sure rest of the batch is not just discarded.
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(sqs_payload),
                DelaySeconds=360 # Resent message is not visible for 6 minutes (at least until we are certain all Lambda invocations are stopped)
            )
            print(f"Skipping for later: {ip_or_domain}")
            continue # Skips rest of the processing code

        if 'ip_address' in new_image:
            vt_link = "https://www.virustotal.com/api/v3/ip_addresses/"
            ip_or_domain = new_image['ip_address']['Value']
        elif 'domain' in new_image:
            vt_link = "https://www.virustotal.com/api/v3/domains/"
            ip_or_domain = new_image['domain']['Value']
        else:
            print(f"No IP or domain found for {UploadFileName}---{TimeStamp}")
            continue

        print(f"Starting VT: {ip_or_domain}")
        json_filename = slugify(ip_or_domain+ '_' + TimeStamp)
        combined_link = vt_link + ip_or_domain
        current_time = time.time()

        for attempt in range(3):
            try:
                time.sleep(1)
                req = urllib.request.Request(url=combined_link, headers={"x-apikey":API_KEY})
                r = urllib.request.urlopen(req)
                status_code = r.getcode()
                if status_code == 200:
                    body = r.read().decode('utf-8')
                    s3.put_object(
                        Bucket=s3_id,
                        Key=f'{UploadFileName}/vt/{json_filename}.json',
                        Body=body,
                        ContentType='application/json'
                    )
                    log_info['duration'] = int(time.time() - current_time)
                    table.update_item(
                        Key={
                            'UploadFileName':  UploadFileName,
                            'TimeStamp': TimeStamp
                        },
                        UpdateExpression='SET vt_status = :val1, vt_info = :val2',
                        ExpressionAttributeValues={
                            ':val1': '200',
                            ':val2': {
                                'vt_file_location': f'{s3_id}/{UploadFileName}/vt/{json_filename}.json',
                                'vt_log_info': log_info
                            }
                        }
                    )
                    print(f"VT successful: {ip_or_domain}")
                    break
                else:
                    raise Exception
            except Exception as e:
                if isinstance(e, urllib.error.HTTPError):
                    status_code = e.code
                else:
                    status_code = 0

                if status_code == 429:
                    try:
                        source_response = lambda_client.list_event_source_mappings(
                            FunctionName=context.function_name
                        )
                        if source_response['EventSourceMappings'][0]['State'] != "Enabled": # Check if another invocation is disabling event source mapping
                            print("Other invocation disabling event source mapping")
                            sqs.send_message( # Send back current message for reprocessing later
                                QueueUrl=queue_url,
                                MessageBody=json.dumps(sqs_payload),
                                DelaySeconds=360
                            )
                            break # Break out of retries loop
                        else: 
                            lambda_response = lambda_client.invoke(
                                FunctionName=lambda_vt_quota_id,
                                InvocationType='Event',
                                Payload=json.dumps('STOP')
                            )
                            time.sleep(30) # Wait for event source mapping to be disabled
                    except Exception as e:
                        print("Failed to disable event source mapping")
                        sns_response = sns.publish(
                            TopicArn=topic_arn,
                            Message=json.dumps({
                                "Body": "WARNING: VT quota exceeded and failed to disable Lambda function" # Received by email
                            })
                        )
                        print(f"Notification sent \n {sns_response}") # In emergency where event source mapping needs to be disabled manually. This should NOT happen.
                        return sns_response # End lambda_handler here.
                    else:
                        print(f"Disabled event source mapping \n {lambda_response}")
                        sqs.send_message( # Send back current message for reprocessing later
                            QueueUrl=queue_url,
                            MessageBody=json.dumps(sqs_payload),
                            DelaySeconds=360
                        )
                        break # Break out of retries loop
                elif status_code == 404:
                    print(f"VT unsuccessful (404 Not Found): {ip_or_domain}")
                    log_info['duration'] = int(time.time() - current_time)
                    table.update_item(
                        Key={
                            'UploadFileName':  UploadFileName,
                            'TimeStamp': TimeStamp
                        },
                        UpdateExpression='SET vt_status = :val1, vt_info = :val2',
                        ExpressionAttributeValues={
                            ':val1': str(status_code),
                            ':val2': {
                                'vt_file_location': 'N/A',
                                'vt_log_info': log_info
                            }
                        }
                    )
                    break
                else: # For other error codes
                    if attempt == 2:
                        print(f"VT unsuccessful: {ip_or_domain} \n {e}")
                        log_info['duration'] = int(time.time() - current_time)
                        table.update_item(
                            Key={
                                'UploadFileName':  UploadFileName,
                                'TimeStamp': TimeStamp
                            },
                            UpdateExpression='SET vt_status = :val1, vt_info = :val2',
                            ExpressionAttributeValues={
                                ':val1': str(e),
                                ':val2': {
                                    'vt_file_location': 'N/A',
                                    'vt_log_info': log_info
                                }
                            }
                        )
                        break
                    else:
                        print(f"VT unsuccessful (Attempt {attempt+1} of 3)")
    return 0