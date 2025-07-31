import json
from waybackpy import WaybackMachineCDXServerAPI
import boto3
import time
from slugify import slugify
import os
from requests.adapters import HTTPAdapter
from requests import Session

s3_id = os.environ['S3_ID']
db_id = os.environ['DB_ID']

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)

user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"

def update_error(UploadFileName, TimeStamp, e, log_info):
    table.update_item(
        Key={
            'UploadFileName': UploadFileName,
            'TimeStamp': TimeStamp
        },
        UpdateExpression='SET hist_status = :status, archived_page_info = :info',
        ExpressionAttributeValues={
            ':status': str(e),
            ':info': {
                'hist_log_info': log_info
            }
        }
    )

def lambda_handler(event, context):
    log_info = {
        'log_stream_name': context.log_stream_name,
        'log_group_name': context.log_group_name,
        'aws_request_id': context.aws_request_id
    }
    for record in event['Records']:
        sqs_payload = json.loads(record['body'])
        new_image = sqs_payload.get('MessageAttributes')
        UploadFileName = new_image['UploadFileName']['Value']
        TimeStamp = new_image['TimeStamp']['Value']
        if 'ip_address' in new_image:
            ip_or_domain = new_image['ip_address']['Value']
            print(f"HIST unsuccessful (IP address input not accepted): {ip_or_domain}")
            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="SET hist_status = :val1, hist_info = :val2",
                ExpressionAttributeValues={
                    ':val1': '400',
                    ':val2': {
                        'hist_log_info': log_info,
                    }
                }
            )
            continue
        elif 'domain' in new_image:
            ip_or_domain = new_image['domain']['Value']
        else:
            print("HIST unsuccessful (No IP or domain found)")
            continue
        
        filename = slugify(ip_or_domain) + '_' + TimeStamp

        print(f"Starting HIST processing for {ip_or_domain}")
        start_time = time.time()
        try:
            cdx_api = WaybackMachineCDXServerAPI(ip_or_domain, user_agent) # If fails, automatically retries another 4 times.
            newest = cdx_api.newest()
            body = json.dumps(newest.__dict__, indent=4, sort_keys=True, default=str)

            hist_location = f"{UploadFileName}/hist/{filename}.json"

            duration = int(time.time() - start_time)
            log_info['duration'] = duration

            s3.put_object(
                Body=body,
                Bucket=s3_id,
                Key=hist_location,
                ContentType='application/json'
            )

            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="set hist_status = :val1, archived_page_info = :val2",
                ExpressionAttributeValues={
                    ':val1': '200',
                    ':val2': {
                        'archive_url': newest.archive_url,
                        'timestamp': newest.datetime_timestamp.strftime("%d-%m-%YT%H:%M:%S"),
                        'archived_page_file_location': f"{s3_id}/{hist_location}",
                        'hist_log_info': log_info
                    }
                }
            )
            print(f"HIST successful: {ip_or_domain}")
            continue
        except Exception as e:
            print(f"HIST unsuccessful: {ip_or_domain} \n {e}")
            duration = int(time.time() - start_time)
            log_info['duration'] = duration
            if "Connection to web.archive.org timed out. (connect timeout=None)" in str(e):
                update_error(UploadFileName, TimeStamp, "443", log_info)
            elif "Wayback Machine's CDX server did not return any records for the query." in str(e):
                update_error(UploadFileName, TimeStamp, "404", log_info)
            elif "[Errno 111] Connection refused" in str(e):
                update_error(UploadFileName, TimeStamp, "111", log_info)
            else:
                update_error(UploadFileName, TimeStamp, e, log_info)

    return {
        'statusCode': 200,
        'body': json.dumps('HIST')
    }
