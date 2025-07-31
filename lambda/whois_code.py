import json
import urllib
import boto3
import whois
import ipwhois
from datetime import datetime
from slugify import slugify
import os
import time

db_id = os.environ["DB_ID"]
s3_id = os.environ['S3_ID']

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)

def serialize_datetimes(w): # Recursive function to serialise w
    if isinstance(w, dict):
        return {k: serialize_datetimes(v) for k, v in w.items()}
    elif isinstance(w, list):
        return [serialize_datetimes(i) for i in w]
    elif isinstance(w, set):
        return [serialize_datetimes(i) for i in w]  # Converts set to list
    elif isinstance(w, datetime):
        return w.isoformat()
    else:
        return w

def update_error(UploadFileName, TimeStamp, e, log_info):
    table.update_item(
        Key={
            'UploadFileName': UploadFileName,
            'TimeStamp': TimeStamp
        },
        UpdateExpression='SET whois_status = :status, whois_info = :info',
        ExpressionAttributeValues={
            ':status': str(e),
            ':info': {
                "whois_log_info": log_info
            }
        }
    )

def lambda_handler(event, context):
    log_info = {
        'log_stream_name': context.log_stream_name,
        'log_group_name': context.log_group_name,
        'aws_request_id': context.aws_request_id
    }
    for record in event['Records']: # If there are five messages in one batch, there will be five records.
        """ Parses records from SQS event trigger """
        sqs_payload = json.loads(record['body'])
        new_image = sqs_payload.get('MessageAttributes')
        UploadFileName = new_image['UploadFileName']['Value']
        TimeStamp = new_image['TimeStamp']['Value']
        
        if 'ip_address' in new_image:
            ip_or_domain = new_image['ip_address']['Value']
        elif 'domain' in new_image:
            ip_or_domain = new_image['domain']['Value']
        else:
            print(f"No IP or domain found for {UploadFileName}---{TimeStamp}")
            continue

        txt_filename = slugify(ip_or_domain) + '_' + TimeStamp
        
        print(f"Starting WHOIS for {ip_or_domain}")
        current_time = time.time()
        if 'domain' in new_image:
            ip_or_domain = new_image['domain']['Value']
            try:
                w = whois.whois(ip_or_domain)
                if w is None:
                    raise Exception("499")
                elif w.domain_name is None:
                    raise Exception("400")
                else:
                    body=serialize_datetimes(w) # Need to make dates JSON compatible for json.dumps() later
            except Exception as e:
                print(f"WHOIS failed for {ip_or_domain} - {UploadFileName}---{TimeStamp} \n {e}")
                log_info['duration'] = int(time.time() - current_time)
                if "No match for" in str(e):
                    update_error(UploadFileName, TimeStamp, "400", log_info)
                else:
                    update_error(UploadFileName, TimeStamp, e, log_info)
                continue
        elif 'ip_address' in new_image:
            ip_or_domain = new_image['ip_address']['Value']
            try:
                w = ipwhois.IPWhois(ip_or_domain).lookup_rdap() 
                if w is None:
                    raise Exception("499")
                elif w['network'] is None:
                    raise Exception("400")
                else:
                    body=serialize_datetimes(w) # Need to make dates JSON compatible for json.dumps() later
            except Exception as e:
                print(f"WHOIS failed for {ip_or_domain} - {UploadFileName}---{TimeStamp} \n {e}")
                log_info['duration'] = int(time.time() - current_time)
                # if w is None: # In case its not caught in if statement nested in try (because exception was raised first)
                #     update_error(UploadFileName, TimeStamp, "499", log_info)
                update_error(UploadFileName, TimeStamp, e, log_info)
                continue
        else:
            print(f"No IP or domain found for {UploadFileName}---{TimeStamp}")
            continue

        s3.put_object(
            Bucket=s3_id,
            Key=f'{UploadFileName}/whois/{txt_filename}.json',
            Body=json.dumps(body),
            ContentType='application/json'
        )

        if 'domain' in new_image:
            log_info['duration'] = int(time.time() - current_time)
            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="SET whois_status = :status, whois_info = :info",
                ExpressionAttributeValues={
                    ':status': '200',
                    ':info': {
                        'registrar': body.get('registrar', None),
                        'name': body.get('name', None),
                        'org': body.get('org', None),
                        'creation_date': body.get('creation_date', None),
                        'updated_date': body.get('updated_date', None),
                        'whois_file_location': f'{s3_id}/{UploadFileName}/whois/{txt_filename}.json',
                        'whois_log_info': log_info
                    }
                }
            )
        elif 'ip_address' in new_image:
            log_info['duration'] = int(time.time() - current_time)
            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="SET whois_status = :status, whois_info = :info",
                ExpressionAttributeValues={
                    ':status': '200',
                    ':info': {
                        'asn_registry': body.get('asn_registry', None),
                        'asn': body.get('asn', None),
                        'asn_cidr': body.get('asn_cidr', None),
                        'asn_country_code': body.get('asn_country_code', None),
                        'asn_date': body.get('asn_date', None),
                        'asn_description': body.get('asn_description', None),
                        'whois_file_location': f'{s3_id}/{UploadFileName}/whois/{txt_filename}.json',
                        'whois_log_info': log_info
                    }
                }
            )

        print(f"WHOIS successful for {ip_or_domain} - {UploadFileName}---{TimeStamp}")

    return {
        'statusCode': 200,
        'body': json.dumps("WHOIS SUCCESSFUL")
    }

