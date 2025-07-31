import json
import socket
import boto3
import dns.resolver
from slugify import slugify
import os
import time

s3_id = os.environ['S3_ID']
db_id = os.environ['DB_ID']

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)

def update_error(UploadFileName, TimeStamp, e, log_info):
    table.update_item(
        Key={
            'UploadFileName': UploadFileName,
            'TimeStamp': TimeStamp
        },
        UpdateExpression='SET dns_status = :status, dns_info = :info',
        ExpressionAttributeValues={
            ':status': str(e),
            ':info': {
                'dns_log_info': log_info
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
        if 'domain' in new_image:
            ip_or_domain = new_image['domain']['Value']
        elif 'ip_address' in new_image:
            ip_or_domain = new_image['ip_address']['Value']
            print(f"DNS unsuccessful (IP address input not accepted): {ip_or_domain}")
            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="SET dns_status = :val1, dns_info = :val2",
                ExpressionAttributeValues={
                    ':val1': '400',
                    ':val2': {
                        'dns_log_info': log_info,
                    }
                }
            )
            continue
        else:
            print(f"No domain found for {UploadFileName}---{TimeStamp}")
            continue
        filename = slugify(ip_or_domain) + '_' + TimeStamp

        print(f"Starting DNS resolution: {ip_or_domain}")
        current_time = time.time()
        for attempt in range(3):
            try:
                """ DNS resolution """
                dns_info = socket.gethostbyname_ex(ip_or_domain)
                alias = dns_info[1]
                other_ip_address = dns_info[2]
                try:
                    ipv6_info = dns.resolver.resolve(ip_or_domain, 'AAAA')
                    for i in ipv6_info:
                        other_ip_address.append(i.to_text())
                except Exception:
                    pass
                dns_dict = {
                    'hostname': dns_info[0],
                    'alias': alias,
                    'other_ip_address': other_ip_address
                }

                for tries in range(3):
                    try:
                        r = dns.resolver.Resolver()
                        r.nameservers = ['1.1.1.1']
                        nameservers = r.resolve(ip_or_domain, 'NS')
                        nameserver_list = sorted([i.to_text() for i in nameservers])
                        dns_dict['nameservers'] = nameserver_list
                        print(f"Name servers successful: {ip_or_domain}---{TimeStamp}")
                        break
                    except Exception as e:
                        if tries == 2:
                            print(f"Name servers unsuccessful: {ip_or_domain}---{TimeStamp} \n {e}")
                            break
                        else: 
                            print(f"Name servers unsuccessful (Attempt {tries+1} of 3)")
                            continue

                s3.put_object(
                    Bucket=s3_id,
                    Key=f'{UploadFileName}/dns/{filename}.txt',
                    Body=json.dumps(dns_dict),
                    ContentType='text/html'
                )
                log_info['duration'] = int(time.time() - current_time)
                table.update_item(
                    Key={
                        'UploadFileName': UploadFileName,
                        'TimeStamp': TimeStamp
                    },
                    UpdateExpression="SET dns_status = :val1, dns_info = :val2",
                    ExpressionAttributeValues={
                        ':val1': '200',
                        ':val2': {
                            'alias': alias,
                            'other_ip_address': other_ip_address,
                            'dns_file_location': f'{s3_id}/{UploadFileName}/dns/{filename}.txt',
                            'dns_log_info': log_info
                        }
                    }
                )
                print(f"DNS successful: {ip_or_domain}---{TimeStamp}")
                break
            except Exception as e:
                if attempt == 2:
                    print(f"DNS unsuccessful: {ip_or_domain}---{TimeStamp} \n {e}")
                    log_info['duration'] = int(time.time() - current_time)
                    if "[Errno -2]" in str(e):
                        update_error(UploadFileName, TimeStamp, "2", log_info)
                    elif "[Errno -5]" in str(e):
                        update_error(UploadFileName, TimeStamp, "5", log_info)
                    else:
                        update_error(UploadFileName, TimeStamp, e, log_info)
                    break
                else: 
                    print(f"DNS unsuccessful (Attempt {attempt+1} of 3)")
                    continue
    return {
        'statusCode': 200,
        'body': json.dumps('DNS successful')
    }
