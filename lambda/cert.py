import json
from crtsh import crtshAPI
import boto3
import time
from slugify import slugify
import os

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
        UpdateExpression='SET cert_status = :status, cert_info = :info',
        ExpressionAttributeValues={
            ':status': str(e),
            ':info': {
                'cert_log_info': log_info
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
        """ Parses records from SQS event trigger """
        sqs_payload = json.loads(record['body'])
        new_image = sqs_payload.get('MessageAttributes')
        UploadFileName = new_image['UploadFileName']['Value']
        TimeStamp = new_image['TimeStamp']['Value']

        if 'domain' in new_image:
            ip_or_domain = new_image['domain']['Value']
        elif 'ip_address' in new_image:
            ip_or_domain = new_image['ip_address']['Value']
            print(f"SSL/TLS unsuccessful (IP address input not accepted): {ip_or_domain}")
            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="SET cert_status = :val1, cert_info = :val2",
                ExpressionAttributeValues={
                    ':val1': '400',
                    ':val2': {
                        'cert_log_info': log_info,
                    }
                }
            )
            continue
        else:
            print(f"No domain found for {UploadFileName}---{TimeStamp}")
            continue
        
        filename = slugify(ip_or_domain) + '_' + TimeStamp

        for attempt in range(2):
            try:
                current_time = time.time()
                time.sleep(12) # crtshAPI can take a maximum of 5 API requests per IP address per minute.
                cert_json = crtshAPI().search(ip_or_domain)
                s3.put_object(
                    Bucket=s3_id,
                    Key=f'{UploadFileName}/cert/{filename}.json',
                    Body=json.dumps(cert_json)
                )
                if cert_json == [] or cert_json == None:
                    raise Exception("No certificate found")
            except Exception as e:
                if attempt == 1:
                    print(f"SSL/TLS unsuccessful: {ip_or_domain} \n {e}")
                    duration = int(time.time() - current_time)
                    log_info['duration'] = duration
                    if "No certificate found" in str(e):
                        update_error(UploadFileName, TimeStamp, "404", log_info)
                    else:
                        update_error(UploadFileName, TimeStamp, e, log_info)
                else:
                    print(f"SSL/TLS unsuccessful (Attempt {attempt+1} of 2)")
            else:
                SubjectCN_set = set()
                Issuer_set = set()
                SerialNo_set = set()
                AltName_set = set()
                AltName_count_min = 99999
                AltName_count_max = 0

                for i in cert_json:
                    SubjectCN_set.add(i["common_name"])
                    Issuer_set.add(i["issuer_name"])
                    SerialNo_set.add(i["serial_number"])
                    AltName_set.add(i["name_value"])

                    NumOfAltNamesInside = (i["name_value"].count("\n"))+1
                    if (NumOfAltNamesInside < AltName_count_min):
                        AltName_count_min = NumOfAltNamesInside
                    if (NumOfAltNamesInside > AltName_count_max):
                        AltName_count_max = NumOfAltNamesInside
                
                duration = int(time.time() - current_time)
                log_info['duration'] = duration
                
                table.update_item(
                    Key={
                        'UploadFileName': UploadFileName,
                        'TimeStamp': TimeStamp
                    },
                    UpdateExpression="SET cert_status = :val1, cert_info = :val2",
                    ExpressionAttributeValues={
                        ':val1': '200',
                        ':val2': {
                            'common_name': cert_json[0]['common_name'], 
                            'name_value': cert_json[0]['name_value'], 
                            'issuer_name': cert_json[0]['issuer_name'], 
                            'not_before':cert_json[0]['not_before'],
                            'not_after': cert_json[0]['not_after'],
                            "latest_cert":cert_json[0]['not_before'], 
                            'length_cert_json': len(cert_json),
                            'cert_file_location': f'{s3_id}/{UploadFileName}/cert/{filename}.json', 
                            "len(SubjectCN_set)":len(SubjectCN_set), 
                            "SubjectCN_set":list(SubjectCN_set), 
                            "len(Issuer_set)":len(Issuer_set),
                            "Issuer_set":list(Issuer_set), 
                            "len(AltName_set)":len(AltName_set), 
                            "AltName_count_min":AltName_count_min, 
                            "AltName_count_max":AltName_count_max,
                            "cert_log_info":log_info
                        }
                    }
                )
                print(f"SSL/TLS successful: {ip_or_domain}")

                break

    return {
        'statusCode': 200,
        'body': json.dumps('SSL function')
    }
