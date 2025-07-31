import subprocess
from slugify import slugify
import boto3
import os
import json

account = os.getenv("ACCOUNT", "187135693246") # May Syi's account is fallback value
queue_name = os.getenv("QUEUE_NAME", "enricher-sqs-ss")
s3_id = os.getenv("S3_ID", "enricher-s3") # Second parameter is fallback value
db_id = os.getenv("DB_ID", "enricher-db")
queue_url = os.getenv("QUEUE_URL", f"https://sqs.ap-southeast-1.amazonaws.com/{account}/{queue_name}")

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)

def update_error(UploadFileName, TimeStamp, e):
    table.update_item(
        Key={
            'UploadFileName': UploadFileName,
            'TimeStamp': TimeStamp
        },
        UpdateExpression='SET ss_status = :status',
        ExpressionAttributeValues={
            ':status': str(e)
        }
    )

def main():
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )
        messages = response.get('Messages', [])
        if not messages:
            break
        for message in messages:
            receipt_handle = message['ReceiptHandle']
            new_image = json.loads(message['Body'])["MessageAttributes"]
            UploadFileName = new_image['UploadFileName']['Value']
            TimeStamp = new_image['TimeStamp']['Value']

            if 'ip_address' in new_image:
                ip_or_domain = new_image['ip_address']['Value']
            elif 'domain' in new_image:
                ip_or_domain = new_image['domain']['Value']
            else:
                print(f'No IP or domain found for {UploadFileName}---{TimeStamp}')
                continue
            
            foldername = slugify(ip_or_domain) + "_" + TimeStamp

            success = 0 # Flag to track if any ONE screenshot was successful out of the two protocols
            files = {} # List to store filenames/errors for both protocols
            for protocol in ["http", "https"]:
                print("Starting shot-scraper for ", protocol + "://" + ip_or_domain)
                filename = slugify(protocol + "://" + ip_or_domain) + "_" + TimeStamp
                query = f"shot-scraper {protocol}://{ip_or_domain} --wait 2000 --timeout 20000 -o /tmp/{filename}.png"

                response = subprocess.run(query, shell=True, capture_output=True, text=True)
                print("shot-scraper response: ", response)

                ss_location = f'{UploadFileName}/ss/{foldername}/{filename}.png'

                try:
                    s3.upload_file(f'/tmp/{filename}.png', s3_id, ss_location)
                    table.update_item(
                        Key={
                            'UploadFileName': UploadFileName,
                            'TimeStamp': TimeStamp
                        },
                        UpdateExpression="SET ss_status = :status",
                        ExpressionAttributeValues={
                            ':status': '200',
                        }
                    )
                    print(f"shot-scraper successful: {filename} uploaded")
                    files[protocol] = {
                        'file_location': f"{s3_id}/{ss_location}"
                    }
                    success = 1
                except Exception as e:
                    files[protocol] = {
                        'aws error:': str(e),
                        'shot-scraper response': str(response)
                    }
                    if success == 0:
                        update_error(UploadFileName, TimeStamp, "400")
                        continue
                    else:
                        continue
            
            table.update_item(
                Key={
                    'UploadFileName': UploadFileName,
                    'TimeStamp': TimeStamp
                },
                UpdateExpression="SET ss_file_location = :location",
                ExpressionAttributeValues={
                    ':location': json.dumps(files)
                }
            )

            if success == 0:
                update_error(UploadFileName, TimeStamp, "400")

            try:
                deletion = sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                print(f"Deleted: {receipt_handle} [{deletion}]")
            except Exception as e:
                print(f"Error deleting message: {ip_or_domain} \n {receipt_handle} - {e}")

if __name__ == "__main__":
    main()