import json
import boto3
import os

topic_arn = os.environ['TOPIC_ARN']

client = boto3.client('sns')

def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            UploadFileName = record['dynamodb']['NewImage']['UploadFileName']['S']
            TimeStamp = record['dynamodb']['NewImage']['TimeStamp']['S']
            # print(record['dynamodb']['NewImage'])

            """ Convert DynamoDB image format to SNS MessageAttributes format """
            message_attributes = {}
            for key, value in record['dynamodb']['NewImage'].items():
                if 'S' in value: # String
                    if value['S'].strip():
                        message_attributes[key] = {
                            'DataType': 'String',
                            'StringValue': value['S']
                        }
                    else:
                        pass
                # elif 'M' in value: # Map # No longer relevant due to changes in DynamoDB table structure
                #     for subkey, subvalue in value['M'].items():
                #         if 'S' in subvalue: # String (Unlikely to show up, but just in case)
                #             message_attributes[subkey] = {
                #                 'DataType': 'String',
                #                 'StringValue': subvalue['S']
                #             }
                #         elif 'N' in subvalue: # Integer
                #             message_attributes[subkey] = {
                #                 'DataType': 'Number',
                #                 'StringValue': subvalue['N']
                #             }
                #         # Other data types should not show up
                elif 'N' in value: # Integer (Unlikely to show up, but just in case)
                    if value['N'].strip():
                        message_attributes[key] = {
                            'DataType': 'Number',
                            'StringValue': value['N']
                        }
                    else:
                        pass
                # Other data types should not show up
            
            # print(message_attributes)

            """ Publish message to SNS topic """
            try:
                response = client.publish(
                    TopicArn = topic_arn,
                    Message = f"New file uploaded: {UploadFileName}---{TimeStamp}",
                    MessageAttributes = message_attributes
                )
                print("Message published:", response)
            except Exception as e:
                print("Error publishing message:", e)
                continue

    return {
        'statusCode': 200,
        'body': json.dumps('Message sent to SNS')
    }
