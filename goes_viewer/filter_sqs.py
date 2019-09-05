"""
Simple function to filter all SQS messages from noaa-goes notifications
for the given file prefix.
"""
import json
import boto3
import os

S3_PREFIX = os.getenv('S3_PREFIX', 'ABI-L2-MCMIPF')
SQS_URL = os.getenv('SQS_URL', '')


def lambda_handler(event, context):
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(SQS_URL)
    entries = []
    i = 1
    for erec in event['Records']:
        sns_msg = json.loads(erec['body'])
        rec = json.loads(sns_msg['Message'])
        for record in rec['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            if key.startswith(S3_PREFIX):
                body = f'{bucket}:{key}'
                print(f'sending to queue {body}')
                entries.append({'Id': str(i), 'MessageBody': body})
                i += 1
    if entries:
        resp = queue.send_messages(Entries=entries)
    else:
        resp = "None"
    return {
        'statusCode': 200,
        'body': json.dumps(resp)
    }
