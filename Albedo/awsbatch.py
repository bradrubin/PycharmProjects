from __future__ import print_function # Python 2/3 compatibility

import boto3
import os
import botocore
import sys
from subprocess import call

client = boto3.client('sqs', region_name='us-east-1')
queue = client.get_queue_url(QueueName='AlbedoFilenames')
queueURL = queue['QueueUrl']

messages = client.receive_message(QueueUrl=queueURL,MaxNumberOfMessages=1)
if 'Messages' in messages: # when the queue is exhausted, the response dict contains no 'Messages' key
    message = messages['Messages'][0]
    dir, fn = os.path.split(message['Body'])
    client.delete_message(QueueUrl=queueURL,ReceiptHandle=message['ReceiptHandle'])
else:
    print('Queue is now empty')
    sys.exit(0)

bucketName = 'org.cicsnc.albedo'
s3 = boto3.resource('s3')

try:
    s3.Bucket(bucketName).download_file('Input/' + fn, 'INPUT/area/' + fn)
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise

outFile = open("OUTPUT/" + fn + "_ImageAcceptor.log" ,"wb")
call(["bin/GSA_GOES_ImageAcceptor_main", "INPUT/area/" + fn, "-c", "ancillary/AlgorithmConfigurationFile_docker", "-v"], stdout=outFile)

outFn = os.listdir("INPUT/img/")[0]
data = open("INPUT/img/" + outFn, 'rb')
key = "Input/img/" + outFn
s3.Bucket(bucketName).put_object(Key=key, Body=data)

logFn = os.listdir("OUTPUT")[0]
data = open("OUTPUT/" + logFn, 'rb')
key = "Output/" + logFn
s3.Bucket(bucketName).put_object(Key=key, Body=data)