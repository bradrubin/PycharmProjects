import os
import sys
from subprocess import check_call

import boto3

bucketName = 'org.cicsnc.albedo'
localLogDir = 'OUTPUT/'
S3LogDir = 'Output/'

def download(fileName):
    s3.Bucket(bucketName).download_file(S3LogDir + fileName, localLogDir + fileName)


def upload(fileName):
    s3.Bucket(bucketName).upload_file(localLogDir + fileName + '.nc', S3LogDir + fileName + '.nc')
    os.remove(localLogDir + fileName + '.nc')


    logFn = fileName + '_Ph3.log'
    s3.Bucket(bucketName).upload_file(localLogDir + logFn, S3LogDir + logFn)
    os.remove(localLogDir + logFn)

def process(fileName):
    outFile = open(localLogDir + fileName + "_Ph3.log", 'w')
    check_call(["bin/GSA_Native2NetCDF4_Converter_Linux64", fileName, localLogDir, localLogDir, '-v'], stdout=outFile)

def cleanup(fileName):
    os.remove(localLogDir + fileName)
    client.delete_message(QueueUrl=queueURL, ReceiptHandle=message['ReceiptHandle'])


s3 = boto3.resource('s3')
client = boto3.client('sqs', region_name='us-east-1')
queue = client.get_queue_url(QueueName='AlbedoPh3')
queueURL = queue['QueueUrl']

while True:
    messages = client.receive_message(QueueUrl=queueURL, MaxNumberOfMessages=1, VisibilityTimeout=300,
                                      WaitTimeSeconds=5)
    if 'Messages' in messages:
        message = messages['Messages'][0]
        dir, fn = os.path.split(message['Body'])

        download(fn)
        process(fn)
        upload(fn)
        cleanup(fn)
    else:
        print('Queue is now empty')
        sys.exit(0)
