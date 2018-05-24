import os
import shutil
import sys
from subprocess import check_call

import boto3

date = '20180410'
bucketName = 'org.cicsnc.pha'
S3ConfigDir = 'configuration/'
localConfigPath = '/home/pha/v4_pha/src/GHCNMv4r/scripts/ghcnmv4_ops/combination0.txt'
S3PostDir = 'post/'
localPostDir = '/home/pha/v4_pha/GHCNMv4r/data/' + date + '.ALL/ghcnmv4/post/'
S3LogDir = 'log/'
localLogDir = '/home/pha/v4_pha/GHCNMv4r/data/' + date + '.ALL/ghcnmv4/log/'


def uploadDirectory(srcPath, dstPath, prefix):
    for root, dirs, files in os.walk(srcPath):
        for fn in files:
            s3.Bucket(bucketName).upload_file(srcPath + fn, dstPath + prefix + '/' + fn)


def download(fileName):
    s3.Bucket(bucketName).download_file(S3ConfigDir + fileName, localConfigPath)
    client.delete_message(QueueUrl=queueURL, ReceiptHandle=message['ReceiptHandle'])


def upload(fileName):
    uploadDirectory(localPostDir, S3PostDir, fileName)
    uploadDirectory(localLogDir, S3LogDir, fileName)
    s3.Bucket(bucketName).upload_file('./run_pha.log', S3LogDir + fileName + '/' + 'run_pha.log')
    s3.Bucket(bucketName).upload_file('/home/pha/log_PHA_' + date, S3LogDir + fileName + '/' + 'main_log')


def process():
    outFile = open('./run_pha.log', 'w')
    check_call(['./run_pha.sh'], stdout=outFile)


def cleanup():
    folder = '/home/pha/v4_pha/GHCNMv4r_9999/data/' + date + '.ALL'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


s3 = boto3.resource('s3')
client = boto3.client('sqs', region_name='us-east-1')
queue = client.get_queue_url(QueueName='PHA')
queueURL = queue['QueueUrl']

while True:
    messages = client.receive_message(QueueUrl=queueURL, MaxNumberOfMessages=1, VisibilityTimeout=60,
                                      WaitTimeSeconds=5)
    if 'Messages' in messages:
        message = messages['Messages'][0]
        fn = message['Body']

        download(fn)
        process()
        upload(fn)
        cleanup()
    else:
        print('Queue is now empty')
        sys.exit(0)
