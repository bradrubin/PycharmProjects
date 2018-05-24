import os
import sys
from subprocess import check_call

import boto3

bucketName = 'org.cicsnc.albedo'
localImgDir = 'INPUT/img/'
S3ImgDir = 'Input/img/'
localLogDir = 'OUTPUT/'
S3LogDir = 'Output/'


def download(fileName):
    s3.Bucket(bucketName).download_file(S3ImgDir + fileName, localImgDir + fileName)


def upload():
    logFns = [f for f in os.listdir(localLogDir) if not f.startswith('.')]
    for logFn in logFns:
        s3.Bucket(bucketName).upload_file(localLogDir + logFn, S3LogDir + logFn)
        os.remove(localLogDir + logFn)


def process(fileName, reset):
    if reset == 'yes':
        check_call("cp /home/albedo/ancillary.src/* /home/albedo/ancillary", shell=True)
    outFile = open(localLogDir + fileName + "_Ph2.log", 'w')
    cmd = "ulimit -s unlimited && bin/GSA_Main_Linux64 -p DAM -r " + reset + " -c ancillary/AlgorithmConfigurationFile_docker -i " + fileName
    check_call(cmd, cwd="/home/albedo/", stdout=outFile, shell=True)
    os.remove(localImgDir + fileName)


def processEoD(fileName):
    outFile = open(localLogDir + fileName + "_Ph2.log", 'w')
    cmd = "ulimit -s unlimited && bin/GSA_Main_Linux64 -p DPM -c ancillary/AlgorithmConfigurationFile_docker"
    check_call(cmd, cwd='/home/albedo', stdout=outFile, shell=True)


def cleanup():
    client.delete_message(QueueUrl=queueURL, ReceiptHandle=message['ReceiptHandle'])


s3 = boto3.resource('s3')
client = boto3.client('sqs', region_name='us-east-1')
queue = client.get_queue_url(QueueName='AlbedoPh2.fifo')
queueURL = queue['QueueUrl']

while True:
    messages = client.receive_message(QueueUrl=queueURL, MaxNumberOfMessages=1, VisibilityTimeout=3600)
    if 'Messages' in messages:
        message = messages['Messages'][0]
        fileName, command = message['Body'].split(',')

        if command == 'no' or command == 'yes':
            download(fileName)
            process(fileName, command)
            cleanup()
        elif command == 'EoD':
            processEoD(fileName)
            upload()
            cleanup()
        else:
            print('Invalid command: ' + command)
            cleanup()
    else:
        print('Queue is now empty')
        sys.exit(0)
