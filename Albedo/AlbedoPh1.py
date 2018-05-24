import os
import sys
from subprocess import check_call

import boto3

bucketName = 'org.cicsnc.albedo'
localAreaDir = 'INPUT/area/'
S3AreaDir = 'Input/area/'
localImgDir = 'INPUT/img/'
S3ImgDir = 'Input/img/'
localLogDir = 'OUTPUT/'
S3LogDir = 'Output/'


def areaToImg(fileName):
    satellite = fileName[4:6]
    lon = ''
    if satellite == '08' or '12' or '13' or '14':
        lon = '075'
    elif satellite == '09' or '10' or '11' or '15':
        lon = '135'
    else:
        print("Invalid satellite setting")
        exit(-1)
    year = fileName[7:11]
    day = fileName[12:15]
    time = fileName[16:21]
    if int(time[4:5]) > 2:
        time = time[0:3] + str(int(time[3:4]) + 1)
    else:
        time = time[0:4]
    img = 'IMG_' + satellite + '_' + 'GOES_' + lon + '_VIS02_' + year + day + '_' + time + '.img'
    return img


def download(fileName):
    s3.Bucket(bucketName).download_file(S3AreaDir + fileName, localAreaDir + fileName)


def upload(fileName):
    imgFn = areaToImg(fileName)
    try:
        s3.Bucket(bucketName).upload_file(localImgDir + imgFn, S3ImgDir + imgFn)
        os.remove(localImgDir + imgFn)
    except:
        print('Missing: ' + imgFn)
        pass


    logFn = fileName + '_Ph1.log'
    try:
        s3.Bucket(bucketName).upload_file(localLogDir + logFn, S3LogDir + logFn)
        os.remove(localLogDir + logFn)
    except:
        print('Missing: ' + logFn)
        pass


def process(fileName):
    outFile = open(localLogDir + fileName + "_Ph1.log", 'w')
    check_call(["bin/GSA_GOES_ImageAcceptor_main", localAreaDir + fileName, "-c",
                "ancillary/AlgorithmConfigurationFile_docker", "-v"], stdout=outFile)


def cleanup(fileName):
    os.remove(localAreaDir + fileName)
    client.delete_message(QueueUrl=queueURL, ReceiptHandle=message['ReceiptHandle'])


s3 = boto3.resource('s3')
client = boto3.client('sqs', region_name='us-east-1')
queue = client.get_queue_url(QueueName='AlbedoPh1')
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
