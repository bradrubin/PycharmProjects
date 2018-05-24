bucketName = 'org.cicsnc.albedo'
basePath = 'Output/'

from itertools import groupby

import boto3

s3 = boto3.client('s3')


def removePrefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]


def getS3FileNames(bucket, folder):
    keys = []
    kwargs = {'Bucket': bucket, 'Prefix': folder}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return keys


fileNames = getS3FileNames(bucketName, basePath + 'GSA_')
fileNames = filter(lambda f: '.' not in f, fileNames)
fileNames = map(lambda d: removePrefix(d, basePath), fileNames)

client = boto3.client('sqs', region_name='us-east-1')
queues = client.list_queues(QueueNamePrefix='AlbedoPh3')
queueURL = queues['QueueUrls'][0]

for f in fileNames:
    print(f)
    enqueueResponse = client.send_message(QueueUrl=queueURL, MessageBody=f)

print(len(list(fileNames)))
