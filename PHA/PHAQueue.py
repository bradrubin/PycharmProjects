bucketName = 'org.cicsnc.pha'
S3ConfigDir = 'configuration/'
dryrun = False

import boto3

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

s3 = boto3.client('s3')

fileNames = getS3FileNames(bucketName, S3ConfigDir)
fileNames = map(lambda d: removePrefix(d, S3ConfigDir), fileNames)[1:]

client = boto3.client('sqs', region_name='us-east-1')
queues = client.list_queues(QueueNamePrefix='PHA')
queueURL = queues['QueueUrls'][0]

for fileName in fileNames:
    print(fileName)
    if not dryrun:
        enqueueResponse = client.send_message(QueueUrl=queueURL, MessageBody=fileName)

print(len(list(fileNames)))
