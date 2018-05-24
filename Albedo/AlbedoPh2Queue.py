bucketName = 'org.cicsnc.albedo'
basePath = 'Input/img/'

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


fileNames = getS3FileNames(bucketName, basePath)
fileNames = map(lambda d: removePrefix(d, basePath), fileNames)[1:]
firstDay = fileNames[0][22:29]
print(firstDay)
d = {}
for k, v in groupby(fileNames, lambda s: s[22:29]):
    d[k] = list(v)
for k in d:
    if k == firstDay:
        d[k][0] = d[k][0] + ',yes'
        d[k][1:] = map(lambda n: n + ',no', d[k][1:])
    else:
        d[k] = map(lambda n: n + ',no', d[k])
    d[k].append(str(k) + ',EoD')
fileInstructions = []
for k in sorted(d):
    fileInstructions = fileInstructions + d[k]

client = boto3.client('sqs', region_name='us-east-1')
queues = client.list_queues(QueueNamePrefix='AlbedoPh2.fifo')
queueURL = queues['QueueUrls'][0]

for f in fileInstructions:
    print(f)
    enqueueResponse = client.send_message(QueueUrl=queueURL, MessageBody=f, MessageGroupId='images')

print(len(fileInstructions))
