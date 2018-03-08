from __future__ import print_function # Python 2/3 compatibility

basePath='/snfs7/goes/'
satellite='goes13'
year=2017
startDay=1
endDay=1
filterBand='BAND_01'

import os

def removePrefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]

dirNames = (basePath + satellite + '/' + str(year) + '/' + "%03d" % day for day in range(startDay, endDay + 1))

fullPath = lambda path: [os.path.join(path,fn) for fn in next(os.walk(path))[2]]
fileNames = map(lambda d: fullPath(d), dirNames)
fileNames = [item for sublist in fileNames for item in sublist]
fileNames = map(lambda d: removePrefix(d, basePath), fileNames)
fileNames = filter(lambda f: f.endswith(filterBand), fileNames)

print("\n".join(fileNames))

import boto3
import json
import decimal

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def putFileName(fn, status):
    response = table.put_item(
        Item={
            'FileName': fn,
            'Status': status,
        }
    )

    print("PutItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))

#dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
#table = dynamodb.Table('AlbedoFileNames')
#map(lambda fn: putFileName(fn, 0), fileNames)

client = boto3.client('sqs', region_name='us-east-1')
client.create_queue(QueueName='AlbedoFilenames')
queues = client.list_queues(QueueNamePrefix='AlbedoFilenames')
testQueueURL = queues['QueueUrls'][0]

for f in fileNames:
    enqueueResponse = client.send_message(QueueUrl=testQueueURL, MessageBody=f)
    print('Message ID : ',enqueueResponse['MessageId'])