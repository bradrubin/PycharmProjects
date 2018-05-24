bucketName = 'org.cicsnc.albedo'
basePath = 'Input/area/'
satellite = 'goes13'
year = '2017'
startDay = 1
endDay = 10
filterBand = 'BAND_01'
dryrun = False

import re
from os import fdopen, remove
from shutil import move
from tempfile import mkstemp

import boto3


def replace(file_path, pattern, subst):
    fh, abs_path = mkstemp()
    with fdopen(fh, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(re.sub(pattern, subst, line))
    remove(file_path)
    move(abs_path, file_path)


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


if satellite == 'goes08' or 'goes12' or 'goes13' or 'goes14':
    startTimeA = 0
    endTimeA = 130
    startTimeB = 730
    endTimeB = 2359
    replace('../ancillary.src/AlgorithmConfigurationFile_docker', 'GRIDID=GOES_..._VIS02', 'GRIDID=GOES_075_VIS02')
elif satellite == 'goes09' or 'goes10' or 'goes11' or 'goes15':
    startTimeA = 0
    endTimeA = 530
    startTimeB = 1130
    endTimeB = 2359
    replace('../ancillary.src/AlgorithmConfigurationFile_docker', 'GRIDID=GOES_..._VIS02', 'GRIDID=GOES_135_VIS02')
else:
    print("Invalid satellite setting")
    exit(-1)

s3 = boto3.client('s3')
s3.upload_file("../ancillary.src/AlgorithmConfigurationFile_docker", bucketName, "AlgorithmConfigurationFile_docker")

fileNames = getS3FileNames(bucketName, basePath)
fileNames = map(lambda d: removePrefix(d, basePath), fileNames)[1:]
fileNames = filter(lambda f: f.startswith(satellite), fileNames)
fileNames = filter(lambda f: f.endswith(filterBand), fileNames)
fileNames = filter(lambda f: f[7:11] == year, fileNames)
days = list("%03d" % day for day in range(startDay, endDay + 1))
fileNames = [x for x in fileNames if x[12:15] in days]
timesA = list("%04d" % time for time in range(startTimeA, endTimeA + 1))
timesB = list("%04d" % time for time in range(startTimeB, endTimeB + 1))
fileNames = [x for x in fileNames if x[16:20] in timesA + timesB]

client = boto3.client('sqs', region_name='us-east-1')
queues = client.list_queues(QueueNamePrefix='AlbedoPh1')
queueURL = queues['QueueUrls'][0]

for fileName in fileNames:
    print(fileName)
    if not dryrun:
        enqueueResponse = client.send_message(QueueUrl=queueURL, MessageBody=fileName)

print(len(fileNames))
