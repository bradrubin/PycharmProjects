basePath = '/snfs7/goes/'
satellite = 'goes13'
year = 2017
startDay = 1
endDay = 10
filterBand = 'BAND_01'
S3InputDir = 'Input/area/'

import os

import boto3


def removePrefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]


dirNames = (basePath + satellite + '/' + str(year) + '/' + "%03d" % day for day in range(startDay, endDay + 1))

fullPath = lambda path: [os.path.join(path, fn) for fn in next(os.walk(path))[2]]
fileNames = map(lambda d: fullPath(d), dirNames)
fileNames = [item for sublist in fileNames for item in sublist]
fileNames = list(filter(lambda f: f.endswith(filterBand), fileNames))

bucketName = 'org.cicsnc.albedo'
s3 = boto3.resource('s3')

for fullName in fileNames:
    fileName = os.path.basename(fullName)
    print(fileName)
    s3.Bucket(bucketName).upload_file(fullName, S3InputDir + fileName)

print(len(fileNames))
