import boto3
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# number of max keys
BATCHLISTING = 500
JSONFILENAME = './reports/S3BucketScan.json'
CSVFILENAME  = './reports/S3BucketScan.csv'

S3Client = boto3.client('s3')

def getObjects(client, Bucket, marker=''):
    if(marker==''):
        response = client.list_objects(
            Bucket=Bucket,
            MaxKeys=BATCHLISTING,
        )
    else:
        response = client.list_objects(
            Bucket=Bucket,
            Marker=marker,
            MaxKeys=BATCHLISTING,
        )
    return response

def listObjects(client, Bucket):
    marker = ''
    size = 0;
    while True:
        response = getObjects(client, Bucket, marker)
        if 'Contents' in response:
            for i in range(len(response['Contents'])):
                keyObj = response['Contents'][i]['Key']
                sizeObj = response['Contents'][i]['Size']
                size = size + sizeObj
                marker = keyObj
        else:
            break;
       
    bucketDetail = dict()
    bucketDetail['bucketName'] = Bucket
    bucketDetail['size'] = convertBytesSize(size)
    return bucketDetail

response = S3Client.list_buckets()
results = dict()
for i in range(len(response['Buckets'])):
    res = listObjects(S3Client, response['Buckets'][i]['Name'])
    results[res['bucketName']] = res['size']

writeJsonFile(JSONFILENAME, results)
writeCsvFile(CSVFILENAME, results)
