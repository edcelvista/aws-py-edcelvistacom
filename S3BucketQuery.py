import boto3
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# number of max keys
BATCHLISTING = 500
JSONFILENAME = './reports/S3BucketQuery.json'
CSVFILENAME  = './reports/S3BucketQuery.csv'
DOWNLOADDIR  = './downloads/'
## User Data ##
BUCKET       = 'flyakeedbackup'
PREFIX       = 'others/amadeus-GDS-fetched-logs/'
SEARCHPREFIX = '587683'

S3Client = boto3.client('s3')

def getObjectsPrefix(client, Bucket, Prefix, marker=''):
    if(marker == ''):
        response = client.list_objects(
            Bucket=Bucket,
            MaxKeys=BATCHLISTING,
            Prefix=Prefix
        )
    else:
        response = client.list_objects(
            Bucket=Bucket,
            Marker=marker,
            MaxKeys=BATCHLISTING,
            Prefix=Prefix
        )
    return response


def listObjects(client, Bucket, Prefix, SearchPrefix):
    marker = ''
    size   = 0;
    foundObj = []
    while True:
        response = getObjectsPrefix(client, Bucket, Prefix, marker)
        if 'Contents' in response:
            for i in range(len(response['Contents'])):
                keyObj  = response['Contents'][i]['Key']
                sizeObj = response['Contents'][i]['Size']
                marker  = keyObj

                if(keyObj.find(SearchPrefix) > 0):
                    size = size + sizeObj
                    foundObj.append(keyObj)
        else:
            break;
       
    bucketDetail = dict()
    bucketDetail['bucketName'] = Bucket
    bucketDetail['size']       = convertBytesSize(size)
    bucketDetail['found']      = foundObj
    return bucketDetail


def fetchObjects(client, Bucket, ObjKey):
    print("Fetching: " + str(ObjKey))
    with open(DOWNLOADDIR+ObjKey.split("/")[-1], 'wb') as data:
        client.download_fileobj(Bucket, ObjKey, data)

response = S3Client.list_buckets()
buckets  = []
for i in range(len(response['Buckets'])):
    buckets.append(response['Buckets'][i]['Name'])

results = dict()
if BUCKET in buckets:
    res = listObjects(S3Client, BUCKET, PREFIX, SEARCHPREFIX)
    results[res['bucketName']] = res['size']
    results['Founds']          = res['found']

    if(len(results['Founds']) > 0):
        for i in range(len(results['Founds'])):
            fetchObjects(S3Client, BUCKET, results['Founds'][i])
        print("Completed! Total Size: " + results[BUCKET])
    else:
        print("Completed! No Prefix Matched!");
else:
    print("Bucket Not Found.");

writeJsonFile(JSONFILENAME, results)
writeCsvFile(CSVFILENAME, results)
