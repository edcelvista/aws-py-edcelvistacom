import boto3
import argparse
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# number of max keys
BATCHLISTING = 500
JSONFILENAME = './reports/S3BucketSizeScan.json'

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract Bucket Size Details")
parser.add_argument("-b",
                    "--bucket",
                    help="Target Bucket Name",
                    required=True)
parser.add_argument("-p",
                    "--prefix",
                    help="Prefix or directories",
                    required=False)

# Read the arguments from the command line
args = parser.parse_args()
bucket = args.bucket
prefix = "" if args.prefix == None else args.prefix

## User Data ##
BUCKET = bucket #'<bucket>'  # 'bucketname'
PREFIX = prefix #'<prefix | dir>'  # 'dir1/dir2/'

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


def listObjects(client, Bucket, Prefix):
    marker = ''
    size   = 0;
    while True:
        response = getObjectsPrefix(client, Bucket, Prefix, marker)
        if 'Contents' in response:
            for i in range(len(response['Contents'])):
                keyObj  = response['Contents'][i]['Key']
                sizeObj = response['Contents'][i]['Size']
                marker  = keyObj
                size    = size + sizeObj
        else:
            break;
       
    bucketDetail = dict()
    bucketDetail['bucketName'] = Bucket
    bucketDetail['size']       = convertBytesSize(size)
    bucketDetail['prefix']     = Prefix
    return bucketDetail


response = S3Client.list_buckets()
buckets  = []
for i in range(len(response['Buckets'])):
    buckets.append(response['Buckets'][i]['Name'])

results = dict()
if BUCKET in buckets:
    res = listObjects(S3Client, BUCKET, PREFIX)
    results[res['bucketName']] = res['size']
    results['prefix']          = res['prefix']
    writeJsonFile(JSONFILENAME, results);
    print(results)
else:
    print("Bucket Not Found.");
