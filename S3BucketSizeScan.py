import boto3, argparse
from os import path
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# Generic Attributes
BATCHLISTING = 500
REPORTSDIR   = './reports'
JSONFILENAME = '{}/S3BucketSizeScan.json'.format(REPORTSDIR)

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract bucket size in specified bucket name and prefix")
parser.add_argument("-b",
                    "--bucket",
                    help="Target Bucket Name",
                    required=True)
parser.add_argument("-p",
                    "--prefix",
                    help="Prefix or directories",
                    required=False)
parser.add_argument("-s",
                    "--awsprofile",
                    help="AWS Profile.",
                    required=False)

# Read the arguments from the command line
args         = parser.parse_args()
bucket       = args.bucket
prefix       = "" if args.prefix == None else args.prefix
awsprofile   = "default" if args.awsprofile == None else args.awsprofile

# User Attributes
BUCKET = bucket #'<bucket>'  # 'bucketname'
PREFIX = prefix #'<prefix | dir>'  # 'dir1/dir2/'

# Session Creation
session  = boto3.Session(profile_name="{}".format(awsprofile))
S3Client = session.client('s3')

def contextSettings(args):
    print(args)

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

# Main
contextSettings(args)
if not path.exists(REPORTSDIR):
    print("Reports Directory not exist... Creating one...")
    os.makedirs(REPORTSDIR)

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