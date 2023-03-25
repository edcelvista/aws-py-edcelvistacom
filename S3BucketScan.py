import boto3, argparse
from os import path
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# Generic Attributes
BATCHLISTING = 500
REPORTSDIR   = './reports'
JSONFILENAME = '{}/S3BucketScan.json'.format(REPORTSDIR)

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract bucket usage details based on the aws profile used")
parser.add_argument("-s",
                    "--awsprofile",
                    help="AWS Profile.",
                    required=False)

# Read the arguments from the command line
args         = parser.parse_args()
awsprofile   = "default" if args.awsprofile == None else args.awsprofile

# Session Creation
session  = boto3.Session(profile_name="{}".format(awsprofile))
S3Client = session.client('s3')

def contextSettings(args):
    print(args)

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

# Main
contextSettings(args)
if not path.exists(REPORTSDIR):
    print("Reports Directory not exist... Creating one...")
    os.makedirs(REPORTSDIR)

response = S3Client.list_buckets()
results = dict()
for i in range(len(response['Buckets'])):
    res = listObjects(S3Client, response['Buckets'][i]['Name'])
    results[res['bucketName']] = res['size']

print(results)
writeJsonFile(JSONFILENAME, results)