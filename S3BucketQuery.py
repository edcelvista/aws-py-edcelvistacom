import boto3, argparse, os
from os import path
from helper import writeCsvFile, writeJsonFile, convertBytesSize

# Generic Attributes
BATCHLISTING = 500
DOWNLOADDIR  = './downloads/'

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract objects in Specified Buckets, prefix and filter")
parser.add_argument("-b",
                    "--bucketname",
                    help="Target Bucket Name.",
                    required=True)
parser.add_argument("-p",
                    "--prefix",
                    help="Bucket prefixes or Directories.",
                    required=False)
parser.add_argument("-f",
                    "--searchfilter",
                    help="Filter name | e.g. part of the name string.",
                    required=True)
parser.add_argument("-s",
                    "--awsprofile",
                    help="AWS Profile.",
                    required=False)
parser.add_argument("-d",
                    "--isdownload",
                    help="Download Objects.",
                    required=False)

# Read the arguments from the command line
args         = parser.parse_args()
bucketname   = args.bucketname
prefix       = "" if args.prefix == None else args.prefix
searchfilter = args.searchfilter
awsprofile   = "default" if args.awsprofile == None else args.awsprofile
isdownload   = "False" if args.isdownload == None else args.isdownload

# User Attributes
BUCKET       = bucketname # '<bucket>'  # 'bucketname'
PREFIX       = prefix  # '<prefix | dir>' #'dir1/dir2/'
SEARCHPREFIX = searchfilter # '<name prefix>'  # 'name-file'

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

                if(keyObj.find(SearchPrefix) >= 0):
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
    if isdownload == "True":
        print("Fetching: " + str(ObjKey))
        with open(DOWNLOADDIR+ObjKey.split("/")[-1], 'wb') as data:
            client.download_fileobj(Bucket, ObjKey, data)
    else:
        print("Listing: " + str(ObjKey))

# Main
contextSettings(args)
if not path.exists(DOWNLOADDIR):
    print("Download Directory not exist... Creating one...")
    os.makedirs(DOWNLOADDIR)

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