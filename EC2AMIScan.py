import boto3
import json
import os
import argparse
from datetime import datetime, date
from helper import writeCsvFile, writeJsonFile

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract AMI Details")
parser.add_argument("-r",
                    "--region",
                    help="Target Region",
                    required=True)
parser.add_argument("-f",
                    "--filtername",
                    help="Filter Tag Name: e.g. [{'Name': 'name', 'Values': ['SAMPLE AMI NAME WILDCARD*']}]",
                    required=True)
parser.add_argument("-a",
                    "--age",
                    help="Age in days",
                    required=True, type=int)


# Read the arguments from the command line
args = parser.parse_args()
region = args.region
filtername = args.filtername
age = 0 if args.age == None else args.age

REGION          = region #'<region>'
FILTERS         = [{'Name': 'name', 'Values': [filtername]}]
OWNERS          = ['self'] # 'self','amazon'
JSONFILENAME    = './reports/EC2AMIScan.json'
DELJSONFILENAME = './reports/EC2AMIScanDelete.json'
AGE             = age # 90  # days # 3 months old AMI 90 days
DELETE          = False # Warning It awill automaticaly delete AMI that will fall to the query results.
HASRETAIN       = 1 # always check if there are retain copies; make sure not all AMI will be deleted even it's too old...

ec2Client = boto3.client('ec2', region_name=REGION)

def daysOld(dt): #Y-m-d
    if 'T' in dt:
        dt = dt.split('T')
        dt = dt[0]
    date_format = '%Y-%m-%d'
    today = date.today().strftime(date_format)
    a = datetime.strptime(dt, date_format)
    b = datetime.strptime(today, date_format)
    delta = b - a
    return int(delta.days)

def parseDeviceMappings(BlockDeviceMappings):
    deviceMapList=[]
    for i in range(len(BlockDeviceMappings)):
        if 'Ebs' in BlockDeviceMappings[i]:
            deviceMap = dict()
            deviceMap['snapshot'] = BlockDeviceMappings[i]['Ebs']['SnapshotId']
            deviceMap['volumeType'] = BlockDeviceMappings[i]['Ebs']['VolumeType']
            deviceMap['VolumeSize'] = BlockDeviceMappings[i]['Ebs']['VolumeSize']
            deviceMapList.append(deviceMap)
        else:
            continue
    return deviceMap

def parseScanedAMIs(images):
    parsedScannedAMIs = dict()
    skipped = 0;
    for i in range(len(images['Images'])):
        CreationDate              = images['Images'][i]['CreationDate']
        ageDays = daysOld(CreationDate)
        if AGE > 0:
            if ageDays < AGE:
                skipped = skipped + 1
                continue

        ImageId                   = images['Images'][i]['ImageId']
        BlockDeviceMappings       = images['Images'][i]['BlockDeviceMappings']
        parsedBlockDeviceMappings = parseDeviceMappings(BlockDeviceMappings)
        parsedScannedAMIs[ImageId] = dict()
        c = parsedScannedAMIs[ImageId]
        c['Name'] = images['Images'][i]['Name']
        c['CreationDate'] = CreationDate
        c['AgeDays'] = ageDays
        c['VolumeGB'] = parsedBlockDeviceMappings

    parsedScannedAMIs['hasRetain'] = skipped
    return parsedScannedAMIs

def scanAmi(ec2Client):
    images = ec2Client.describe_images(Owners=OWNERS, Filters=FILTERS)
    parsedAMIs = parseScanedAMIs(images)
    return parsedAMIs

def cleanAMI(ec2Client, amiId, snapshotId):
    cleanOpsDetails=dict()
    deRegAMI = ec2Client.deregister_image(
        ImageId=amiId,
        DryRun=False
    )
    delSnap = ec2Client.delete_snapshot(
        SnapshotId=snapshotId,
        DryRun=False
    )
    cleanOpsDetails[amiId] = deRegAMI['ResponseMetadata']['HTTPStatusCode']
    cleanOpsDetails[snapshotId] = delSnap['ResponseMetadata']['HTTPStatusCode']
    return cleanOpsDetails

scannedAMI = scanAmi(ec2Client)
if len(scannedAMI) > 0:
    if os.path.exists(JSONFILENAME) == True:
        os.remove(JSONFILENAME)
    if os.path.exists(DELJSONFILENAME) == True:
        os.remove(DELJSONFILENAME)

if DELETE == True:
    print("Delete Flag Detected...")
    if scannedAMI['hasRetain'] >= HASRETAIN:
        print("Will Retain: " + str(scannedAMI['hasRetain']) + " Copies...")
        delOps = dict()
        hasFetched = False
        for i in scannedAMI:
            if i == 'hasRetain':
                continue
            else:
                hasFetched = True
                snapshotid = scannedAMI[i]['VolumeGB']['snapshot']
                imageName  = scannedAMI[i]['Name']
                # Disable 
                # delRes = 200
                delRes = cleanAMI(ec2Client, i, snapshotid)
                delOps[imageName] = delRes
        if(hasFetched):
            writeJsonFile(DELJSONFILENAME, delOps)
            print("Delete Report Generated...")
        else:
            print("Nothing Found...")
    else:
        print("No Delete Due to Retain Constraint: " + str(scannedAMI['hasRetain']) + " Copies... No Copies will be retain if delete proceeds... Manual checking is recommended...")

else:
    print("Delete Flag NOT Detected... No Deletion will be done...")
    delOps = dict()
    hasFetched = False;
    for i in scannedAMI:
        if i == 'hasRetain':
            continue
        else:
            hasFetched = True;
            snapshotid = scannedAMI[i]['VolumeGB']['snapshot']
            imageName = scannedAMI[i]['Name']
            # Disable
            # delRes = 200
            delRes = dict()
            delRes[i] = i
            delRes[snapshotid] = snapshotid
            delOps[imageName] = delRes
    if(hasFetched):
        writeJsonFile(DELJSONFILENAME, delOps)
        print("Report Generated...")
    else:
        print("Nothing Found...")

