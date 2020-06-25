import boto3, json, os
from datetime import datetime, date
from helper import writeCsvFile, writeJsonFile

REGION          = '<region>'
FILTERS         = '<Filter>' #[{'Name': 'name', 'Values': ['SAMPLE AMI NAME WILDCARD*']}]
OWNERS          = ['self'] # 'self','amazon'
JSONFILENAME    = './reports/EC2AMIScan.json'
CSVFILENAME     = './reports/EC2AMIScan.csv'
DELJSONFILENAME = './reports/EC2AMIScanDelete.json'
DELCSVFILENAME  = './reports/EC2AMIScanDelete.csv'
AGE             = 90  # days # 3 months old AMI 90 days
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
        if AGE != '':
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

print("Starting Scan...")
scannedAMI = scanAmi(ec2Client)
if len(scannedAMI) > 0:
    if os.path.exists(JSONFILENAME) == True:
        os.remove(JSONFILENAME)
    if os.path.exists(CSVFILENAME) == True:
        os.remove(CSVFILENAME)
    if os.path.exists(DELJSONFILENAME) == True:
        os.remove(DELJSONFILENAME)
    if os.path.exists(DELCSVFILENAME) == True:
        os.remove(DELCSVFILENAME)

if DELETE == True:
    print("Delete Flag Detected...")
    if scannedAMI['hasRetain'] >= HASRETAIN:
        print("Will Retain: " + str(scannedAMI['hasRetain']) + " Copies...")
        delOps = dict()
        for i in scannedAMI:
            if i == 'hasRetain':
                continue
            else:
                snapshotid = scannedAMI[i]['VolumeGB']['snapshot']
                imageName  = scannedAMI[i]['Name']
                # Disable 
                # delRes = 200
                delRes = cleanAMI(ec2Client, i, snapshotid)
                delOps[imageName] = delRes
        writeJsonFile(DELJSONFILENAME, delOps)
        writeCsvFile(DELCSVFILENAME, delOps, ['AMI Name', 'Status'])
        print("Delete Report Generated...")
    else:
        print("No Delete Due to Retain Constraint: " + str(scannedAMI['hasRetain']) + " Copies... No Copies will be retain if delete proceeds... Manual checking is recommended...")
    
writeJsonFile(JSONFILENAME, scannedAMI)
writeCsvFile(CSVFILENAME, scannedAMI, ['AMI ID', 'Details'])
print("Report Generated...")


