import boto3
import json
import argparse
from helper import writeCsvFile, writeJsonFile

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract AMI Details")
parser.add_argument("-r",
                    "--region",
                    help="Target Region",
                    required=True)
parser.add_argument("-s",
                    "--scantype",
                    help="scantype: CIDR | TAG",
                    required=True)
parser.add_argument("-p",
                    "--prefix",
                    help="prefix tag or cird to be scanned",
                    required=False)

# Read the arguments from the command line
args = parser.parse_args()
region = args.region
scantype = args.scantype
prefix = args.prefix

REGION       = region #'<region>'
SEARCHSTR    = prefix
SCANTYPE     = scantype  # CIDR or TAG
JSONFILENAME = './reports/EC2SGScan.json'

ec2Client = boto3.client('ec2', region_name=REGION)

def scanSg(ec2Client):
    security_groups_dict = ec2Client.describe_security_groups()
    forCheckSg = dict()
    for i in range(len(security_groups_dict['SecurityGroups'])):
        if(len(security_groups_dict['SecurityGroups'][i]['IpPermissions']) > 0):
            for j in range(len(security_groups_dict['SecurityGroups'][i]['IpPermissions'])):
                for k in range(len(security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'])):
                    if SCANTYPE == 'CIDR':
                        if security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'][k]['CidrIp'] == SEARCHSTR:
                            GroupId = security_groups_dict['SecurityGroups'][i]['GroupId'].rstrip("\n\t")
                            port = security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['FromPort']
                            if GroupId in forCheckSg:
                                c = forCheckSg[GroupId]
                                c.append(port)
                            else:
                                c = []
                                c.append(port)
                                forCheckSg[GroupId] = c
                    elif SCANTYPE == 'TAG':
                        if 'Description' in security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'][k]:
                            if SEARCHSTR in security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'][k]['Description']:
                                GroupId = security_groups_dict['SecurityGroups'][i]['GroupId'].rstrip("\n\t")
                                port = security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['FromPort']
                                if GroupId in forCheckSg:
                                    c = forCheckSg[GroupId]
                                    c.append(port)
                                else:
                                    c = []
                                    c.append(port)
                                    forCheckSg[GroupId] = c
    return forCheckSg

scannedSg = scanSg(ec2Client)
if(len(scannedSg) > 0):
    writeJsonFile(JSONFILENAME, scannedSg)
    print("Report Generated...")
else:
    print("Nothing Found...")
