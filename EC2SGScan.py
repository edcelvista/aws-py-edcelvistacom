import boto3, json
from helper import writeCsvFile, writeJsonFile

REGION       = '<region>'
CIDRFIND     = '<Cidr Value>'#'0.0.0.0/0'
# TAGFIND    = 'Delete Me'
TAGFIND      = '<Security Group Tag>' #'Bahrain-API'
SCANTYPE     = 'TAG'  # CIDR or TAG
JSONFILENAME = './reports/EC2SGScan.json'
CSVFILENAME  = './reports/EC2SGScan.csv'

ec2Client = boto3.client('ec2', region_name=REGION)

def scanSg(ec2Client):
    security_groups_dict = ec2Client.describe_security_groups()
    forCheckSg = dict()
    for i in range(len(security_groups_dict['SecurityGroups'])):
        if(len(security_groups_dict['SecurityGroups'][i]['IpPermissions']) > 0):
            for j in range(len(security_groups_dict['SecurityGroups'][i]['IpPermissions'])):
                for k in range(len(security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'])):
                    if SCANTYPE == 'CIDR':
                        if security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'][k]['CidrIp'] == CIDRFIND:
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
                            if TAGFIND in security_groups_dict['SecurityGroups'][i]['IpPermissions'][j]['IpRanges'][k]['Description']:
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
writeJsonFile(JSONFILENAME, scannedSg)
writeCsvFile(CSVFILENAME, scannedSg, ['Security Group ID', 'Ingress Ports'])
