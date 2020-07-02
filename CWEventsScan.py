import boto3
import json
import os
import time
import argparse
from datetime import datetime, date, timedelta
from helper import writeCsvFile, writeJsonFile

# Define arguments for command line execution
parser = argparse.ArgumentParser(
    description="Extract Logs in Specified Cloudwatch Group Get Latest Stream")
parser.add_argument("-r",
                    "--region",
                    help="Target CW Log Group Region",
                    required=True)
parser.add_argument("-l",
                    "--cloudwatchgroup",
                    help="Target CW Log Group Name",
                    required=True)
# parser.add_argument("-a",
#                     "--logage",
#                     help="Log age to be fetched",
#                     required=False)

# Read the arguments from the command line
args            = parser.parse_args()
region          = args.region
cloudwatchgroup = args.cloudwatchgroup
# logage          = 10 if args.logage == None else args.logage

REGION            = region
CWLOGGROUP        = cloudwatchgroup   # '<Log Group Name>' #"/aws/lambda/notificationLambda"
# MAXEVENTLOGSAGE   = logage
JSONFILENAME      = './reports/CWEventsScan.json'

logsclient = boto3.client('logs', region_name=REGION)

def describeLogGroup(client, logGroupNamePrefix):
    parsedScannedCWlg = []
    cwlg = client.describe_log_groups(
        logGroupNamePrefix=logGroupNamePrefix,
        # nextToken='string',
        limit=50
    )
    for i in range(len(cwlg['logGroups'])):
        c = dict()
        c['arn'] = cwlg['logGroups'][i]['arn']
        c['logGroupName'] = cwlg['logGroups'][i]['logGroupName']
        parsedScannedCWlg.append(c)
    return parsedScannedCWlg


def getLogEvents(client, logGroupName, logStreamName):
    response = client.get_log_events(
        logGroupName=logGroupName,
        logStreamName=logStreamName,
        # startTime=startTime,
        # endTime=endTime,
        # nextToken='string',
        limit=500,
        startFromHead=True
    )

    return response


def getLogStreams(client, logGroupName):
    parsedScannedStream = []
    cwstream = client.describe_log_streams(
        logGroupName=logGroupName,
        # logStreamNamePrefix=logStreamName,
        # orderBy='LogStreamName' | 'LastEventTime',
        orderBy='LastEventTime',
        descending=True,
        # nextToken='string',
        limit=50
    )
    for i in range(len(cwstream['logStreams'])):
        if "lastEventTimestamp" in cwstream['logStreams'][i]:
            c = dict()
            c['arn'] = cwstream['logStreams'][i]['arn']
            c['logStreamName'] = cwstream['logStreams'][i]['logStreamName']
            c['lastEventTimestamp'] = cwstream['logStreams'][i]['lastEventTimestamp']
            parsedScannedStream.append(c)

    return parsedScannedStream


logGroupDetails = describeLogGroup(logsclient, CWLOGGROUP)

logsparsed = []
hasFetchStream = False
for i in range(len(logGroupDetails)):
    cwlgname = logGroupDetails[i]['logGroupName']
    cwlgRecentStream = getLogStreams(logsclient, cwlgname)

    for j in range(len(cwlgRecentStream)):
        hasFetchStream = True
        cwlgStreamGetLatest = cwlgRecentStream[i]['logStreamName'] # get the latest stream..
        # last_hour_date_time = datetime.now() - timedelta(hours=MAXEVENTLOGSAGE)
        # int(time.time()), int(last_hour_date_time.strftime("%s"))
        cwlgEvents = getLogEvents(logsclient, cwlgname, cwlgStreamGetLatest)
        logsparsed.append(cwlgEvents)

if(hasFetchStream):
    writeJsonFile(JSONFILENAME, logsparsed)
    print("Report Generated...")
else:
    print("Nothing Found...")

