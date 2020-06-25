import boto3
import json
import os
import time
from datetime import datetime, date, timedelta

from helper import writeCsvFile, writeJsonFile

REGION            = '<region>'
# MAXEVENTLOGSAGE = 10 ## hours
SFNARN            = '<step function arn>'
JSONFILENAME      = './reports/StepFunctionExecScan.json'

client = boto3.client('stepfunctions')

def describeStateMachine(client, stateMachineArn):
    stateMachineDetails = {}
    response = client.describe_state_machine(
        stateMachineArn=stateMachineArn
    )
    stateMachineDetails['status'] = response['status']
    stateMachineDetails['definition'] = response['definition']
    stateMachineDetails['status'] = response['status']
    stateMachineDetails['roleArn'] = response['roleArn']

    return stateMachineDetails

def listExecutions(client, stateMachineArn):
    executions = []
    response = client.list_executions(
        stateMachineArn=stateMachineArn,
        # statusFilter='RUNNING' | 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'ABORTED',
        maxResults=10,
        # nextToken='string'
    )
    if "executions" in response:
        for i in range(len(response['executions'])):
            response['executions'][i]['startDate'] = str(response['executions'][i]['startDate'])
            response['executions'][i]['stopDate'] = str(response['executions'][i]['stopDate'])
            executions.append(response['executions'][i])

    return executions

def getExecHistory(client, executionArn):
    events = []
    response = client.get_execution_history(
        executionArn=executionArn,
        maxResults=50,
        # reverseOrder=True | False,
        reverseOrder=True
        # nextToken='string'
    )
    if 'events' in response:
        for i in range(len(response['events'])):
            response['events'][i]['timestamp'] = str(response['events'][i]['timestamp'])
            events.append(response['events'][i])

    return events

executionList = listExecutions(client, SFNARN)
exectionHistoryPerExecutionArn = []
for i in range(len(executionList)):
    c = dict()
    executionArn = executionList[i]['executionArn']
    executionEventList = getExecHistory(client, executionArn)
    c[executionArn] = executionEventList
    exectionHistoryPerExecutionArn.append(c)

writeJsonFile(JSONFILENAME, exectionHistoryPerExecutionArn)
print("Report Generated...")
