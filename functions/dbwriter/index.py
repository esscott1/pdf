import json
import os
import boto3
import csv
import pylightxl as xl
import io

def saveToDynamodb(data):
    print('--- saving to Dynamodb')
    try:
        db = boto3.client('dynamodb')
        tablenane='claimant'
        print(f"table name from environment variable is: {os.environ.get('TableName')}")
        db.put_item(TableName=tablenane, Item={'lastname':{'S':'Scott'},'firstname':{'S':'Eric'}})
    except Exception as e:
        print(f'--- error saving to dynamodb ---:  error:{e}')



def lambda_handler(event, context):

    print("JSON Event from SNS: " + json.dumps(event))

