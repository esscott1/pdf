import json
import os
import boto3
import csv
import pylightxl as xl
import io
from boto3.dynamodb.types import TypeSerializer

@claimant_dataclasss
class Claimant:
    firstname: str
    lastname: str
    ssn: str
    dob: str

def saveToDynamodb(data):
    print('--- saving to Dynamodb')
    try:
        db = boto3.client('dynamodb')
#        db.put_item(TableName='claimant', Item={'lastname':{'S':'Scott'},'firstname':{'S':'Eric'}})
        datatoload = json.loads(data)
        print(f"--- printing data to load ---")
        print(datatoload)

       serializer = TypeSerializer()
        print(f"--- printing dynamo json data to load ---")
        try:
            dyamamoJson = {k: serializer.serialize(v) for k, v in Claimant.Schema().dump(datatoload) if v != ""}
            print(dynamoJson)
        except Exception as e:
            print(f'failed making dynamo db, error: {e}')
        db.put_item(TableName='claimant', Item=datatoload)
    except Exception as e:
        print(f'--- error saving to dynamodb ---:  error:{e}')

def publishSNS(workbook):
    snsclient = boto3.client('sns')
    print('---- trying to publish message ----')
    try:
        response= snsclient.publish(
            TopicArn='arn:aws:sns:us-west-2:021025786029:ARCHERClaimantSNSTopic',
            Message=str(workbook),
            Subject='SNS message from Lambda')
        print(f'--- published sns message ---')
        print(response)
    except Exception as e:
        print(f'---  error in publishing to sns; error: {e}')


def lambda_handler(event, context):

    print("Triggered getTextFromS3PDF event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    if 'pdf' in str(key):
        print('---- found a pdf file ---')
        try:
            textract = boto3.client('textract')
            textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            JobTag=key + '_Job',
            FeatureTypes=[
                'FORMS'
            ],
            NotificationChannel={
                'RoleArn': os.environ['SNSROLEARN'],
                'SNSTopicArn': os.environ['SNSTOPIC']
            })
            
            return 'Triggered PDF Processing for ' + key
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
    else:
        print('--- did not find PDF file ---')
    if 'xlsx' in str(key):
        print('---- found an Excel file ---')
        s3 = boto3.client('s3')
        s3.download_file(bucket,key,'/tmp/test.xlsx')
        wb = xl.readxl(fn='/tmp/test.xlsx')
        print(wb.ws_names)
        data = []
        dict = {}
        columns = wb.ws('Sheet1').row(1)
        print("--- trying excel to json ----")
        try:
            print(f"length of columns is {len(columns)}")
            for i in range(len(columns)):
                dict[columns[i]] = wb.ws('Sheet1').row(2)[i]
        except expression as identifier:
            print(f"error json from excel: error {identifier}")

        publishSNS(json.dumps(dict))
        saveToDynamodb(json.dumps(dict))
       
    else:
        print('--- did not find an Excel file ---')