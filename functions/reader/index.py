import json
import os
import boto3
import csv
import pylightxl as xl
import io

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
        print('--- did not find file ---')
    if 'xlsx' in str(key):
        print('---- found an Excel file ---')
        s3 = boto3.client('s3')
        s3.download_file(bucket,key,'/tmp/test.xlsx')
        wb = load_workbook.readxl(fn='/tmp/test.xlsx')
        print(wb.ws_names)
    else:
        print('--- did not find an Excel file ---')