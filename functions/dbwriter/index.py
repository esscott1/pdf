import json
import os
import boto3
import csv
import pylightxl as xl
import io

def saveJsonToPostgres(data):
    print('--- saving to Dynamodb')
    try:
        db = boto3.client('dynamodb')
        tablenane='claimant'
        print(f"table name from environment variable is: {os.environ.get('TableName')}")
        db.put_item(TableName=tablenane, Item={'lastname':{'S':'Scott'},'firstname':{'S':'Eric'}})
    except Exception as e:
        print(f'--- error saving to dynamodb ---:  error:{e}')

def get_connection():
    """
    Method to establish the connection to RDS using IAM Role based authentication.
    """
    try:
        print ('Connecting to database')
        client = boto3.client('rds')
        DBEndPoint = os.environ.get('DBEndPoint')
        DatabaseName = os.environ.get('DatabaseName')
        DBUserName = os.environ.get('DBUserName')
        # Generates an auth token used to connect to a db with IAM credentials.
        print ('trying to get password with auth token for endpoint: ',DBEndPoint)
        password = client.generate_db_auth_token(
            DBHostname=DBEndPoint, Port=5432, DBUsername=DBUserName
        )
        print ('connecting to: ', DatabaseName)
        print ('connecting as: ', DBUserName)
        print ('connecting with pw: ', password)
        # Establishes the connection with the server using the token generated as password
        conn = pg8000.connect(
            host=DBEndPoint,
            user=DBUserName,
            database=DatabaseName,
            password=password,
            ssl={'sslmode': 'verify-full', 'sslrootcert': 'rds-ca-2015-root.pem'},
        )
        print ("Succesful connection!")
        return conn
    except Exception as e:
        print ("While connecting failed due to :{0}".format(str(e)))
        return None

def lambda_handler(event, context):

    print("JSON Event from SNS: " + json.dumps(event))
    msg = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    print(f'message to save to jsondata in database {msg}')
#    saveJsonToPostgres(msg)

