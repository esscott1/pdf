import json
import boto3
import os
import pg8000
import csv

def get_connection():
    """
    Method to establish the connection to RDS using IAM Role based authentication.
    """
    try:
        print ('Connecting to database')
        client = boto3.client('rds')
        #dDBEndPoint = os.environ.get('DBEndPoint')
        DBEndPoint = 'ddhgkdwsopbt3a.c3bquq8vfcla.us-west-2.rds.amazonaws.com'
        #DatabaseName = os.environ.get('DatabaseName')
        DatabaseName = 'InvoiceDB'
        #DBUserName = os.environ.get('DBUserName')
        DBUserName = 'demouser'
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

get_connection()