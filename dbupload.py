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

def write_dict_to_db(mydict, connection):
    """
    Write dictionary to the table name provided in the SAM deployment statement as lambda environment variable.
    """
#    DBTable = os.environ.get('TableName')
    DBTable = 'ca_existing'

    placeholders = ', '.join(['%s'] * len(mydict))
    columns = ', '.join(mydict.keys())
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (DBTable, columns, placeholders)

    fieldtextlist = []
    fieldvaluelist =  list(mydict.values())
    for fieldvalue in fieldvaluelist:
        fieldtextlist.append(str(fieldvalue))

    print(sql, fieldvaluelist)
    print(fieldtextlist)
    cursor = connection.cursor()
    cursor.execute(sql, fieldtextlist)

    connection.commit()
    cursor.close()

def csv_to_dict():
    allvalues = []
#    dict={}
    with open('testaddress.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            dict={}
            archer_id = row[0]
            print('archerid is: '+archer_id+'and the type is: '+type(archer_id))

            dict['archer_id']=archer_id
            dict['address']=row[1]
            dict['city']=row[2]
            dict['state']=row[3]
            dict['zip']=row[4]
#            print(f', '.join(dict.keys()))
            allvalues.append(dict)
#        print(allvalues)
    return allvalues
#    print(dict)

connect = get_connection()
data = csv_to_dict()
for rowdata in data:
    write_dict_to_db(rowdata,connect)