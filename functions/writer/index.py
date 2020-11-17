import json
import boto3
import os
import pg8000
import csv
from trp import Document

def getJobResults(jobId):
    """
    Get readed pages based on jobId
    """

    pages = []
    textract = boto3.client('textract')
    response = textract.get_document_analysis(JobId=jobId)
    
    pages.append(response)

    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        response = textract.get_document_analysis(JobId=jobId, NextToken=nextToken)
        pages.append(response)
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def convert_row_to_list(row):
    """
    Helper method to convert a row to a list.
    """
    list_of_cells = [cell.text.strip() for cell in row.cells]
    return list_of_cells

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

def save_orc_to_bucket(all_values):
    csv_file='/tmp/data.csv'
    csv_columns = ['LastName','FirstName','Phone','SSN','Street','City','State','Zip']
   ## writing to lambda temp area
    print('trying to write file to temp lambda space')
    try:
        with open('/tmp/data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in all_values:
                writer.writerow(data)
    except Exception as e:
       print('error writing csv to lambda local:', e)

    # upload file to s3 bucket
    AWS_BUCKET_NAME = 'archer-ocr-doc-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    try:
        bucket.upload_file(csv_file,'s3tosalesforce/data_for_salesforce.csv')
    except Exception as s:
        print('error uploading local lambda file to s3')


def save_to_bucket(all_values):
    csv_file='/tmp/data.csv'
    csv_columns = ['DATE','DESCRIPTION','RATE','HOURS','AMOUNT']
   ## writing to lambda temp area
    print('trying to write file to temp lambda space')
    try:
        with open('/tmp/data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in all_values:
                writer.writerow(data)
    except Exception as e:
       print('error writing csv to lambda local:', e)

    # upload file to s3 bucket
    AWS_BUCKET_NAME = 'archer-ocr-doc-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    path = 'all_values.json'
    data = b'some data'

    try:
        bucket.upload_file(csv_file,'data.csv')
    except Exception as s:
        print('error uploading local lambda file to s3')

    print('trying to write file all_values.json to bucket')
    print(bucket)
    try:
        bucket.put_object(
            ACL='public-read',
            ContentType='application/json',
            Key=path,
            Body=data,
        )
    except Exception as e:
        print(e)
        print('error trying to write to bucket')

    body = {
        "uploaded": "true",
        "bucket": AWS_BUCKET_NAME,
        "path": path,
    }

def write_dict_to_db(mydict, connection):
    """
    Write dictionary to our invoices table.
    """
    DBTable = os.environ.get('TableName')
    cursor = connection.cursor()
    placeholders = ', '.join(['%s'] * len(mydict))
    columns = ', '.join(mydict.keys())
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (DBTable, columns, placeholders)
    print(sql, mydict.values())
    print(list(mydict.values()))
    cursor.execute(sql, list(mydict.values()))
    connection.commit()
    cursor.close()

def printresponsetos3(doc):
     # upload file to s3 bucket
    AWS_BUCKET_NAME = 'archer-ocr-doc-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    path = 'textractresponse.txt'
    data = doc
    print('trying to print textract response to s3')
    try:
        bucket.put_object(
            ACL='public-read',
            ContentType='application/json',
            Key=path,
            Body= str(data),
        )
    except Exception as e:
        print(e)
        print('error trying to write doc to bucket')


def lambda_handler(event, context):
    """
    Get Extraction Status, JobTag and JobId from SNS. 
    If the Status is SUCCEEDED then create a dict of the values and write those to the RDS database.
    """
    notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    
    pdfTextExtractionStatus = json.loads(notificationMessage)['Status']
    pdfTextExtractionJobTag = json.loads(notificationMessage)['JobTag']
    pdfTextExtractionJobId = json.loads(notificationMessage)['JobId']
    pdfTextExtractionDocLoc = json.loads(notificationMessage)['DocumentLocation']

    print(pdfTextExtractionJobTag + ' : ' + pdfTextExtractionStatus)
    print('printing doc location')
    print(pdfTextExtractionDocLoc)

    docname = json.loads(pdfTextExtractionDocLoc)['S3ObjectName']
    print('document name is: '+docname)
    if(pdfTextExtractionStatus == 'SUCCEEDED'):
        response = getJobResults(pdfTextExtractionJobId)
        doc = Document(response)

    printresponsetos3(doc)
    all_values = []

    for page in doc.pages:
        for field in page.form.fields:
            print(f'key found in form:{field.key}: with value :{field.value}:')
            if str(field.key) == 'Phone':
                phone = field.value
                print('the phone number is: ',field.value)
            if str(field.key) == 'First':
                first = field.value
                print('the first name is: ',field.value)
            if str(field.key) == 'Last':
                last = field.value
                print('the last name is: ',field.value)
            if str(field.key) == 'Social Security Number':
                ssn = field.value
                print('the ssn is: ',field.value)
            if str(field.key) == 'Street/P.O. Box':
                street = field.value
                print('the street is: ',field.value)
            if str(field.key) == 'City':
                city = field.value
                print('the city is: ',field.value)
            if str(field.key) == 'State':
                state = field.value
                print('the state is: ',field.value)
            if str(field.key) == 'Zip':
                zip = field.value
                print('the zip is: ',field.value)

    all_values.append({'FirstName': first,'LastName':last,'Phone':phone,'SSN':ssn,'Street':street,'City':city,'State':state,'Zip':zip})


    print('printing all values:')
    print(all_values)
    save_orc_to_bucket(all_values)
    #    print(page.form)
"""


    for page in doc.pages:
        for table in page.tables:
            for i, row in enumerate(table.rows):
                if i == 0:
                    keys = convert_row_to_list(table.rows[0])
                else:
                    values = convert_row_to_list(row)
                    all_values.append(dict(zip(keys, values)))

    #  print('printing all values')
    #  print(all_values)
    save_to_bucket(all_values)
    connection = get_connection()
    for dictionary in all_values:
        write_dict_to_db(dictionary, connection)
"""