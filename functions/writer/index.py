import json
import boto3
import os
import pg8000
import csv
from trp import Document


csv_2_ocr_map = {
'Claimant First Name': {'ocr_key':'First', 'PageNo': 2, 'TopPos': 1, 'geometry':{'top':0.995}}, 
'Claimant Last Name': {'ocr_key':'Last', 'PageNo': 2, 'TopPos': 1, 'geometry':{'top':0.995}}
}

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

def save_orc_to_bucket(all_values, docname):
    csv_file='/tmp/'+docname+'_data.csv'
    csv_columns = ['LastName','FirstName','Phone','SSN','Street','City','State','Zip','SourceDocName']
   ## writing to lambda temp area
    print('trying to write file to temp lambda space named: '+csv_file)
    try:
        with open('/tmp/'+docname+'_data.csv', 'w') as csvfile:
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
        bucket.upload_file(csv_file,'s3tosalesforce/'+docname+'.csv')
    except Exception as s:
        print('error uploading local lambda file to s3')

'''
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
'''
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

'''
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
'''

def write_csv(mydict, docname):
    csv_columns = mydict.keys()
    csv_file='/tmp/'+docname+'_data.csv'
#    csv_columns = ['LastName','FirstName','Phone','SSN','Street','City','State','Zip','SourceDocName']
   ## writing to lambda temp area
    print('trying to write file to temp lambda space named: '+csv_file)
    try:
        with open('/tmp/'+docname+'_data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in mydict:
                writer.writerow(data)
    except Exception as e:
       print('error writing csv to lambda local:', e)

    # upload file to s3 bucket
    AWS_BUCKET_NAME = 'archer-ocr-doc-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    try:
        bucket.upload_file(csv_file,'s3tosalesforce/'+docname+'.csv')
    except Exception as s:
        print('error uploading local lambda file to s3')


csv_2_orc_map = {'Claimant First Name': 'First', 'Claimant Last Name': 'Last'}


"""
def get_enrollment_dict(doc):
    mydict = {}
    for page in doc.pages:
        print('---- page ----')
        
        for csv_key in csv_2_ocr_map
            x = filter(lambda x: x.key == csv_2_ocr_map[csv_key] page.form.fields)  # finding the field in ocr form that matches the csv column
            l = list(x)
            for ocritem in l:
                mydict[csv_key] = str(ocritem.value) # adding the 
            mydict.update()
            if(list(x)>1):
                print('figure which one')


            mydict.append()

def get_first_field(fields):
    for field in fields:
        print(field.key.geometry.top)
    retunn fields[0]
"""

def GetFromTheTop(fieldlist, pos):
    print('--- unsorted ---')
    for field in fieldlist:
        print('key: ',field.key,' value: ',field.value,' toplocation: ',field.key.geometry.boundingBox.top)

    sorted_field = sorted(fieldlist, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)

    print('--- sorted ---')
    for field in sorted_field:
        print('key: ',field.key,' value: ',field.value,' toplocation: ',field.key.geometry.boundingBox.top)
    
    print('first one is',sorted_field[pos].value)
    return sorted_field[pos]


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
#     print('printing doc location')
#     print(pdfTextExtractionDocLoc)

    docname = pdfTextExtractionDocLoc['S3ObjectName']
    print('document name is: '+docname)
    if(pdfTextExtractionStatus == 'SUCCEEDED'):
        response = getJobResults(pdfTextExtractionJobId)
        doc = Document(response)

#    printresponsetos3(doc)
    all_keys = []
    all_values = []
    pageno = 0
#   building the array of KVP
    for page in doc.pages:
        pageno = pageno + 1
        print('---- page ',str(pageno),' ----',)
        for csv_key in csv_2_ocr_map:
            print('Looking for csv_key is: ',csv_key,' | ocr key: ', csv_2_ocr_map[csv_key]['ocr_key'],' | at TopPos: ', str(csv_2_ocr_map[csv_key]['TopPos'])) 
            #),str(csv_2_orc_map[csv_key]['TopPos']) )
            es = filter(lambda x: str(x.key)== str(csv_2_ocr_map[csv_key]['ocr_key']),page.form.fields) 

            lFields = list(es)
            print(f"i found {str(len(lFields))} field objects")
            if(len(lFields)>0):
                correctField = GetFromTheTop(lFields,0)
                print(f'--- the correctField is {correctField.value}')
            else:
                print(' --- no correctField found --- ')
            #print(f'write a cell to column: {csv_key} with value: {correctField.value}')

"""         for field in page.form.fields:
            GetKvp
            if str(field.key) == 'Last':
                print(f'key found in form:{field.key}: with value :{field.value}: at top: {field.key.geometry.boundingBox.top}')
            if str(field.key) == 'First':
                print(f'key found in form:{field.key}: with value :{field.value}: at top: {field.key.geometry.boundingBox.top}')
           all_keys.append(str(field.key))
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
                ssn = str(field.value).replace(" ","")
                print('the ssn is: ',field.value)
            if str(field.key).startswith('Street'):
                street = field.value
                print('the street is: ',field.value)
            if str(field.key) == 'City':
                city = field.value
                print('the city is: ',field.value)
            if str(field.key).startswith('State'):
                state = field.value
                print('the state is: ',field.value)
            if str(field.key) == 'Zip':
                zip = field.value
                print('the zip is: ',field.value)
"""

#    all_values.append({'FirstName': first,'LastName':last,'Phone':phone,'SSN':ssn,'Street':street,'City':city,'State':state,'Zip':zip,'SourceDocName':docname})

#    print('all keys:')
#    print(all_keys)
#    print('printing all values:')
#    print(all_values)
#    save_orc_to_bucket(all_values, docname)
#    printSections(doc)
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
def printSections(doc):
    print('trying to print out SelectionElement:')
    for page in doc.pages:
        for field in page.form.fields:
            if str(field.value) == 'SELECTED':
                print('checkbox: '+str(field.key)+' is: '+str(field.value))
                print(' with confidence: '+str(field.key.confidence))
                print ('block: '+str(field.key.block))