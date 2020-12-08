import json
import boto3
import os
import pg8000
import csv
from trp import Document

csv_headers = ['Primary Attorney',
'HTX ARCHER ID',
'ATX ARCHER ID',
'Referring Firm',
'Law Firm ID (if applicable)',
'Claimant Participation Status',
'Lawsuit Filed (Filed/Unfiled)',
'Court/Jurisdiction',
'Case Caption',
'Case No',
'Derivative Claim of Consortium Plaintiff',
'Consortium Plaintiff First Name',
'Consortium Plaintiff Last Name',
'Claim Package Scanned Date',
'Claim Package Logged Date',
'Analyst Name',
'Claimant First Name',
'Claimant Last Name',
'Estate Representative First Name',
'Estate Representative Last Name',
'Address 1',
'Address 2',
'City',
'State',
'Zip Code',
'Email Address',
'Phone Number 1',
'Phone Number 2',
'Estate Representative Email Address',
'Estate Representative Phone Number 1',
'Estate Representative Phone Number 2',
'Roundup Enrollment Form[I elect to participate in the ROUNDUP Settlement Program]',
'Claimant Last Name2',
'Claimant First Name3',
'Claimant Social Security Number',
'Claimant Date of Birth',
'Address 14',
'Address 25',
'City6',
'State7',
'Zip Code8',
'Current Citizenship Status',
'Citizen Status Eligiblity',
'Citizenship Status at time of Exposure',
'Citizen Status at time of Exposure Eligiblity',
'Were you married at any time from the date of your initial NHL Diagnosis to the Present? (Yes of No)',
'Spouse First Name',
'Spouse Middle Name',
'Spouse Last Name',
'Is the Claim being brought by a Representative on behalf of the Product User? (Yes or No)',
'Relationship to Product User',
'If Other, specify below',
'Estate Representative Last Name9',
'Estate Representative First Name10',
'Estate Representative Middle Name',
'Estate Representative Address 1',
'Estate Representative Address 2',
'Estate Representative City',
'Estate Representative State',
'Estate Representative Zip Code',
'Estate Representative Social Security Number',
'Estate Representative Date of Birth',
'Claimant Date of Death',
'Do you claim that Roundup Products caused the Death? (if applicable - yes or no)',
'Eligibility Materials[A completed and signed Enrollment Form]',
'Eligibility Materials[Medical Record confirming Claimant\'s NHL diagnosis]',
'Eligibility Materials[Eligibility Affidavit]',
'Eligibility Materials[Executed Release]',
'Eligibility Materials[Stipulation of Dismissal with Prejudice for all cases filed in a court of law]',
'Enrollment Form Signature (Yes or No)',
'Enrollment Form Signature Date',
'Begin Date of Roundup Exposure',
'End Date of Roundup Exposure',
'Frequency of Roundup Exposure',
'Exposure to Roundup[Residential]',
'Exposure to Roundup[Agriculture]',
'Exposure to Roundup[Industrial, Turf & Oarnamental]',
'During the time period I was exposed to Roundup[Less than 10 hours per year on average]',
'During the time period I was exposed to Roundup[10-20 hours per year on average]',
'During the time period I was exposed to Roundup[20-40 hours per year on average]',
'During the time period I was exposed to Roundup[Over 40 hours per year on average]',
'My exposure to Roundup Prodcuts was in the following situations[Residential]',
'My exposure to Roundup Prodcuts was in the following situations[Agricultural]',
'My exposure to Roundup Prodcuts was in the following situations[Industrial/Occupational (landscaping, maintenance)]',
'The Roundup Products to which I was exposed include[Ready-to-Use Spray]',
'The Roundup Products to which I was exposed include[Mixed from Concentrate]',
'Have you ever had an incident in which you were soaked with Roundup? (Yes or No)',
'Purchase or Acquire Roundup Products at (state name of store, if applicable)',
'Unknown[I cannot remember or unknown]',
'Proof of Roundup Product Use (Yes of No)[Yes]',
'Proof of Roundup Product Use (Yes of No)[No]',
'If yes, please specify Proof of Roundup Product Use (pictures of products used, receipts of purchase, e.g.)',
'Date of Diagnosis of Non-Hodgkin Lymphoma (NHL)',
'Copy of Medical Record Available[Yes]',
'Copy of Medical Record Available[No]',
'Does date of diagnosis with NHL match Medical Record[Yes]',
'Does date of diagnosis with NHL match Medical Record[No]',
'Affidavit Signature (Yes or No)',
'Signature Date',
'Notarized (Yes or No)',
'Notarized Date',
'Confidential Release',
'Medicare? (Yes or No)',
'Signature by Releasing Party',
'Printed Name',
'Signed (Yes or No)',
'Social Security Number',
'Date',
'Notarized (Yes or No)11',
'Notarized Date12',
'Signature by Spouse/Derivative Party',
'Printed Name13',
'Signed (Yes or No)14',
'Social Security Number15',
'Date of Birth',
'Relationship to Claimant',
'Date16',
'Notarized (Yes or No)17',
'Notarized Date18',
'Amendment to Confidential Release',
'Signed (Yes or No)19',
'Signature Date20',
'Notarized (Yes or No)21',
'Notarized Date22',
'Estate Documents[Death Certificate]',
'Estate Documents[Last Will and Testament]',
'Estate Documents[Trust Documents]',
'Estate Documents[Probate Documents]',
'Additional Notes and Comments'
] 

csv_2_ocr_map_enroll = {
'Claimant First Name': {'ocr_key':'First', 'PageNo': 2, 'TopPos': 1}, 
'Claimant Last Name': {'ocr_key':'Last', 'PageNo': 2, 'TopPos': 1},
'City':{'ocr_key':'City', 'PageNo': 2, 'TopPos': 1},
'State':{'ocr_key':'State', 'PageNo': 2, 'TopPos': 1},
'Zip Code':{'ocr_key':'Zip', 'PageNo': 2, 'TopPos': 1},
'Address 1':{'ocr_key':'Street/P.O. B', 'PageNo': 2, 'TopPos': 1},
'Claimant Date of Birth':{'ocr_key':'(Month/Day/Year)', 'PageNo': 3, 'TopPos': 1}
}

csv_2_ocr_map_relfull = {
'Claimant First Name': {'ocr_key':'First', 'PageNo': 3, 'TopPos': 1}, 
'Claimant Last Name': {'ocr_key':'Last', 'PageNo': 3, 'TopPos': 1},
'City':{'ocr_key':'City', 'PageNo': 3, 'TopPos': 1},
'State':{'ocr_key':'State', 'PageNo': 3, 'TopPos': 1},
'Zip Code':{'ocr_key':'Zip', 'PageNo': 3, 'TopPos': 1},
'Address 1':{'ocr_key':'Street/P.O. B', 'PageNo': 3, 'TopPos': 1},
'Claimant Date of Birth':{'ocr_key':'(Month/Day/Year)', 'PageNo': 3, 'TopPos': 1}
}

#csv_2_ocr_map = csv_2_ocr_map_enroll

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
    csv_columns = ['Claimant First Name', 'Claimant Last Name', 'City']
   ## writing to lambda temp area
    print('trying to write file to temp lambda space named: '+csv_file)
    try:
        with open('/tmp/'+docname+'_data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
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



def GetFromTheTopofPage(fieldlist, pos, page):
    print('--- unsorted ---')
#    for field in fieldlist:
#        print('key: ',field.key,' value: ',field.value,' toplocation: ',field.key.geometry.boundingBox.top)

    sorted_field = sorted(fieldlist, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)

#    print('--- sorted ---')
#    for field in sorted_field:
#        print('key: ',field.key,' value: ',field.value,' toplocation: ',field.key.geometry.boundingBox.top)
    
#    print('first one is',sorted_field[pos].value)
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
# logic for getting the correct map based on file name
    if(str(docname).find('ENROLL') > -1):
        csv_2_ocr_map = csv_2_ocr_map_enroll
    if(str(docname).find('RELFULL') > -1):
        csv_2_ocr_map = csv_2_ocr_map_relfull

# End logic for getting the correct map based on file name
#    printresponsetos3(doc)
    all_keys = []
    all_values = []
    pageno = 0
    dictrow = {}
#   building the array of KVP
    for page in doc.pages:
        pageno = pageno + 1
#        print('---- page ',str(pageno),' ----',)
        pagelines = page.lines
        for line in page.lines:

#        print(type(pagelines))
#        print(len(pagelines))
#        print('--- printing page lines ---')
#        print(pagelines)
#        print('--- did it print a page line number?')
        for csv_key in csv_2_ocr_map:    # Getting the keys to build up a row
3            print('Looking for csv_key is: ',csv_key,' | ocr key: ', csv_2_ocr_map[csv_key]['ocr_key'],' | at TopPos: ', str(csv_2_ocr_map[csv_key]['TopPos']),' on Page: ',str(pageno)) 
            #),str(csv_2_orc_map[csv_key]['TopPos']) )
            es = filter(lambda x: str(x.key).startswith(str(csv_2_ocr_map[csv_key]['ocr_key'])) and  csv_2_ocr_map[csv_key]['PageNo'] == pageno ,page.form.fields) 
#            selections = filter(lambda x: str(x.key).startswith(str(csv_2_ocr_map[csv_key]['ocr_key'])) and  csv_2_ocr_map[csv_key]['PageNo'] == pageno ,page.form.fields) 

            lFields = list(es)
#            print(f"i found {str(len(lFields))} field objects")
            if(len(lFields)>0):
                correctField = GetFromTheTopofPage(lFields,0,2)
                dictrow[csv_key] = correctField.value
#                print(f'--- the csv key is: {csv_key}  the correctField is {correctField.value}')
            else:
#                print(' --- no correctField found --- ')
            #print(f'write a cell to column: {csv_key} with value: {correctField.value}')
#        print(f'---------------- print dictrow afterpage {pageno} is processed ----------')
#        print(dictrow)
    all_values.append(dictrow)

    save_orc_to_bucket(all_values, 'testeric')
    printSections(doc)
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
def printSections(doc):
    print('trying to print out SelectionElement:')
    for page in doc.pages:
        for field in page.form.fields:
            if str(field.value) == 'SELECTED':
                print('checkbox: '+str(field.key)+' is: '+str(field.value))
                print(' with confidence: '+str(field.key.confidence))
                print ('block: '+str(field.key.block))

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



