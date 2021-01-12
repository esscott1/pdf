import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re


csv_2_ocr_map_enroll = {
'Claimant First Name': {'ocr_key':'First', 'PageNo': 2, 'TopPos': 1}, 
'Claimant Last Name': {'ocr_key':'Last', 'PageNo': 2, 'TopPos': 1},
'City':{'ocr_key':'City', 'PageNo': 2, 'TopPos': 1},
'State':{'ocr_key':'State', 'PageNo': 2, 'TopPos': 1},
'Zip Code':{'ocr_key':'Zip', 'PageNo': 2, 'TopPos': 1},
'Address 1':{'ocr_key':'Street/P.O. B', 'PageNo': 2, 'TopPos': 1},
'Claimant Date of Birth':{'ocr_key':'(Month/Day/Year)', 'PageNo': 2, 'TopPos': 1},
'Current Citizenship Status_YES':{'ocr_key':'YES','PageNo': 2, 'TopPos': 1},
'Current Citizenship Status_NO':{'ocr_key': 'NO', 'PageNo': 2, 'TopPos': 1},
'Citizenship Status at time of Exposure_YES':{'ocr_key':'YES','PageNo':2, 'TopPos': 2},
'Citizenship Status at time of Exposure_NO':{'ocr_key':'NO','PageNo':2, 'TopPos': 2},
'Were you married_YES':{'ocr_key': 'YES', 'PageNo': 3, 'TopPos':1},
'Were you married_NO':{'ocr_key': 'NO', 'PageNo': 3, 'TopPos':1}

}
db_csv_2_ocr_map_enroll2 = {}

db_csv_2_ocr_map_enroll = {
'Claimant_First_Name': {'ocr_key':'First', 'PageNo': 2, 'TopPos': 1}, 
'Claimant_Last_Name': {'ocr_key':'Last', 'PageNo': 2, 'TopPos': 1},
'City':{'ocr_key':'City', 'PageNo': 2, 'TopPos': 1},
'State':{'ocr_key':'State', 'PageNo': 2, 'TopPos': 1},
'Zip_Code':{'ocr_key':'Zip', 'PageNo': 2, 'TopPos': 1},
'Address_1':{'ocr_key':'Street/P.O. B', 'PageNo': 2, 'TopPos': 1},
'Claimant_Date_of_Birth':{'ocr_key':'(Month/Day/Year)', 'PageNo': 2, 'TopPos': 1},
'Current_Citizenship_Status_YES':{'ocr_key':'YES','PageNo': 2, 'TopPos': 1},
'Current_Citizenship_Status_NO':{'ocr_key': 'NO', 'PageNo': 2, 'TopPos': 1},
'Citizenship_Status_at_time_of_Exposure_YES':{'ocr_key':'YES','PageNo':2, 'TopPos': 2},
'Citizenship_Status_at_time_of_Exposure_NO':{'ocr_key':'NO','PageNo':2, 'TopPos': 2},
'Were_you_married_YES':{'ocr_key': 'YES', 'PageNo': 3, 'TopPos':1},
'Were_you_married_NO':{'ocr_key': 'NO', 'PageNo': 3, 'TopPos':1},
'Claimant_Social_Security_Number':{'ocr_key': 'ssn', 'PageNo': 2, 'TopPos':1}

}

db_csv_2_ocr_map_afft = {
'afft_Address': {'ocr_key':'Address', 'PageNo': 1, 'TopPos': 1},
'afft_City_state_zip': {'ocr_key':'City', 'PageNo': 1, 'TopPos': 1},
'afft_ssn':{'ocr_key':'My Social Security', 'PageNo': 1, 'TopPos': 1},
'afft_dob':{'ocr_key':'My Date of Birth', 'PageNo': 1, 'TopPos': 1}

}

csv_2_ocr_map_relfull = {
'Claimant First Name': {'ocr_key':'First', 'PageNo': 3, 'TopPos': 1}, 
'Claimant Last Name': {'ocr_key':'Last', 'PageNo': 3, 'TopPos': 1},
'City':{'ocr_key':'City', 'PageNo': 3, 'TopPos': 1},
'State':{'ocr_key':'State', 'PageNo': 3, 'TopPos': 1},
'Zip Code':{'ocr_key':'Zip', 'PageNo': 3, 'TopPos': 1},
'Address 1':{'ocr_key':'Street/P.O. B', 'PageNo': 3, 'TopPos': 1},
'Claimant Date of Birth':{'ocr_key':'(Month/Day/Year)', 'PageNo': 3, 'TopPos': 1},
'Current Citizenship Status':{'ocr_key':'YES','PageNo': 3, 'TopPos': 1},
'Citizenship Status at time of Exposure':{'ocr_key':'YES','PageNo':3, 'TopPos': 2}
}

#csv_2_ocr_map = csv_2_ocr_map_enroll

def CollapeYESNO(dict):
    ca_Current_Citizenship_Status_Yes = dict.pop('ca_Current_Citizenship_Status_YES', None)
    ca_Current_Citizenship_Status_No = dict.pop('ca_Current_Citizenship_Status_NO', None)

    Current_Citizenship_Status_Yes = dict.pop('Current_Citizenship_Status_YES', None)
    Current_Citizenship_Status_No = dict.pop('Current_Citizenship_Status_NO', None)
    if(str(Current_Citizenship_Status_Yes) == 'SELECTED'):
        dict["Current_Citizenship_Status"] = 'YES'
    if(str(Current_Citizenship_Status_No) == 'SELECTED'):
        dict["Current_Citizenship_Status"] = 'NO'

    ca_Citizenship_Status_at_time_of_Exposure_YES = dict.pop('ca_Citizenship_Status_at_time_of_Exposure_YES', None)
    ca_Citizenship_Status_at_time_of_Exposure_NO = dict.pop('ca_Citizenship_Status_at_time_of_Exposure_NO', None)
    Citizenship_Status_at_time_of_Exposure_YES = dict.pop('Citizenship_Status_at_time_of_Exposure_YES', None)
    Citizenship_Status_at_time_of_Exposure_NO = dict.pop('Citizenship_Status_at_time_of_Exposure_NO', None)
    if(str(Citizenship_Status_at_time_of_Exposure_YES) == 'SELECTED'):
        dict["Citizenship_Status_at_time_of_Exposure"] = 'YES'
    if(str(Citizenship_Status_at_time_of_Exposure_NO) == 'SELECTED'):
        dict["Citizenship_Status_at_time_of_Exposure"] = 'NO'

    ca_Were_you_married_YES = dict.pop('ca_Were_you_married_YES', None)
    ca_Were_you_married_NO = dict.pop('ca_Were_you_married_NO', None)
    Were_you_married_YES = dict.pop('Were_you_married_YES', None)
    Were_you_married_NO = dict.pop('Were_you_married_NO', None)
    if(str(Were_you_married_YES) == 'SELECTED'):
        dict["Were_you_married_at_any_time_from_the_date_of_your_initial"] = 'YES'
    if(str(Were_you_married_NO) == 'SELECTED'):
        dict["Were_you_married_at_any_time_from_the_date_of_your_initial"] = 'NO'

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

def read_config():
    bucket = 'archer-ocr-doc-bucket'
    s3 = boto3.client('s3')
    key = 'ocr_config.json'

    try:
        response = s3.get_object(Bucket = bucket, Key = key)
        content = response['Body']
        ocr_config_json = json.loads(content.read())
        ocr_maps = ocr_config_json["ocr_maps"]
        print('--- printing config info ---')

        print(ocr_config_json)
        print('--- printing ocr maps ---')
        print(ocr_maps)
        print('---  ocr enroll map ---')
        print(ocr_maps['db_csv_2_ocr_map_enroll'])
        db_csv_2_ocr_map_enroll2 = ocr_maps['db_csv_2_ocr_map_enroll']

    except Exception as e:
        print('error reading json config')
        print(e)

'''
def save_orc_to_bucket(all_values, docname):
    csv_file='/tmp/'+docname+'_data.csv'
#    csv_columns = ['Claimant First Name', 'Claimant Last Name', 'City']
   ## writing to lambda temp area
    print('trying to write file to temp lambda space named: '+csv_file)
    try:
        with open('/tmp/'+docname+'_data.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=db_csv_headers)
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

def write_dict_to_db(mydict, connection):
    """
    Write dictionary to the table name provided in the SAM deployment statement as lambda environment variable.
    """
    DBTable = os.environ.get('TableName')
    cursor = connection.cursor()
    placeholders = ', '.join(['%s'] * len(mydict))
    columns = ', '.join(mydict.keys())
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (DBTable, columns, placeholders)
    
    fieldtextlist = []
    fieldvaluelist =  list(mydict.values())
    for fieldvalue in fieldvaluelist:
        fieldtextlist.append(str(fieldvalue))

    print(sql, fieldvaluelist)
    print(fieldtextlist)
    cursor.execute(sql, fieldtextlist)

    connection.commit()
    cursor.close()


def CleanDate(dateFieldValue):
    datestring = str(dateFieldValue)
    cleanDateResult = 'Trouble Reading, see PDF'
    ca_cleanDateResult = '0'
    print(f'--- trying to clean: {datestring}----')
    try:
        parsed_date = parse(datestring)
        cleanDateResult = parsed_date.strftime('%m/%d/%Y')
        print(f'----Cleaned date to: {cleanDateResult}')
        ca_cleanDateResult = dateFieldValue.content[0].confidence
    except Exception as e:
        print(f'error cleaning date string provided {datestring}:  error: {e}')

    listCleanDateResult = [cleanDateResult, ca_cleanDateResult]
    return listCleanDateResult


def GetFromTheTopofPage(fieldlist, pos, page):
    sorted_field = sorted(fieldlist, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
    return sorted_field[pos]

def writetosnstopic(claimantname):
    sns = boto3.client('sns')
    
    response = sns.publish(
        TopicArn = 'arn:aws:sns:us-west-2:021025786029:ARCHERClaimantSNSTopicSNSTopic',
        Message=f'test from lambda,i have a new claimant named  {claimantname}  - sent from writer lambda',)
    print(response)


def lambda_handler(event, context):
    """
    Get Extraction Status, JobTag and JobId from SNS. 
    If the Status is SUCCEEDED then create a dict of the values and write those to the RDS database.
    """

    read_config()
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
        csv_2_ocr_map = db_csv_2_ocr_map_enroll2
    if(str(docname).find('RELFULL') > -1):
        csv_2_ocr_map = csv_2_ocr_map_relfull
    if(str(docname).find('AFFT') > -1):
        csv_2_ocr_map = db_csv_2_ocr_map_afft

# End logic for getting the correct map based on file name
#    printresponsetos3(doc)
    all_keys = []
    all_values = []
    pageno = 0
    dictrow = {}
    dictrow['SourceFileName'] = docname
    print(f'docname type is: {type(docname)}')
    dictrow['archer_id'] = docname[0:11]
    ssn = ''
    ca_ssn = ''
    regex = re.compile('-..-')
#   building the array of KVP
    for page in doc.pages:
        pageno = pageno + 1
        print('---- page ',str(pageno),' ----',)

        pagelines = page.lines
        lineNo = -1
        for line in page.lines:
            lineNo += 1
#            for word in line.words:  # update to read CSV and pull page number to avoid dups.
#                if re.search('-..-',str(word)):
#                    print(f'--- found -??- in word: {word} on line {lineNo}')
#                    print(f'the line {lineNo} is: {page.lines[lineNo]}')
#                    ssn = str(word)
#                    ca_ssn = word.confidence

#        print(type(pagelines))
#        print(len(pagelines))
#        print('--- printing page lines ---')
#        print(pagelines)
#        print('--- did it print a page line number?')
        for csv_key in csv_2_ocr_map:    # Getting the keys to build up a row
            print('Looking for csv_key is: ',csv_key,' | ocr key: ', csv_2_ocr_map[csv_key]['ocr_key'],' | at TopPos: ', str(csv_2_ocr_map[csv_key]['TopPos']),' on Page: ',str(pageno))

            es = filter(lambda x: str(x.key).startswith(str(csv_2_ocr_map[csv_key]['ocr_key'])) and  csv_2_ocr_map[csv_key]['PageNo'] == pageno ,page.form.fields) 
#            selections = filter(lambda x: str(x.key).startswith(str(csv_2_ocr_map[csv_key]['ocr_key'])) and  csv_2_ocr_map[csv_key]['PageNo'] == pageno ,page.form.fields) 

            lFields = list(es)
            print(f"i found {str(len(lFields))} field objects")
            if(len(lFields)>0):
                correctField = GetFromTheTopofPage(lFields,0,2)
                ca_csv_key = 'ca_'+csv_key
                print(f'value of correctField is: {correctField.value}')
                if(csv_key) == 'Claimant_Date_of_Birth':
                    print('trying to clean date')
                    ListcleanDOB = CleanDate(correctField.value)
                    cleanDOB = ListcleanDOB[0]
                    dictrow[csv_key] = cleanDOB
                    dictrow[ca_csv_key] = ListcleanDOB[1]
                else:
                    dictrow[csv_key] = str(correctField.value)
#                    ca_csv_key = 'ca_'+csv_key
                    try:
                        dictrow[ca_csv_key] = str(correctField.value.content[0].confidence)
                    except Exception as e:
                        dictrow[ca_csv_key] = 'error getting confidence, see PDF'
                        print(f'error getting confidence: {e}')

    dictrow['Claimant_Social_Security_Number'] = ssn
    dictrow['ca_Claimant_Social_Security_Number'] = ca_ssn


    CollapeYESNO(dictrow)
    print('--- printing dictrow ---')
    print(dictrow)
    try:
        print('tring to dumps and print json')
        json_object = json.dumps(dictrow, indent = 2)
        print(json_object)
    except Exception as e:
        print(f'--- error print json: {e}')
        #dictrow['jsondata']=json_object

    dictrow['jsondata'] = json_object
    all_values.append(dictrow)
    try:
        writetosnstopic(dictrow['Claimant_Social_Security_Number'])
    except Exception as e:
        print('failed to write to custom SNS Topic, need to update yaml to push it correct with permissions')
        print(e)

#    save_orc_to_bucket(all_values, 'testeric')
    connection = get_connection()
    for dictionary in all_values:
        print('writing this to DB')
        print(dictionary)
        write_dict_to_db(dictionary, connection)
    print('trying to read config')



#    print(dictrow)
#    printSections(doc)

def printSections(doc):
    print('trying to print out SelectionElement:')
    for page in doc.pages:
        for field in page.form.fields:
            if str(field.value) == 'SELECTED':
                print('checkbox: '+str(field.key)+' is: '+str(field.value))
                print(' with confidence: '+str(field.key.confidence))
                print ('block: '+str(field.key.block))





