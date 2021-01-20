import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re




#  taking YES and NO and collaping into a single DB field
def CollapeYESNO(dict):
    ca_Current_Citizenship_Status_Yes = dict.pop('ca_Current_Citizenship_Status_YES', None)
    ca_Current_Citizenship_Status_No = dict.pop('ca_Current_Citizenship_Status_NO', None)

    Current_Citizenship_Status_Yes = dict.pop('Current_Citizenship_Status_YES', None)
    Current_Citizenship_Status_No = dict.pop('Current_Citizenship_Status_NO', None)
    if(str(Current_Citizenship_Status_Yes) == 'YES'):
        dict["Current_Citizenship_Status"] = 'YES'
    if(str(Current_Citizenship_Status_No) == 'YES'):
        dict["Current_Citizenship_Status"] = 'NO'

    ca_Citizenship_Status_at_time_of_Exposure_YES = dict.pop('ca_Citizenship_Status_at_time_of_Exposure_YES', None)
    ca_Citizenship_Status_at_time_of_Exposure_NO = dict.pop('ca_Citizenship_Status_at_time_of_Exposure_NO', None)
    Citizenship_Status_at_time_of_Exposure_YES = dict.pop('Citizenship_Status_at_time_of_Exposure_YES', None)
    Citizenship_Status_at_time_of_Exposure_NO = dict.pop('Citizenship_Status_at_time_of_Exposure_NO', None)
    if(str(Citizenship_Status_at_time_of_Exposure_YES) == 'YES'):
        dict["Citizenship_Status_at_time_of_Exposure"] = 'YES'
    if(str(Citizenship_Status_at_time_of_Exposure_NO) == 'YES'):
        dict["Citizenship_Status_at_time_of_Exposure"] = 'NO'

    ca_Were_you_married_YES = dict.pop('ca_Were_you_married_YES', None)
    ca_Were_you_married_NO = dict.pop('ca_Were_you_married_NO', None)
    Were_you_married_YES = dict.pop('Were_you_married_YES', None)
    Were_you_married_NO = dict.pop('Were_you_married_NO', None)
    if(str(Were_you_married_YES) == 'YES'):
        dict["Were_you_married_at_any_time_from_the_date_of_your_initial"] = 'YES'
    if(str(Were_you_married_NO) == 'YES'):
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
    global db_csv_2_ocr_map_enroll
    global db_csv_2_ocr_map_afft
    global csv_2_ocr_map_relfull
    global db_csv_2_ocr_map_lygdaa
    global ocr_config_json
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
        db_csv_2_ocr_map_enroll = ocr_maps['db_csv_2_ocr_map_enroll']
        print('--- relful map ---')
        csv_2_ocr_map_relfull = ocr_maps['csv_2_ocr_map_relfull']
        print(csv_2_ocr_map_relfull)
        db_csv_2_ocr_map_afft = ocr_maps['db_csv_2_ocr_map_afft']
        print('----  affidatite map ---')
        print(db_csv_2_ocr_map_afft)
        db_csv_2_ocr_map_lygdaa = ocr_maps['db_csv_2_ocr_map_lygdaa']
        print('----  lygdaa Dallas map ---')
        print(db_csv_2_ocr_map_lygdaa)

    except Exception as e:
        print('error reading json config')
        print(e)



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
    #pulling confidence but not using.. might want to use to help determine the appropriate date to return
    return cleanDateResult


def GetFromTheTopofPage(fieldlist, pos, page):
    sorted_field = sorted(fieldlist, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
    return sorted_field[pos]

def writetosnstopic(claimantname):
    sns = boto3.client('sns')
    
    response = sns.publish(
        TopicArn = 'arn:aws:sns:us-west-2:021025786029:ARCHERClaimantSNSTopicSNSTopic',
        Message=f'test from lambda,i have a new claimant named  {claimantname}  - sent from writer lambda',)
    print(response)

def get_csv_2_ocr_map(docname):
    # logic for getting the correct map based on file name
    if(str(docname).find('ENROLL') > -1):
        csv_2_ocr_map = db_csv_2_ocr_map_enroll
    if(str(docname).find('RELFULL') > -1):
        csv_2_ocr_map = csv_2_ocr_map_relfull
    if(str(docname).find('AFFT') > -1):
        csv_2_ocr_map = db_csv_2_ocr_map_afft
        print(f'it is an AFFT doc so using: {db_csv_2_ocr_map_afft}')
    if(str(docname).find('LYGDAA') > -1):
        csv_2_ocr_map = db_csv_2_ocr_map_lygdaa
        print(f'it is an LYGDAA doc from Dallas docket so using: {db_csv_2_ocr_map_lygdaa}')
    print(f'the csv_2_ocr_map is: {csv_2_ocr_map}')
    return csv_2_ocr_map


def CleanSelectionFieldValueToStr(value, valueType):
    result = ''
    if(value != None):
        if(str(valueType) == 'Selection'):
            result = 'NO'
            print(f'value passed into clean method is {value}')
            if(str(value) == 'SELECTED'):
                result = 'YES'
        elif(str(valueType) == 'Date'):
            result = CleanDate(value)
        else:
            result = str(value)
    return result
    
def process_ocr_form(csv_2_ocr_map, csv_key, dictrow, pageno, page):

    es = filter(lambda x: str(csv_2_ocr_map[csv_key]['ocr'][0]['ocr_key']) in str(x.key) and  csv_2_ocr_map[csv_key]['ocr'][0]['PageNo'] == pageno ,page.form.fields) 
    lFields = list(es)
    print(f"i found {str(len(lFields))} field objects")
    if(len(lFields)>0):
        tpos = csv_2_ocr_map[csv_key]['ocr'][0]['TopPos']
        print(f'looking for position: {tpos}')
        correctField = None
        correctCleanValueStr = ''
        ca_csv_key = 'ca_'+csv_key
        # could add check to ensure the number returned is equal or more than the TopPos looking for.  else error will occur.
        sorted_field = sorted(lFields, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
        correctField = sorted_field[csv_2_ocr_map[csv_key]['ocr'][0]['TopPos']-1]
        correctCleanValueStr = CleanSelectionFieldValueToStr(correctField.value, csv_2_ocr_map[csv_key]['ocr'][0]['Type'])

        dictrow[csv_key] = correctCleanValueStr

        if(correctField != None and correctField.value != None):
            dictrow[ca_csv_key] = str(correctField.value.content[0].confidence)
        else:
            dictrow[ca_csv_key] = '0'
    return dictrow

def process_ocr_yesno(csv_2_ocr_map, csv_key, dictrow, pageno, page):
    es_0 = filter(lambda  x: str(csv_2_ocr_map[csv_key]['ocr'][0]['ocr_key']) in str(x.key) and  csv_2_ocr_map[csv_key]['ocr'][0]['PageNo'] == pageno ,page.form.fields)
    es_1 = filter(lambda  x: str(csv_2_ocr_map[csv_key]['ocr'][1]['ocr_key']) in str(x.key) and  csv_2_ocr_map[csv_key]['ocr'][1]['PageNo'] == pageno ,page.form.fields)
    correctField0 = None
    correctField1 = None
    lFields0 = list(es_0)
    lFields1 = list(es_1)
    if(len(lFields0) != 0 and len(lFields1) != 0):
    #if(len(lFields0)>0):
        print(f"found {len(lFields0)} in ocr_key {str(csv_2_ocr_map[csv_key]['ocr'][0]['ocr_key'])}")
        print(f"found {len(lFields1)} in ocr_key {str(csv_2_ocr_map[csv_key]['ocr'][1]['ocr_key'])}")
        sf0 = sorted(lFields0, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
        correctField0 = sf0[csv_2_ocr_map[csv_key]['ocr'][0]['TopPos']-1]
    #if(len(lFields1)>0):
        sf1 = sorted(lFields1, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
        correctField1 = sf1[csv_2_ocr_map[csv_key]['ocr'][1]['TopPos']-1]
        correctCleanValueStr0 = CleanSelectionFieldValueToStr(correctField0.value,csv_2_ocr_map[csv_key]['ocr'][0]["Type"])
        correctCleanValueStr1 = CleanSelectionFieldValueToStr(correctField1.value,csv_2_ocr_map[csv_key]['ocr'][1]["Type"])
        print(f'for csv_key: {csv_key} the correctCleanValueStr0 is: {correctCleanValueStr0}')
        print(f'for csv_key: {csv_key} the correctCleanValueStr1 is: {correctCleanValueStr1}')
        if(str(csv_2_ocr_map[csv_key]['ocr'][0]['ocr_key']) == 'YES'): # know that index 0 is for YES and index 1 is for NO
            if(correctCleanValueStr0 == 'YES' and correctCleanValueStr1 == 'YES'):
                dictrow[csv_key] = 'YES and NO'
            elif(correctCleanValueStr1 == 'YES'):
                dictrow[csv_key] = 'NO'
            elif(correctCleanValueStr0 == 'YES'):
                dictrow[csv_key] = 'YES'
            else:
                dictrow[csv_key] = ''

    return dictrow

def get_tablename(docname):
    result = ''
    tablefilemaps = ocr_config_json["ocr_table_file_maps"]
    print('---  tablefilemaps ---')
    print(tablefilemaps)
    for map in tablefilemaps:
        rex = map["fileregex"]
        if(str(docname).find(str(rex)) > -1):
            result = str(map["table"])
    return result


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


    docname = pdfTextExtractionDocLoc['S3ObjectName']
    csv_2_ocr_map = get_csv_2_ocr_map(docname)
    tablename = get_tablename(docname)

    print('document name is: '+docname)
    print(f'content of document should print to table name: {tablename}')
    if(pdfTextExtractionStatus == 'SUCCEEDED'):
        response = getJobResults(pdfTextExtractionJobId)
        doc = Document(response)

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

        lineNo = -1
#        for line in page.lines:
#            lineNo += 1
#            for word in line.words:  # update to read CSV and pull page number to avoid dups.
#                if re.search('-..-',str(word)):
#                    print(f'--- found -??- in word: {word} on line {lineNo}')
#                    print(f'the line {lineNo} is: {page.lines[lineNo]}')
#                    ssn = str(word)
#                    ca_ssn = word.confidence

        for csv_key in csv_2_ocr_map:    # Getting the keys to build up a row
            if(csv_2_ocr_map[csv_key]["Type"] == 'Form'):
                print(f'looking for csv_key: {csv_key}')
                dictrow = process_ocr_form(csv_2_ocr_map, csv_key, dictrow, pageno, page)
            if(csv_2_ocr_map[csv_key]["Type"] == 'YesNo'):
                print(f'looking for csv_key: {csv_key}')
                dictrow = process_ocr_yesno(csv_2_ocr_map, csv_key, dictrow, pageno, page)
#    dictrow['Claimant_Social_Security_Number'] = ssn
#    dictrow['ca_Claimant_Social_Security_Number'] = ca_ssn


    #CollapeYESNO(dictrow)
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
    #try:
    #    writetosnstopic(dictrow['Claimant_Social_Security_Number'])
    #except Exception as e:
    #    print('failed to write to custom SNS Topic, need to update yaml to push it correct with permissions')
    #    print(e)

#    save_ocr_to_bucket(all_values, 'testeric')
    connection = get_connection()
    for dictionary in all_values:
        print('writing this to DB')
        print(dictionary)
        write_dict_to_db(dictionary, connection)
    print('trying to read config')



#    print(dictrow)
#    printSections(doc)

#def printSections(doc):
#    print('trying to print out SelectionElement:')
#    for page in doc.pages:
#        for field in page.form.fields:
#            if str(field.value) == 'SELECTED':
#                print('checkbox: '+str(field.key)+' is: '+str(field.value))
#                print(' with confidence: '+str(field.key.confidence))
#                print ('block: '+str(field.key.block))


def convert_row_to_list(row):
    list_of_cells = [cell.text.strip() for cell in row.cells]
    return list_of_cells



