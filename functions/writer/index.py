import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re


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

'''
Returns a dictionanry of KVP that are in the json config file
'''
def read_config():

    ocr_config_json = {}
    bucket = 'archer-ocr-doc-bucket'
    s3 = boto3.client('s3')
    key = 'ocr_config.json'
    try:
        response = s3.get_object(Bucket = bucket, Key = key)
        content = response['Body']
        ocr_config_json = json.loads(content.read())
        return ocr_config_json
    except Exception as e:
        print('error reading json config')
        print(e)



def write_dict_to_db(mydict, connection, tablename):
    """
    Write dictionary to the table name provided in the SAM deployment statement as lambda environment variable.
    """
    #DBTable = os.environ.get('TableName')
    DBTable = tablename
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

def get_csv_2_ocr_map(docname,configDict, prefixName):
    # logic for getting the correct map based on file name
    result = {}
    for snippet in configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"]:
        if(str(docname).find(snippet) > -1):
            print(f'map should be {configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"][snippet]}')
            omap = configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"][snippet]
            print(f'map should by {configDict["ocr_maps"][omap]}')
            result = configDict["ocr_maps"][omap]
    return result


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

def get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, itemNo):
    result = None
    es = filter(lambda x: str(csv_2_ocr_map[csv_key]['ocr'][itemNo]['ocr_key']) in str(x.key) and  csv_2_ocr_map[csv_key]['ocr'][itemNo]['PageNo'] == pageno ,page.form.fields) 
    lFields = list(es)
    print(f"i found {str(len(lFields))} field objects")
    if(len(lFields)>0):
        tpos = csv_2_ocr_map[csv_key]['ocr'][itemNo]['TopPos']
        print(f'looking for position: {tpos}')
        # could add check to ensure the number returned is equal or more than the TopPos looking for.  else error will occur.
        sorted_field = sorted(lFields, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
        result = sorted_field[csv_2_ocr_map[csv_key]['ocr'][itemNo]['TopPos']-1]
    return result

    
def process_ocr_form(csv_2_ocr_map, csv_key, dictrow, pageno, page):

    print(f'in the process_ocr_form method csv_2_ocr_map is {csv_2_ocr_map}')
    correctField = get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, 0)
    correctCleanValueStr = CleanSelectionFieldValueToStr(correctField.value, csv_2_ocr_map[csv_key]['ocr'][0]['Type'])
    dictrow[csv_key] = correctCleanValueStr
    ca_csv_key = 'ca_'+csv_key
    if(correctField != None and correctField.value != None):
        dictrow[ca_csv_key] = str(correctField.value.content[0].confidence)
    else:
        dictrow[ca_csv_key] = '0'
    return dictrow

def process_ocr_yesno(csv_2_ocr_map, csv_key, dictrow, pageno, page):
    '''
    Won't work if the yes and no are split between 2 different pages.
    '''
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

def lambda_handler(event, context):
    """
    Get Extraction Status, JobTag and JobId from SNS. 
    If the Status is SUCCEEDED then create a dict of the values and write those to the RDS database.
    """

    configDict = read_config()
    notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    
    pdfTextExtractionStatus = json.loads(notificationMessage)['Status']
    pdfTextExtractionJobTag = json.loads(notificationMessage)['JobTag']
    pdfTextExtractionJobId = json.loads(notificationMessage)['JobId']
    pdfTextExtractionDocLoc = json.loads(notificationMessage)['DocumentLocation']

    print(pdfTextExtractionJobTag + ' : ' + pdfTextExtractionStatus)
    print(f'----  Document Location ----')
    print(pdfTextExtractionDocLoc)
    print(f'----  Job Tag ----')
    print(pdfTextExtractionJobTag)
    docname = pdfTextExtractionDocLoc['S3ObjectName']
    prefixName = docname[0:docname.find('/')]
    print(f'prefix name is {prefixName}')
    tablename = configDict["s3_prefix_table_map"][prefixName]["table"]
    print(f'---- table name from config Dict is: {tablename} ----')

    csv_2_ocr_map = get_csv_2_ocr_map(docname, configDict, prefixName)
    print(csv_2_ocr_map)
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
        print('---- printing the csv_2_ocr_map again ---')
        print(csv_2_ocr_map)
        for csv_key in csv_2_ocr_map:    # Getting the keys to build up a row
            if(csv_2_ocr_map[csv_key]["Type"] == 'Form' and str(csv_2_ocr_map[csv_key]["ocr"][0]["PageNo"]) == str(pageno)):
                print(f'looking for csv_key: {csv_key}')
                dictrow = process_ocr_form(csv_2_ocr_map, csv_key, dictrow, pageno, page)
            if(csv_2_ocr_map[csv_key]["Type"] == 'YesNo'):
                print(f'looking for csv_key: {csv_key}' and str(csv_2_ocr_map[csv_key]["ocr"][0]["PageNo"]) == str(pageno))
                # method below doesn't work if yes / no boxes are split between pages, so only looking at first object in array.  should enhance
                dictrow = process_ocr_yesno(csv_2_ocr_map, csv_key, dictrow, pageno, page)
#    dictrow['Claimant_Social_Security_Number'] = ssn
#    dictrow['ca_Claimant_Social_Security_Number'] = ca_ssn


    print('--- printing dictrow ---')
    print(dictrow)
    try:
    #    print('tring to dumps and print json')
        json_object = json.dumps(dictrow, indent = 2)
    #    print(json_object)
    except Exception as e:
        print(f'--- error print json: {e}')
    #    #dictrow['jsondata']=json_object

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
        write_dict_to_db(dictionary, connection, tablename)
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



