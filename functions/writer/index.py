import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re
import traceback

debug, snsnotify = '',''

def read_config():

    configFileBucket = os.environ.get('ConfigBucket')
    configFileKey = os.environ.get('ConfigKey')
    print(f'read ConfigBucket as: {configFileBucket} and ConfigKey as {configFileKey}')
    ocr_config_json = {}
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket = configFileBucket, Key = configFileKey)
        content = response['Body']
        ocr_config_json = json.loads(content.read())
        global debug
        debug = ocr_config_json['logginglevel']
        print(f'read config has debug as: {debug}')
        global snsnotify
        snsnotify = ocr_config_json['snsnotification']
        print(f'read config has snsnotify as: {snsnotify}')
        return ocr_config_json
    except:
        traceback.print_exc()

def eprint(msg, sev=10, sendsns=True):
    '''
    default is debug or notset unless correctly specified in config
    sev options are: critical: 50 | error: 40 | warning: 30 | info: 20 | debug: 10 | notset: 0 
    '''
    global debug
    print(f'debug in eprint is: {debug}')
    if debug.lower() not in {'critical', 'error', 'warning', 'info', 'debug'}:
        print(f'debug in config file set to something other than "critical", "error", "warning", "info" or "debug" therefore the setting will be "debug".')
        debug = 'debug'
    loglevel = 10 if debug == 'debug' else 20 if debug == 'info' else 30 if debug == 'warning' else 40 if debug == 'error' else 50 if debug == 'critical' else 0
    if(sev >= loglevel):
        print(msg)
    if(sendsns):
        writetosnstopic(msg, sev)


def writetosnstopic(msg, sev=10):
    '''
    default off unless turned on by correct config
    '''
    eprint('--- trying to write to SNS topic ---',10,False)
    global snsnotify
    eprint(f'snsnotify in writetosnstopic is: {snsnotify}',10,False)
    if snsnotify.lower() not in {'critical', 'error', 'warning', 'info', 'debug', 'off')):
        eprint(f'snsnotification in config file set to something other than "critical", "error", "warning", "info" or "debug" therefore the setting will be "error".',10,False)
        snsnotify = 'error'
    snslevel = 10 if snsnotify == 'debug' else 20 if snsnotify == 'info' else 30 if snsnotify == 'warning' else 40 if snsnotify == 'error' else 50 if snsnotify == 'critical' else 0
    if(sev >= snslevel):
        try:
            sns = boto3.client('sns')
            response = sns.publish(
                TopicArn = 'arn:aws:sns:us-west-2:021025786029:ARCHERClaimantSNSTopic',
                Message=msg,)
        except Exception as e:
            eprint(f'SNS publish ERROR: {e} on lineNo {e.__traceback__.tb_lineno}',40,False)


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
        eprint ('Connecting to database')
        client = boto3.client('rds')
        DBEndPoint = os.environ.get('DBEndPoint')
        DatabaseName = os.environ.get('DatabaseName')
        DBUserName = os.environ.get('DBUserName')
        # Generates an auth token used to connect to a db with IAM credentials.
        eprint (f'trying to get password with auth token for endpoint: {DBEndPoint}')
        password = client.generate_db_auth_token(
            DBHostname=DBEndPoint, Port=5432, DBUsername=DBUserName
        )
        eprint (f'connecting to: {DatabaseName}')
        eprint (f'connecting as: {DBUserName}')
        eprint (f'connecting with pw: {password}')
        # Establishes the connection with the server using the token generated as password
        conn = pg8000.connect(
            host=DBEndPoint,
            user=DBUserName,
            database=DatabaseName,
            password=password,
            ssl={'sslmode': 'verify-full', 'sslrootcert': 'rds-ca-2015-root.pem'},
        )
        eprint ("Succesful connection!")
        return conn
    except Exception as e:
        msg = f'DB connection error: {e} on lineNo: {e.__traceback__.tb_lineno}'
        eprint (msg, 40)
        return None

'''
Returns a dictionanry of KVP that are in the json config file
'''



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

    eprint(f'{sql} and {fieldvaluelist}')
    eprint(fieldtextlist)
    cursor.execute(sql, fieldtextlist)

    connection.commit()
    cursor.close()


def CleanDate(dateFieldValue):
    datestring = str(dateFieldValue)
    cleanDateResult = 'Trouble Reading, see PDF'
    ca_cleanDateResult = '0'
    eprint(f'--- trying to clean: {datestring}----')
    try:
        parsed_date = parse(datestring)
        cleanDateResult = parsed_date.strftime('%m/%d/%Y')
        eprint(f'----Cleaned date to: {cleanDateResult}')
        ca_cleanDateResult = dateFieldValue.content[0].confidence
    except Exception as e:
        msg = f'error cleaning date string provided {datestring}:  error: {e} on lineNo: {e.__traceback__.tb_lineno}'
        eprint(msg, 40)

    listCleanDateResult = [cleanDateResult, ca_cleanDateResult]
    #pulling confidence but not using.. might want to use to help determine the appropriate date to return
    return cleanDateResult




def get_csv_2_ocr_map(docname,configDict, prefixName):
    # logic for getting the correct map based on file name
    result = {}
    for snippet in configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"]:
        if(str(docname).find(snippet) > -1):
            eprint(f'map should be {configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"][snippet]}')
            omap = configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"][snippet]
            eprint(f'map should by {configDict["ocr_maps"][omap]}')
            result = configDict["ocr_maps"][omap]
    return result


def CleanSelectionFieldValueToStr(value, valueType):
    result = ''
    if(value != None):
        if(str(valueType) == 'Selection'):
            result = 'NO'
            eprint(f'value passed into clean method is {value}')
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
    eprint(f"i found {str(len(lFields))} field objects")
    if(len(lFields)>0):
        tpos = csv_2_ocr_map[csv_key]['ocr'][itemNo]['TopPos']
        eprint(f'looking for position: {tpos}')
        # could add check to ensure the number returned is equal or more than the TopPos looking for.  else error will occur.
        sorted_field = sorted(lFields, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
        result = sorted_field[csv_2_ocr_map[csv_key]['ocr'][itemNo]['TopPos']-1]
    return result

    
def process_ocr_form(csv_2_ocr_map, csv_key, dictrow, pageno, page):

    eprint(f'in the process_ocr_form method csv_2_ocr_map is {csv_2_ocr_map}')
    correctField = get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, 0)
    if(correctField != None):
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
    correctField0 = get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, 0)
    correctField1 = get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, 1)
    if(correctField0 != None and correctField1 != None):
    #if(len(lFields1)>0):
        correctCleanValueStr0 = CleanSelectionFieldValueToStr(correctField0.value,csv_2_ocr_map[csv_key]['ocr'][0]["Type"])
        correctCleanValueStr1 = CleanSelectionFieldValueToStr(correctField1.value,csv_2_ocr_map[csv_key]['ocr'][1]["Type"])
        eprint(f'for csv_key: {csv_key} the correctCleanValueStr0 is: {correctCleanValueStr0}')
        eprint(f'for csv_key: {csv_key} the correctCleanValueStr1 is: {correctCleanValueStr1}')
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
    print(f'Debugging is set to: {debug}')
    print(f'SNS notification: {snsnotify}')
    try:
        notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
        
        pdfTextExtractionStatus = json.loads(notificationMessage)['Status']
        pdfTextExtractionJobTag = json.loads(notificationMessage)['JobTag']
        pdfTextExtractionJobId = json.loads(notificationMessage)['JobId']
        pdfTextExtractionDocLoc = json.loads(notificationMessage)['DocumentLocation']

        eprint(pdfTextExtractionJobTag + ' : ' + pdfTextExtractionStatus)
        eprint(f'----  Document Location ----')
        eprint(pdfTextExtractionDocLoc)
        eprint(f'----  Job Tag ----')
        eprint(pdfTextExtractionJobTag)
        docname = pdfTextExtractionDocLoc['S3ObjectName']
        prefixName = docname[0:docname.find('/')]
        eprint(f'prefix name is {prefixName}')
        tablename = configDict["s3_prefix_table_map"][prefixName]["table"]
        eprint(f'---- table name from config Dict is: {tablename} ----')

        csv_2_ocr_map = get_csv_2_ocr_map(docname, configDict, prefixName)
        eprint(csv_2_ocr_map)
        eprint('document name is: '+docname)
        eprint(f'content of document should eprint to table name: {tablename}')
        if(pdfTextExtractionStatus == 'SUCCEEDED'):
            response = getJobResults(pdfTextExtractionJobId)
            doc = Document(response)

    # End logic for getting the correct map based on file name

        all_keys, all_values, pageno, dictrow = [], [], 0, {}
        dictrow['SourceFileName'] = docname
        eprint(f'docname type is: {type(docname)}')
        dictrow['archer_id'] = docname[0:11]
        ssn, ca_ssn = '', ''

        regex = re.compile('-..-')
    #   building the array of KVP
        for page in doc.pages:
            pageno = pageno + 1
            eprint(f'---- page {str(pageno)} ----',)

            lineNo = -1
    #        for line in page.lines:
    #            lineNo += 1
    #            for word in line.words:  # update to read CSV and pull page number to avoid dups.
    #                if re.search('-..-',str(word)):
    #                    eprint(f'--- found -??- in word: {word} on line {lineNo}')
    #                    eprint(f'the line {lineNo} is: {page.lines[lineNo]}')
    #                    ssn = str(word)
    #                    ca_ssn = word.confidence
            eprint('---- eprinting the csv_2_ocr_map again ---')
            eprint(csv_2_ocr_map)
            for csv_key in csv_2_ocr_map:    # Getting the keys to build up a row
                if(csv_2_ocr_map[csv_key]["Type"] == 'Form' and str(csv_2_ocr_map[csv_key]["ocr"][0]["PageNo"]) == str(pageno)):
                    eprint(f'looking for csv_key: {csv_key}')
                    dictrow = process_ocr_form(csv_2_ocr_map, csv_key, dictrow, pageno, page)
                if(csv_2_ocr_map[csv_key]["Type"] == 'YesNo'):
                    eprint(f'looking for csv_key: {csv_key}' and str(csv_2_ocr_map[csv_key]["ocr"][0]["PageNo"]) == str(pageno))
                    # method below doesn't work if yes / no boxes are split between pages, so only looking at first object in array.  should enhance
                    dictrow = process_ocr_yesno(csv_2_ocr_map, csv_key, dictrow, pageno, page)
    #    dictrow['Claimant_Social_Security_Number'] = ssn
    #    dictrow['ca_Claimant_Social_Security_Number'] = ca_ssn

        eprint('--- eprinting dictrow ---')
        eprint(dictrow)
        try:
            dictrow['jsondata'] = json.dumps(dictrow, indent = 2)
        except Exception as e:
            msg = f'error writing jsondata to dictrow, err: {e} on lineNo: {e.__traceback__.tb_lineno}'
            eprint(msg, 40)
        #dictrow['jsondata'] = json_object
        all_values.append(dictrow)

    #    save_ocr_to_bucket(all_values, 'testeric')
        connection = get_connection()
        for dictionary in all_values:
            eprint('writing this to DB')
            eprint(dictionary)
            write_dict_to_db(dictionary, connection, tablename)
            eprint("successfully wrote OCR data for document: "+docname, 10)
    except Exception as e:
        tberror =traceback.print_exc()
        emsg = f'error thrown {e} from line no {e.__traceback__.tb_lineno}'
        print(emsg, "error")





#    eprint(dictrow)
#    eprintSections(doc)

#def eprintSections(doc):
#    eprint('trying to eprint out SelectionElement:')
#    for page in doc.pages:
#        for field in page.form.fields:
#            if str(field.value) == 'SELECTED':
#                eprint('checkbox: '+str(field.key)+' is: '+str(field.value))
#                eprint(' with confidence: '+str(field.key.confidence))
#                eprint ('block: '+str(field.key.block))


def convert_row_to_list(row):
    list_of_cells = [cell.text.strip() for cell in row.cells]
    return list_of_cells



