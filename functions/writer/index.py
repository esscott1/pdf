import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re
import traceback

debug, snsnotify, gDocumentName = '','',''


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
        global snsnotify
        snsnotify = ocr_config_json['snsnotification']
        return ocr_config_json
    except:
        traceback.print_exc()

def eprint(msg, sev=10, sendsns=True):
    '''
    default is debug or notset unless correctly specified in config
    sev options are: critical: 50 | error: 40 | warning: 30 | info: 20 | debug: 10 | verbose: 0 
    '''
    global gDocumentName
    global debug
    if(type(msg) == str):
        msg = "Doc name is: "+gDocumentName +" msg is: "+msg
    if debug.lower() not in {'critical', 'error', 'warning', 'info', 'debug', 'verbose'}:
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
    global snsnotify
    eprint(f'--- trying to write to SNS topic, snsnotify level is set to {snsnotify} ---',0,False)
    if snsnotify.lower() not in {'critical', 'error', 'warning', 'info', 'debug', 'off'}:
        eprint(f'snsnotification in config file set to something other than "critical", "error", "warning", "info" or "debug" therefore the setting will be "error".',0,False)
        snsnotify = 'error'
    snslevel = 10 if snsnotify == 'debug' else 20 if snsnotify == 'info' else 30 if snsnotify == 'warning' else 40 if snsnotify == 'error' else 50 if snsnotify == 'critical' else 0
    sevstring = 'debug' if sev == 10 else 'info' if sev == 20 else 'warning' if sev == 30 else 'error' if sev == 40 else 'critical' if sev ==50 else 'unknown'
    if(sev >= snslevel):
        try:
            sns = boto3.client('sns')
            response = sns.publish(
                TopicArn = 'arn:aws:sns:us-west-2:021025786029:ARCHERClaimantSNSTopic',
                Subject= sevstring+': from OCR Service',
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
    eprint(f'----  textract response status: {response["JobStatus"]} ---',20)
    if(response['JobStatus'] != "SUCCEEDED"):
        eprint(f'----  textract response status: {response["JobStatus"]} message: {response["StatusMessage"]}',40)
        return None
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

    placeholders = ', '.join(['%s'] * len(mydict))
    columns = ', '.join(mydict.keys())
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (DBTable, columns, placeholders)
    
    fieldtextlist = []
    fieldvaluelist =  list(mydict.values())
    for fieldvalue in fieldvaluelist:
        fieldtextlist.append(str(fieldvalue))

    eprint(f'{sql} and {fieldvaluelist}')
    eprint(fieldtextlist)
    try:
        cursor = connection.cursor()
        cursor.execute(sql, fieldtextlist)
        connection.commit()
        cursor.close()
        return 0
    except Exception as e:
        eprint(f'Error writing to Database, error: {e} on lineNo {e.__traceback__.tb_lineno}',40)
        return -1

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
            eprint(f'map should be {configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"][snippet]}',0)
            omap = configDict["s3_prefix_table_map"][prefixName]["filename_ocrmap"][snippet]
            eprint(f'map should by {configDict["ocr_maps"][omap]}',0)
            result = configDict["ocr_maps"][omap]
    return result

def FormatSSN(value):
    ssn = str(value)
    if(ssn != None and re.compile('[0-9]{3}-[0-9]{2}-[0-9]{4}').match(ssn) == None):
        ssn = ssn.replace('.', '')
        ssn = ssn.replace(' ', '')
        ssn = ssn.replace('-', '')
        eprint(f'ssn after replace periods is {ssn}',10)
        eprint(f'length of ssn: {len(ssn)}',10)
        if(len(ssn) == 9):
            ssn = ssn[:5] + '-' + ssn[5:]
            ssn = ssn[:3] + '-' + ssn[3:]
        else:
            ssn = str(value)
    else:
        eprint('SSN was in correct format',10)
    eprint(f'ssn after cleaning is: {ssn}',10)
    return ssn

def CleanSelectionFieldValueToStr(value, valueType):
    result = ''
    if(value != None):
        if(str(valueType) == 'Selection'):
            result = 'NO'
            eprint(f'value passed into clean method is {value}',10)
            if(str(value) == 'SELECTED'):
                result = 'YES'
        elif(str(valueType) == 'Date'):
            result = CleanDate(value)
        elif(str(valueType) == 'SSN'):
            result = FormatSSN(value)
        else:
            result = str(value)
        eprint(f'value after clean is {result}')
    else:
        eprint(f'value passed into clean method is None', 10)
    return result

def get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, itemNo):
    result = None
    es = filter(lambda x: str(csv_2_ocr_map[csv_key]['ocr'][itemNo]['ocr_key']) in str(x.key) and  pageno in csv_2_ocr_map[csv_key]['ocr'][itemNo]['PageNo'] ,page.form.fields) 
    lFields = list(es)
    eprint(f"i found {str(len(lFields))} field objects",10)
    if(len(lFields)>1):
        for x in  lFields:
            eprint(f'found  field key name: {str(x.key)}',10)
    if(len(lFields)>0):
        tpos = csv_2_ocr_map[csv_key]['ocr'][itemNo]['TopPos']
        if(tpos <= len(lFields)):
            eprint(f'looking for Top position: {tpos}',10)  #  BUG NEED TO FIX.
            # could add check to ensure the number returned is equal or more than the TopPos looking for.  else error will occur.
            sorted_field = sorted(lFields, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
            result = sorted_field[csv_2_ocr_map[csv_key]['ocr'][itemNo]['TopPos']-1]
            eprint(f'the field key for result is {str(result.key)}')
        else:
            eprint(f'trying to find a Top Pos {tpos} when only {len(lFields)} field objects found', 30)
    return result

def get_form_kvp(csv_2_ocr_map, csv_key, dictrow, pageno, page):

    #eprint(f'in the process_ocr_form method csv_2_ocr_map is {csv_2_ocr_map}')
    newdata = {}
    correctField = get_correct_field(csv_2_ocr_map, csv_key, dictrow, pageno, page, 0)
    if(correctField != None):
        correctCleanValueStr = CleanSelectionFieldValueToStr(correctField.value, csv_2_ocr_map[csv_key]['ocr'][0]['Type'])
        newdata[csv_key] = correctCleanValueStr
        ca_csv_key = 'ca_'+csv_key
        if(correctField != None and correctField.value != None):
            newdata[ca_csv_key] = str(correctField.value.content[0].confidence)
        else:
            newdata[ca_csv_key] = '0'
    return newdata

def get__yesno_kvp(csv_2_ocr_map, csv_key, dictrow, pageno, page):
    '''
    Won't work if the yes and no are split between 2 different pages.
    '''
    newdata = {}
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
                newdata[csv_key] = 'YES and NO'
            elif(correctCleanValueStr1 == 'YES'):
                newdata[csv_key] = 'NO'
            elif(correctCleanValueStr0 == 'YES'):
                newdata[csv_key] = 'YES'
            else:
                newdata[csv_key] = ''
    return newdata

def process_ocr_yesno(csv_2_ocr_map, csv_key, dictrow, pageno, page):
    '''
    Deprecated...    Won't work if the yes and no are split between 2 different pages.
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
    global gDocumentName
    configDict = read_config()
    print(f'Debugging level is set to: {debug}')
    print(f'SNS notification level set to: {snsnotify} ')
    try:

        notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
        pdfResultType = "does not exist"
        if('ResultType' in event and event['ResultType'] !=""):
            pdfResultType = event['ResultType']
        eprint(f'Textract event result: {pdfResultType}',20)
        pdfTextExtractionStatus = json.loads(notificationMessage)['Status']
        pdfTextExtractionJobTag = json.loads(notificationMessage)['JobTag']
        pdfTextExtractionJobId = json.loads(notificationMessage)['JobId']
        pdfTextExtractionDocLoc = json.loads(notificationMessage)['DocumentLocation']

        eprint(f'-----  sns message ---- ',10)
        eprint(notificationMessage,10)
        eprint(pdfTextExtractionJobTag + ' : ' + pdfTextExtractionStatus)
        eprint(f'----  Document Location ----')
        eprint(pdfTextExtractionDocLoc)
        eprint(f'----  Job Tag ----')
        eprint(pdfTextExtractionJobTag)
        eprint('----  Job Status ----',10)
        eprint(pdfTextExtractionStatus,10)
        docname = pdfTextExtractionDocLoc['S3ObjectName']
        
        prefixName = docname[0:docname.find('/')]
        eprint(f'prefix name is {prefixName}')
        tablename = configDict["s3_prefix_table_map"][prefixName]["table"]
        eprint(f'---- table name from config Dict is: {tablename} ----')

        csv_2_ocr_map = get_csv_2_ocr_map(docname, configDict, prefixName)
        eprint(csv_2_ocr_map)
        eprint('document name is: '+docname)
        eprint(f'content of document should eprint to table name: {tablename}')
        gDocumentName = str(docname[docname.find('/')+1:])
        response = getJobResults(pdfTextExtractionJobId)
        if response == None: #textract didn't return a response.
            return
        doc = Document(response)
        ocrProcessor = OCRProcessor()
        docdata = ocrProcessor.getDocValues(response, csv_2_ocr_map)
        print(f'---  length of docjson is {len(docdata)}')

    # End logic for getting the correct map based on file name

        all_keys, all_values, pageno, dictrow, jsondatarecord = [], [], 0, {}, {}
        dictrow['SourceFileName'] = docname
        eprint(f'docname type is: {type(docname)}',0)
        dictrow['archer_id'] = docname[docname.find('/')+1:docname.find('/')+12]
        dictrow['jsondata'] = json.dumps(docdata, indent = 2)
        ssn, ca_ssn = '', ''
        jsondatarecord = dictrow.copy()
        regex = re.compile('-..-')
    #   building the array of KVP
        for page in doc.pages:
            pageno = pageno + 1
            eprint(f'---- page {str(pageno)} ----',0)

            eprint('---- eprinting the csv_2_ocr_map again ---',0)
            eprint(csv_2_ocr_map,0)

    #    dictrow['Claimant_Social_Security_Number'] = ssn
    #    dictrow['ca_Claimant_Social_Security_Number'] = ca_ssn

        eprint('--- eprinting dictrow ---', 0)
        eprint(dictrow, 0)

        try:
            dictrow['textract_response'] = json.dumps(response, indent = 2)
        except Exception as e:
            msg = f'error writing textract_response to dictrow, err: {e} on lineNo: {e.__traceback__.tb_lineno}'
            eprint(msg, 40)
        #dictrow['jsondata'] = json_object
        all_values.append(dictrow)

    #    save_ocr_to_bucket(all_values, 'testeric')
        connection = get_connection()
        for dictionary in all_values:
            eprint('writing this to DB', 20)
            eprint(dictionary, 20)
            code = write_dict_to_db(dictionary, connection, tablename)
            if(code >=0):
                eprint("successfully wrote OCR data for document: "+docname, 20)
            else:
                eprint("error writing OCR data for document: "+docname, 40)
    except Exception as e:
        tberror =traceback.print_exc()
        emsg = f'error thrown {e} from line no {e.__traceback__.tb_lineno}'
        eprint(emsg, 50)


class OCRProcessor:
    def __init__(self) -> None:
        pass

    def getDocValues(self, response, ocr_map):
        data, metadata = {}, {}
        doc = Document(response)
        pageno = 0
        for page in doc.pages:
            pageno = pageno + 1
            ocr_form = page.form
            #ocr_form = doc.pages[1].form
            #ocr_map = self.getOcrMap()
            for csv_key in ocr_map:
                matching_fields = filter(lambda x: ocr_map[csv_key]['ocr'][0]['ocr_key'].lower() in str(x.key).lower() and 
                pageno in ocr_map[csv_key]['ocr'][0]['PageNo'] , ocr_form.fields)
                field_list = list(matching_fields)
                sCorrect_field_key, sCorrect_field_value, correct_value_confidence = self.getCorrectField(field_list,ocr_map,csv_key)
                if(pageno == ocr_map[csv_key]['ocr'][0]['PageNo'][0] ):
                    data[csv_key] = sCorrect_field_value
                    #print(f'csv key: {csv_key}  Ocr_key: {sCorrect_field_key} with value: {sCorrect_field_value} on page: {pageno}')

        return data

    def getForm_Form(self, field_list, ocr_map, csv_key):
        return self.get_correct_field(field_list, ocr_map, csv_key, 0)

    def getCorrectField(self, field_list, ocr_map, csv_key):
        field_type = ocr_map[csv_key]["Type"]
        method_name = 'getForm_'+str(field_type)
        method = getattr(self, method_name, lambda: "Invalid Type in Config")
        return method(field_list,ocr_map, csv_key)

    def get_correct_field(self, field_list, ocr_map, csv_key, itemNo):
        correct_field, correct_field_value, correct_field_confidence= 'Not Found', 'Not Found', 0
        #print(f'length of ocr field in ocr map for {csv_key} is: {len(ocr_map[csv_key]["ocr"])}')
        #print(f'field list count is: {len(field_list)}')
        if(len(ocr_map[csv_key]["ocr"]) == 1):
            tPos = ocr_map[csv_key]['ocr'][itemNo]["TopPos"]
            if(len(field_list)==0 or len(field_list)< tPos):
                return None, None, None
            else:
                sorted_fields = sorted(field_list, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
                correct_field = sorted_fields[tPos-1]
                correct_field_value = str(correct_field.value)
                if(correct_field.value is not None):
                    correct_field_confidence = correct_field.value.confidence
            return str(correct_field.key), str(correct_field_value), str(correct_field_confidence)

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



#        for line in page.lines:
#            lineNo += 1
#            for word in line.words:  # update to read CSV and pull page number to avoid dups.
#                if re.search('-..-',str(word)):
#                    eprint(f'--- found -??- in word: {word} on line {lineNo}')
#                    eprint(f'the line {lineNo} is: {page.lines[lineNo]}')
#                    ssn = str(word)
#                    ca_ssn = word.confidence





