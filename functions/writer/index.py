import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse

db_csv_headers =['Primary_Attorney',
'HTX_ARCHER_ID',
'ATX_ARCHER_ID',
'Referring_Firm',
'Law_Firm_ID',
'Claimant_Participation_Status',
'Lawsuit_Filed',
'Court_Jurisdiction',
'Case_Caption',
'Case_No',
'Derivative_Claim_of_Consortium_Plaintiff',
'Consortium_Plaintiff_First_Name',
'Consortium_Plaintiff_Last_Name',
'Claim_Package_Scanned_Date',
'Claim_Package_Logged_Date',
'Analyst_Name',
'Claimant_First_Name',
'Claimant_Last_Name',
'Estate_Representative_First_Name',
'Estate_Representative_Last_Name',
'Address_1',
'Address_2',
'City',
'State',
'Zip_Code',
'Email_Address',
'Phone_Number_1',
'Phone_Number_2',
'Estate_Representative_Email_Address',
'Estate_Representative_Phone_Number_1',
'Estate_Representative_Phone_Number_2',
'Roundup_Enrollment_Form_I_elect_to_participate_in_the_ROUN',
'Claimant_Last_Name2',
'Claimant_First_Name3',
'Claimant_Social_Security_Number',
'Claimant_Date_of_Birth',
'Address_14',
'Address_25',
'City6',
'State7',
'Zip_Code8',
'Current_Citizenship_Status',
'Citizen_Status_Eligiblity',
'Citizenship_Status_at_time_of_Exposure',
'Citizen_Status_at_time_of_Exposure_Eligiblity',
'Were_you_married_at_any_time_from_the_date_of_your_initial',
'Spouse_First_Name',
'Spouse_Middle_Name',
'Spouse_Last_Name',
'Is_the_Claim_being_brought_by_a_Representative_on_behalf_o',
'Relationship_to_Product_User',
'If_Other_specify_below',
'Estate_Representative_Last_Name9',
'Estate_Representative_First_Name10',
'Estate_Representative_Middle_Name',
'Estate_Representative_Address_1',
'Estate_Representative_Address_2',
'Estate_Representative_City',
'Estate_Representative_State',
'Estate_Representative_Zip_Code',
'Estate_Representative_Social_Security_Number',
'Estate_Representative_Date_of_Birth',
'Claimant_Date_of_Death',
'Do_you_claim_that_Roundup_Products_caused_the_Death',
'Eligibility_Materials_A_completed_and_signed_Enrollment_Fo',
'Eligibility_Materials_Medical_Record_confirming_Claimants_',
'Eligibility_Materials_Eligibility_Affidavit_',
'Eligibility_Materials_Executed_Release_',
'Eligibility_Materials_Stipulation_of_Dismissal_with_Prejud',
'Enrollment_Form_Signature',
'Enrollment_Form_Signature_Date',
'Begin_Date_of_Roundup_Exposure',
'End_Date_of_Roundup_Exposure',
'Frequency_of_Roundup_Exposure',
'Exposure_to_Roundup_Residential',
'Exposure_to_Roundup_Agriculture',
'Exposure_to_Roundup_Industrial_Turf__Oarnamental',
'During_the_time_period_I_was_exposed_to_Roundup_Less_than_',
'During_the_time_period_I_was_exposed_to_Roundup_10_20_hour',
'During_the_time_period_I_was_exposed_to_Roundup_20_40_hour',
'During_the_time_period_I_was_exposed_to_Roundup_Over_40_ho',
'My_exposure_to_Roundup_Prodcuts_Residential',
'My_exposure_to_Roundup_Prodcuts_Agricultural',
'My_exposure_to_Roundup_Prodcuts_Industrial_Occupational',
'The_Roundup_Products_to_which_I_was_exposed_include_Ready_',
'The_Roundup_Products_to_which_I_was_exposed_include_Mixed_',
'Have_you_ever_had_an_incident_in_which_you_were_soaked_wit',
'Purchase_or_Acquire_Roundup_Products_at',
'Unknown_I_cannot_remember_or_unknown',
'Proof_of_Roundup_Product_Use_Yes',
'Proof_of_Roundup_Product_Use_No',
'If_yes_please_specify_Proof_of_Roundup_Product_Use',
'Date_of_Diagnosis_of_Non_Hodgkin_Lymphoma',
'Copy_of_Medical_Record_Available_Yes',
'Copy_of_Medical_Record_Available_No',
'Does_date_of_diagnosis_with_NHL_match_Medical_Record_Yes',
'Does_date_of_diagnosis_with_NHL_match_Medical_Record_No',
'Affidavit_Signature',
'Signature_Date',
'Notarized',
'Notarized_Date',
'Confidential_Release',
'Medicare',
'Signature_by_Releasing_Party',
'Printed_Name',
'Signed',
'Social_Security_Number',
'Date',
'Notarized11',
'Notarized_Date_12',
'Signature_by_Spouse_Derivative_Party',
'Printed_Name13',
'Signed14',
'Social_Security_Number15',
'Date_of_Birth',
'Relationship_to_Claimant',
'Date16',
'Notarized_17',
'Notarized_Date18',
'Amendment_to_Confidential_Release',
'Signed_19',
'Signature_Date20',
'Notarized_21',
'Notarized_Date22',
'Estate_Documents_Death_Certificate_',
'Estate_Documents_Last_Will_and_Testament_',
'Estate_Documents_Trust_Documents_',
'Estate_Documents_Probate_Documents_',
'Additional_Notes_and_Comments'
]

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
'Claimant Date of Birth':{'ocr_key':'(Month/Day/Year)', 'PageNo': 2, 'TopPos': 1},
'Current Citizenship Status_YES':{'ocr_key':'YES','PageNo': 2, 'TopPos': 1},
'Current Citizenship Status_NO':{'ocr_key': 'NO', 'PageNo': 2, 'TopPos': 1},
'Citizenship Status at time of Exposure_YES':{'ocr_key':'YES','PageNo':2, 'TopPos': 2},
'Citizenship Status at time of Exposure_NO':{'ocr_key':'NO','PageNo':2, 'TopPos': 2},
'Were you married_YES':{'ocr_key': 'YES', 'PageNo': 3, 'TopPos':1},
'Were you married_NO':{'ocr_key': 'NO', 'PageNo': 3, 'TopPos':1}

}

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
'Were_you_married_NO':{'ocr_key': 'NO', 'PageNo': 3, 'TopPos':1}

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
    Current_Citizenship_Status_Yes = dict.pop('Current_Citizenship_Status_YES', None)
    Current_Citizenship_Status_No = dict.pop('Current_Citizenship_Status_NO', None)
    if(str(Current_Citizenship_Status_Yes) == 'SELECTED'):
        dict["Current_Citizenship_Status"] = 'YES'
    if(str(Current_Citizenship_Status_No) == 'SELECTED'):
        dict["Current_Citizenship_Status"] = 'NO'

    Citizenship_Status_at_time_of_Exposure_YES = dict.pop('Citizenship_Status_at_time_of_Exposure_YES', None)
    Citizenship_Status_at_time_of_Exposure_NO = dict.pop('Citizenship_Status_at_time_of_Exposure_NO', None)
    if(str(Citizenship_Status_at_time_of_Exposure_YES) == 'SELECTED'):
        dict["Citizenship_Status_at_time_of_Exposure"] = 'YES'
    if(str(Citizenship_Status_at_time_of_Exposure_NO) == 'SELECTED'):
        dict["Citizenship_Status_at_time_of_Exposure"] = 'NO'

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
def CleanDate(datestring):
    cleanDateResult = 'Trouble Reading, see PDF'
    print(f'--- trying to clean: {datestring}----')
    try:
        parsed_date = parse(datestring)
        cleanDateResult = parsed_date.strftime('%m/%d/%Y')
        print(f'----Cleaned date to: {cleanDateResult}')
    except Exception as e:
        print(f'error cleaning date string provided {datestring}:  error: {e}')
    return cleanDateResult


def GetFromTheTopofPage(fieldlist, pos, page):
#    print('--- unsorted ---')
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
        csv_2_ocr_map = db_csv_2_ocr_map_enroll
    if(str(docname).find('RELFULL') > -1):
        csv_2_ocr_map = csv_2_ocr_map_relfull

# End logic for getting the correct map based on file name
#    printresponsetos3(doc)
    all_keys = []
    all_values = []
    pageno = 0
    dictrow = {}
    dictrow['SourceFileName'] = docname
#   building the array of KVP
    for page in doc.pages:
        pageno = pageno + 1
        print('---- page ',str(pageno),' ----',)
        pagelines = page.lines
        lineNo += 1
        for line in page.lines:
            lineNo =+
            for words in line:
                for word in words:
                    if 'Social' in word:
                        print(f'---  SSN line number is: {lineno}')
                        print(line)

#        print(type(pagelines))
#        print(len(pagelines))
#        print('--- printing page lines ---')
#        print(pagelines)
#        print('--- did it print a page line number?')
        for csv_key in csv_2_ocr_map:    # Getting the keys to build up a row
#            print('Looking for csv_key is: ',csv_key,' | ocr key: ', csv_2_ocr_map[csv_key]['ocr_key'],' | at TopPos: ', str(csv_2_ocr_map[csv_key]['TopPos']),' on Page: ',str(pageno))

            es = filter(lambda x: str(x.key).startswith(str(csv_2_ocr_map[csv_key]['ocr_key'])) and  csv_2_ocr_map[csv_key]['PageNo'] == pageno ,page.form.fields) 
#            selections = filter(lambda x: str(x.key).startswith(str(csv_2_ocr_map[csv_key]['ocr_key'])) and  csv_2_ocr_map[csv_key]['PageNo'] == pageno ,page.form.fields) 

            lFields = list(es)
#            print(f"i found {str(len(lFields))} field objects")
            if(len(lFields)>0):
                correctField = GetFromTheTopofPage(lFields,0,2)
                if(csv_key) == 'Claimant_Date_of_Birth':
                    print('trying to clean date')
                    cleanDOB = CleanDate(str(correctField.value))
                    dictrow[csv_key] = cleanDOB
                else:
                    dictrow[csv_key] = correctField.value
#                print('--- KVP pair block: '+str(correctField.key.block))
#                print(f'--- the csv key is: {csv_key}  the correctField is {correctField.value}')
#            else:
#                print(' --- no correctField found --- ')
            #print(f'write a cell to column: {csv_key} with value: {correctField.value}')
#        print(f'---------------- print dictrow afterpage {pageno} is processed ----------')
#        print(dictrow)
    CollapeYESNO(dictrow)
    all_values.append(dictrow)

#    save_orc_to_bucket(all_values, 'testeric')
    connection = get_connection()
    for dictionary in all_values:
        write_dict_to_db(dictionary, connection)


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



