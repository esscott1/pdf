import json
import boto3
import pg8000
import os
from numpy.lib.type_check import _nan_to_num_dispatcher
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re
import traceback
from pdfrw import PdfReader, PdfWriter
import numpy as np


class e:
    def __init__(self) -> None:
        pass
        
        
    def print (msg, sev=10, sendsns=True):
        '''
        default is debug or notset unless correctly specified in config
        sev options are: critical: 50 | error: 40 | warning: 30 | info: 20 | debug: 10 | verbose: 0 
        '''
        debug = 'debug'
        #if(type(msg) == str):
        #    msg = "Doc name is: "+gDocumentName +" msg is: "+msg
        if debug.lower() not in {'critical', 'error', 'warning', 'info', 'debug', 'verbose'}:
            print(f'debug in config file set to something other than "critical", "error", "warning", "info" or "debug" therefore the setting will be "debug".')
            debug = 'debug'
        loglevel = 10 if debug == 'debug' else 20 if debug == 'info' else 30 if debug == 'warning' else 40 if debug == 'error' else 50 if debug == 'critical' else 0
        if(sev >= loglevel):
            print(msg)
        #if(sendsns):
        #    writetosnstopic(msg, sev)


class TestOCR:

    def __init__(self):
        self.color = 'blue'
        #filename = 'apiResponse_sin.json'
        filename = 'apiResponse_big.json'

        with open(filename, 'rb') as f:
            result = json.load(f)
        self._ocrResults = result
        #self._prefixName = 'llnl'
        #self._docname = 'llnl/SINUCA10087-CMDF--202002171204.pdf'
        self._docketName = 'combined_flint'
        self._prefixName = 'flint1'
        self._docname = 'flint1/Flint_opt_good_test2.pdf'


    @property
    def prefixName(self):
        return self._prefixName

    def getConfigInfo(self, formName = 'registration', prefixName = 'combined_flint', docname = 'flint_REG_somthing.pdf'):
        config_file = 'ocr_config_new.json'
        with open(os.path.join(os.pardir, config_file)) as f:
            ocr_config_json = json.load(f)

        #s3 = boto3.client('s3')
        #response = s3.get_object(Bucket = 'archer-ocr-doc-bucket', Key='ocr_config.json')
        #content = response['Body']
        #ocr_config_json = json.loads(content.read())
        configDict = ocr_config_json.copy()

        ocrmap, cleanse_rule = {},{}
        for docket_name, docket_def in configDict["docket_info"].items():
            if(docket_def['s3prefix'] == prefixName):
                docketName = docket_name

        #print('---------------  forms ---------------')
        #print(configDict["doc_definition"].get(docketName,{}).get('forms',{}).items())
        for form_name, form_def in configDict["doc_definition"].get(docketName,{}).get('forms',{}).items():
            print(f'{form_def["doc_name_contains"]}')
            if(form_def['doc_name_contains'] in docname):
                print(f'form name is: {form_name}')
                print(f'form def is: {form_def}')

        doc_def_Name = configDict["docket_info"].get(docketName,{}).get("doc_definition",{})
        doc_definition = configDict["doc_definition"].get(doc_def_Name,{}) 

        ocrmapName = configDict["doc_definition"].get(doc_def_Name,{}).get('forms',{}).get(formName,{}).get("ocr_map")
        ocrmap = configDict["ocr_maps"].get(ocrmapName,{})
        
        cleanse_rules_Name = configDict["doc_definition"].get(doc_def_Name,{}).get('forms',{}).get(formName,{}).get("cleanse_rules")
        cleanse_rule = configDict["cleanse_rules"].get(cleanse_rules_Name,{})

        #print(doc_definition)
        return ocrmap, cleanse_rule, doc_definition
                
        for snippet in configDict["docket_info"].get(self._docketName,{})["form_info"]:

            if(str(self._docname).find(snippet) > -1):
                #print(f'map should be {configDict["docket_info"][self._prefixName]["form_info"][snippet]["ocr_map"]}')
                omap = configDict["docket_info"][self._prefixName]["form_info"][snippet]["ocr_map"]
                #print(f'map should by {configDict["ocr_maps"][omap]}')
                ocrmap = configDict["ocr_maps"][omap]
                cleanse_rule_name = configDict["docket_info"][self._prefixName]["form_info"][snippet].get("cleanse_rules",None)
                cleanse_rule = ocr_config_json.get('cleanse_rules',{}).get(str(cleanse_rule_name),{}) if cleanse_rule_name != None else {}

                doc_definition = ocr_config_json.get('doc_definition',{}).get(self._prefixName,{}) if self._prefixName != None else {}
                #print(f'Cleanse rule name is {cleanse_rule_name}')
                #print(f'Cleanse rule is {cleanse_rule}')

    def getValueByKey(self, ocrkey, pageno):

        doc = Document(self._ocrResults)
        page = doc.pages[pageno-1]
        field = page.form.getFieldByKey(str(ocrkey))
        if(field is not None):
            return str(field.value), str(field.value.confidence)
        return 'none', 'none'

    def getLines(self):
        result = []
        response = self._ocrResults
        doc = Document(response)
        pageno = 0
        ocr_map, cleanse_rule = self.getOcrMap()
        for page in doc.pages:
            for line in page.lines:
                if(line.text.find('I 2 2') != -1):
                    print(line)

    def pagelines(self, page):
        lines = page.getLinesInReadingOrder()
        for lineno, linetxt in enumerate(lines):
            print(linetxt)

    def findPage(self, search_text, document):
        '''
        returns list of source pages the search text is in
        '''
        page_found_in = []
        pageno = 0
        for page in document.pages:
            pageno += 1
            page_lines = page.getLinesInReadingOrder()
            target_page = 0
            for line in page_lines:
                if search_text.lower() in str(line[1].lower()):
                    page_found_in.append(pageno)
                    print(f'-- found {search_text} in {line[1]} on page: {pageno}--')
        return page_found_in

    def addRemainingPages(self, sourcePageCount, page_rewrite_map):
        found_pages = []
        result = {}
        source_list = [*range(1,sourcePageCount+1,1)]
        #print(f'source list: {source_list}')
        for source_page_no in range(sourcePageCount):
            for form in page_rewrite_map:
                for map in page_rewrite_map[form]:
                    if(source_page_no == map[0]):
                    #res = any(source_page_no in map for map in  page_rewrite_map[form])
                    #if(res):
                        found_pages.append(source_page_no)
        #print(f'found source pages: {found_pages}')
        not_found_pages = np.setdiff1d(source_list,found_pages)
        print(f'source page numbers not assigned: {not_found_pages}')
        i = 0

        for unassigned_page in not_found_pages:
            i +=1
            #print(unassigned_page)
            if('supporting_docs' not in result):
                result["supporting_docs"] = [[unassigned_page,i]]
            else:
                result["supporting_docs"].append([unassigned_page,i])
        #print(f'result: {result}')
        return result


    def getPageMap(self, ocrResponse, doc_definition):
        '''
        returns a dictionary of lists that have the form name as key and the list contains
        the source page number and the target page number of the key form name 
        '''
        data, metadata, ocrResult, count_not_found, count_found, poor_confidence_count = {}, {}, None, 0, 0,0
        # response = self._ocrResults
        #response = self.read_db(self.get_connection())
        response = ocrResponse
        doc = Document(response)
        page_rewrite_map = {}
        pages_map = []
        #ocr_map, cleanse_rule, doc_definition = self.getOcrMap()
        print()
        #print('------------- doc_defintion from getOcrMap')
        #print(doc_definition)
        print()
        form_names = list(doc_definition.get('forms',{}).keys())
        #print(f'form name is: {form_name[0]}')
        for form in form_names:
            source_2_target_map = []
            pageno = 0
            form_type_name = doc_definition.get('forms',{}).get(form,{}).get('doc_name_contains','badconfig')
            pages_map = list(doc_definition.get('forms',{}).get(form,{}).get('pages',{}))
            for form_page_definition in pages_map:
                search_text = form_page_definition['text']
                target_page = form_page_definition['page_no']
                found_in_page = self.findPage(search_text, doc)

                #if(len(found_in_page)>1): #  2 or more of same form in PDF.
                for form_number in range(len(found_in_page)):
                    #source_2_target_map.append([found_in_page[form_number],target_page])
                    if(form_type_name+'_'+str(form_number) not in page_rewrite_map):
                        page_rewrite_map[form_type_name+'_'+str(form_number)] = [[found_in_page[form_number],target_page] ]
                    else:
                        #print(f'appending {[found_in_page[form_number],target_page]}')
                        page_rewrite_map[form_type_name+'_'+str(form_number)].append([found_in_page[form_number],target_page] )

        support_doc_dict = self.addRemainingPages(len(doc.pages),page_rewrite_map)
        page_rewrite_map.update(support_doc_dict)
        #self.splitPDF(page_rewrite_map)
        print(page_rewrite_map)

        return page_rewrite_map

    def splitPDF(self, sourcePath, input_file_name, targetPath, s3Prefex):
        #input_file = "C:/src/egit/pdf/test/Flint_OCR_Test_Big.pdf"
        configDict = self.getConfigDict()
        docket_name = self.getDocket_Name(s3Prefex, configDict)
        doc_definition = self.getDoc_Definition(docket_name, configDict)
        ocrResponse = self.read_db(self.get_connection(),input_file_name )
        if ocrResponse == -1:
            return
        ocrmap, cleanse_rule, doc_defintion = self.getConfigInfo()

        page_rewrite_map = self.getPageMap(ocrResponse, doc_defintion)
        reader_input = PdfReader(sourcePath+'/'+input_file_name)
        for targetform in page_rewrite_map:
            writer_output = PdfWriter()
            output_file_name = input_file_name.replace('RELFULL',str(targetform).upper())
            output_file = targetPath+'/'+output_file_name
            #output_file = "C:/src/egit/pdf/test/split/Flint_big_"+str(targetform)+".pdf"
            #print(f'sorted map for form:  {targetform}')
            sorted_source_2_target_map = sorted(page_rewrite_map[targetform], key=lambda x: x[0])
            #print(sorted_source_2_target_map)

            for map in sorted_source_2_target_map:
                writer_output.addPage(reader_input.pages[map[0]-1])
            #print(f'writing to file: {output_file} with {sorted_source_2_target_map}')
            writer_output.write(output_file)
        #for current_page in range(len(reader_input.pages)):
        #        writer_output.addpage(reader_input.pages[source_page_no])


    def getDocket_Name(self, s3Prefix, configDict):
        result = ''
        for docket_name, docket_def in configDict["docket_info"].items():
            if(docket_def['s3prefix'] == s3Prefix):
                result = docket_name
        return result
                
    def getConfigDict(self):
        result = {}
        config_file = 'ocr_config_new.json'
        with open(os.path.join(os.pardir, config_file)) as f:
            result = json.load(f) 
        return result

    def getDoc_Definition(self, docketName, configDict):
        result = {}
        doc_def_Name = configDict["docket_info"].get(docketName,{}).get("doc_definition",{})
        result = configDict["doc_definition"].get(doc_def_Name,{}) 
        return result

    def getDocValues(self):
        data, metadata, ocrResult, count_not_found, count_found, poor_confidence_count = {}, {}, None, 0, 0,0
        response = self._ocrResults
        doc = Document(response)
        pageno = 0
        ocr_map, cleanse_rule, doc_defintion = self.getOcrMap()
        print('Doc definition')
        print(doc_defintion)
        for page in doc.pages:
            pageno = pageno + 1
            ocr_form = page.form
            #ocr_form = doc.pages[1].form

            #e.print(cleanse_rule)
            for csv_key in ocr_map:
                matching_fields = filter(lambda x: ocr_map[csv_key]['ocr'][0]['ocr_key'].lower() in str(x.key).lower() and 
                pageno in ocr_map[csv_key]['ocr'][0]['PageNo'] , ocr_form.fields)
                field_list = list(matching_fields)

                if(pageno == ocr_map[csv_key]['ocr'][0]['PageNo'][0] ): # clumsy logic to verify i've got the correct field form the correct page. should not need based on filter
                    sCorrect_field_key, sCorrect_field_value, correct_value_confidence = self.getCorrectField(field_list,ocr_map,csv_key)
                    if(cleanse_rule != None and sCorrect_field_value != 'Not_Found'):
                        sCorrect_field_value = self.formatDataType(cleanse_rule, ocr_map[csv_key]['ocr'][0]['Type'], sCorrect_field_value)
                    #field_count += 1
                    print(f'csv key: {csv_key}  Ocr_key: {sCorrect_field_key} with value: {sCorrect_field_value} Conf: {correct_value_confidence} on page: {pageno}')
                    #data[csv_key] = sCorrect_field_value
                    data[csv_key] = {'value': sCorrect_field_value, 'confidence': correct_value_confidence}
                    if(sCorrect_field_value == 'Not_Found'):
                        count_not_found += 1 
                    else:
                        count_found += 1
                        if(correct_value_confidence < 80 and correct_value_confidence > 0):
                            poor_confidence_count += 1
                    #print('')
            #if pageno == 2:
            #    self.pagelines(page)


        fp = count_found / len(ocr_map)  if len(ocr_map) !=0 else 0
        read_percent = (count_found - poor_confidence_count) / count_found if count_found !=0 else 0
        metadata["search_quality"] = {'expected_fields': len(ocr_map), 'found_fields': count_found,'found_percentage': fp }
        metadata["read_quality"] = {'count_less_than_80_percent': poor_confidence_count, 'high_quality_read_percent': read_percent}
        return data, metadata


    def getForm_Form(self, field_list, ocr_map, csv_key, itemNo = 0):
        correct_field, correct_field_value, correct_field_confidence= 'Not_Found', 'Not_Found', 0
        #print(f'length of ocr field in ocr map for {csv_key} is: {len(ocr_map[csv_key]["ocr"])}')
        #print(f'field list count is: {len(field_list)}')
        if(len(ocr_map[csv_key]["ocr"]) == 1):
            tPos = ocr_map[csv_key]['ocr'][itemNo]["TopPos"]
            if(len(field_list)==0 or len(field_list)< tPos):
                return 'Not_Found', 'Not_Found', 0
            else:
                sorted_fields = sorted(field_list, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)
                correct_field = sorted_fields[tPos-1]
                correct_field_value = str(correct_field.value)
                if(correct_field.value is not None):
                    correct_field_confidence = correct_field.value.confidence
            return str(correct_field.key), str(correct_field_value), correct_field_confidence

    def getForm_YesNo(self, field_list, ocr_map, csv_key):
        sOcrKey, sOcrValue, OcrConfidence = '','',0
        if(len(ocr_map[csv_key]["ocr"])!= 2):
            sOcrKey, sOcrValue, OcrConfidence = "Bad Config", "Bad Config", 0
        else:
            cf_key_0, cf_value_0, cf_value_ca_0 = self.get_correct_field(field_list, ocr_map,csv_key, 0)
            cf_key_1, cf_value_1, cf_value_ca_1 = self.get_correct_field(field_list, ocr_map,csv_key, 1)
            if(cf_key_0 is None or cf_key_1 is None):
                sOcrKey, sOcrValue, OcrConfidence =  "Not Found", "Not Found", 0
            if(cf_key_0.lower() == 'yes'):
                if (cf_value_0 == 'SELECTED'):
                    sOcrKey, sOcrValue, OcrConfidence = cf_key_0, cf_value_0, cf_value_ca_0
                if(cf_value_1 == 'SELECTED'):
                    sOcrKey, sOcrValue, OcrConfidence = cf_key_1, cf_value_1, cf_value_ca_1
            elif(cf_key_1.lower() == 'yes'):
                if (cf_value_0 == 'SELECTED'):
                    sOcrKey, sOcrValue, OcrConfidence = cf_key_0, cf_value_0, cf_value_ca_0
                if(cf_value_1 == 'SELECTED'):
                    sOcrKey, sOcrValue, OcrConfidence = cf_key_1, cf_value_1, cf_value_ca_1
            else:
                return "Not_Found", "Not_Found", "Not_Found"
            yes_field = cf_key_1 if str(ocr_map[csv_key]['ocr'][0]['ocr_key']).upper() == 'YES' else cf_key_2 if str(ocr_map[csv_key]['ocr'][1]['ocr_key']).upper() == 'YES' else None
            if(yes_field is None):
                return  "Bad Config", "Bad Config", "Bad Config"
            if (yes_field == cf_key_1):
                return  "Bad Config", "Bad Config", "Bad Config" #  need to work on this function
            return  "Bad Config", "Bad Config", "Bad Config"


    def formatDataType(self, rule_set, data_type, value):
        sValue = str(value)
        cleanse_rule = rule_set.get(str(data_type), None)
        if(cleanse_rule is None):
            return value
        replaceRule = cleanse_rule['replace']
        insertRule = cleanse_rule['insert']
        rex = cleanse_rule['regex']
        targetlength = cleanse_rule['length']
        if(sValue is not 'Not_Found' or sValue is not 'None'):
            if(sValue != None and re.compile(str(rex)).match(sValue) == None):
                for rule in replaceRule:
                    #print(f'replace {rule["this"]} for {rule["with"]}')
                    sValue = sValue.replace(rule["this"],rule["with"])
                #print(f'{data_type} after replace periods is {sValue}')
                #print(f'length of {data_type}: {len(sValue)}')
                if(str(len(sValue)) == str(targetlength)):
                    for insert in insertRule:
                        sValue = sValue[:insert['at']] + insert['this'] + sValue[insert['at']:]
        return sValue



    def getCorrectField(self, field_list, ocr_map, csv_key):
        field_type = ocr_map[csv_key]["Type"]
        method_name = 'getForm_'+str(field_type)
        method = getattr(self, method_name, lambda f, o ,c: "Invalid Type in Config")
        sKey, sValue, confidence = method(field_list,ocr_map, csv_key)
        if(str(ocr_map[csv_key]['ocr'][0]['Type']) == 'ssn'):
            print(f'******  found SSN on key {csv_key} ********')
        #sValue = self.formatDataType(sValue, str(ocr_map[csv_key]['ocr'][0]['Type']))
        return sKey,sValue, confidence




    def get_connection(self):
        """
        Method to establish the connection to RDS using IAM Role based authentication.
        """
        try:
            print()
            print ('Connecting to database')
            client = boto3.client('rds')
            DBEndPoint = 'ddhgkdwsopbt3a.c3bquq8vfcla.us-west-2.rds.amazonaws.com' #os.environ.get('DBEndPoint')
            DatabaseName = 'InvoiceDB' #os.environ.get('DatabaseName')
            DBUserName = 'lvthillo' #os.environ.get('DBUserName')
            # Generates an auth token used to connect to a db with IAM credentials.
            #print (f'trying to get password with auth token for endpoint: {DBEndPoint}')
            password = 'notsupersecret' #client.generate_db_auth_token(DBHostname=DBEndPoint, Port=5432, DBUsername=DBUserName)
            #print (f'connecting to: {DatabaseName}')
            #print (f'connecting as: {DBUserName}')
            #print (f'connecting with pw: {password}')
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
            msg = f'DB connection error: {e} on lineNo: {e.__traceback__.tb_lineno}'
            print (msg, 40)
            return None
    
    def read_db(self, connection, filename):
        sql = "SELECT sourcefilename, textract_response from combined_flint where sourcefilename like %s"
        try:
            cursor = connection.cursor()
            value = filename
            fieldlist = []
            fieldlist.append('%'+str(value))
            #print(sql)
            #print(fieldlist)
            cursor.execute(sql, fieldlist)
            #print("the number of rows: ",cursor.rowcount)
            if cursor.rowcount == 0:
                print(f'***************  warning, no ocr-result found for file {filename} *****')
                return -1
            row = cursor.fetchone()
            result = row[1]
            while row is not None:
                #print(row[0])
                row = cursor.fetchone()
            cursor.close()
            return result
        except Exception as e:
            print(e)
            return -1


    def write_to_db(self, connection,tablename='combined_flint'):
        sql = "INSERT INTO combined_flint (sourcefilename) VALUES (somethingtxt) "
        try:
            cursor = connection.cursor()
            cursor.execute('''INSERT INTO combined_flint(sourcefilename) VALUES ('something')''')
            connection.commit()
            cursor.close()
            return 0
        except Exception as e:
            print(e)
            return -1



    def ocrResults(self):
        return self._ocrResults
        

        


