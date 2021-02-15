import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re
import traceback

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
        with open('apiResponse_opt.json', 'rb') as f:
            result = json.load(f)
        self._ocrResults = result

    @staticmethod
    def getOcrMap():
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket = 'archer-ocr-doc-bucket', Key='ocr_config.json')
        content = response['Body']
        ocr_config_json = json.loads(content.read())
        ocrmap = ocr_config_json['ocr_maps']['db_csv_2_ocr_map_flint1']
        cleanse_rule = ocr_config_json['cleanse_rules']['flint1']
        #print(cleanes_rule)
        return ocrmap, cleanse_rule


    def getValueByKey(self, ocrkey, pageno):

        doc = Document(self._ocrResults)
        page = doc.pages[pageno-1]
        field = page.form.getFieldByKey(str(ocrkey))
        if(field is not None):
            return str(field.value), str(field.value.confidence)
        return 'none', 'none'

    def getDocValues(self):
        data, metadata, ocrResult, count_not_found, field_count, poor_confidence_count = {}, {}, None, 0, 0,0
        response = self._ocrResults
        doc = Document(response)
        pageno = 0
        for page in doc.pages:
            pageno = pageno + 1
            ocr_form = page.form
            #ocr_form = doc.pages[1].form
            ocr_map, cleanse_rule = self.getOcrMap()
            #e.print(cleanse_rule)
            for csv_key in ocr_map:
                matching_fields = filter(lambda x: ocr_map[csv_key]['ocr'][0]['ocr_key'].lower() in str(x.key).lower() and 
                pageno in ocr_map[csv_key]['ocr'][0]['PageNo'] , ocr_form.fields)
                field_list = list(matching_fields)

                if(pageno == ocr_map[csv_key]['ocr'][0]['PageNo'][0] ): # clumsy logic to verify i've got the correct field form the correct page. should not need based on filter
                    sCorrect_field_key, sCorrect_field_value, correct_value_confidence = self.getCorrectField(field_list,ocr_map,csv_key)
                    sCorrect_field_value = self.formatDataType(cleanse_rule, ocr_map[csv_key]['ocr'][0]['Type'], sCorrect_field_value)
                    field_count += 1
                    print(f'csv key: {csv_key}  Ocr_key: {sCorrect_field_key} with value: {sCorrect_field_value} Conf: {correct_value_confidence} on page: {pageno}')
                    #data[csv_key] = sCorrect_field_value
                    data[csv_key] = {'value': sCorrect_field_value, 'confidence': correct_value_confidence}
                    if(sCorrect_field_value == 'Not_Found'):
                        count_not_found += 1 
                    else:
                        if(correct_value_confidence < 80 and correct_value_confidence > 0):
                            poor_confidence_count += 1
                    #print('')
        found_fields = field_count-count_not_found
        fp = found_fields / field_count 
        metadata["search_quality"] = {'expected_fields': field_count, 'found_fields': found_fields,'found_percentage': fp }
        metadata["read_quality"] = {'count_less_than_80_percent': poor_confidence_count, 'high_quality_read_percent': (found_fields - poor_confidence_count) / found_fields}
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









    def ocrResults(self):
        return self._ocrResults
        

        


