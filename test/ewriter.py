import json
import boto3
import os
import pg8000
import csv
from trp import Document
from dateutil.parser import parse
import re
import traceback

class TestOCR:

    def __init__(self):
        self.color = 'blue'
        with open('apiResponse3.json', 'rb') as f:
            result = json.load(f)
        self._ocrResults = result

    @staticmethod
    def getOcrMap():
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket = 'archer-ocr-doc-bucket', Key='ocr_config.json')
        content = response['Body']
        ocr_config_json = json.loads(content.read())
        ocrmap = ocr_config_json['ocr_maps']['db_csv_2_ocr_map_flint1']
        return ocrmap


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
            ocr_map = self.getOcrMap()
            for csv_key in ocr_map:
                matching_fields = filter(lambda x: ocr_map[csv_key]['ocr'][0]['ocr_key'].lower() in str(x.key).lower() and 
                pageno in ocr_map[csv_key]['ocr'][0]['PageNo'] , ocr_form.fields)
                field_list = list(matching_fields)
                sCorrect_field_key, sCorrect_field_value, correct_value_confidence = self.getCorrectField(field_list,ocr_map,csv_key)
                if(pageno == ocr_map[csv_key]['ocr'][0]['PageNo'][0] ): # clumsy logic to verify i've got the correct field form the correct page. should not need based on filter
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


        
    def getForm_Form(self, field_list, ocr_map, csv_key):
        return self.get_correct_field(field_list, ocr_map, csv_key, 0)


    def get_correct_field(self, field_list, ocr_map, csv_key, itemNo):
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


    def getCorrectField(self, field_list, ocr_map, csv_key):
        field_type = ocr_map[csv_key]["Type"]
        method_name = 'getForm_'+str(field_type)
        method = getattr(self, method_name, lambda: "Invalid Type in Config")
        return method(field_list,ocr_map, csv_key)









    def ocrResults(self):
        return self._ocrResults
        

        


