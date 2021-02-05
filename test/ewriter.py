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
        with open('apiResponse.json', 'rb') as f:
            result = json.load(f)
        self._ocrResults = result

    @staticmethod
    def getOcrMap():
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket = 'archer-ocr-doc-bucket', Key='ocr_config.json')
        content = response['Body']
        ocr_config_json = json.loads(content.read())
        ocrmap = ocr_config_json['ocr_maps']['db_csv_2_ocr_map_llnl']
        return ocrmap


    def getValueByKey(self, ocrkey, pageno):

        doc = Document(self._ocrResults)
        page = doc.pages[pageno-1]
        field = page.form.getFieldByKey(str(ocrkey))
        if(field is not None):
            return str(field.value), str(field.value.confidence)
        return 'none', 'none'

    def getDocValues(self):
        response = self._ocrResults
        doc = Document(response)
        ocr_map = self.getOcrMap()
        for csv_key in ocr_map:
            print(csv_key)

    
    def ocrResults(self):
        return self._ocrResults
        

        


