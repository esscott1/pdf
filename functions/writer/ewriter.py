import json
import boto3
import os
import pg8000
import csv
#from trp import Document
from dateutil.parser import parse
import re
import traceback


def getresults:
    with open('C:\SampleData\LLNR\test\ocroutput.json') as f:
        d = json.load(f)
        print(d)

getresults()