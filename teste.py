import csv
import json
from dateutil.parser import parse
import pkg_resources
import re

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

ocr_map1  =  {
    's3_prefix_table_map':
	{
		'dallas':{
			'table':'dallas_test',
			'filename_ocrmap':
			{
				'enroll':'db_csv_2_ocr_map_enroll',
				'relfull': 'csv_2_ocr_map_relfull',
				'afft':'db_csv_2_ocr_map_afft'
			}
		},
		'dbj': {
			'table': 'ca_packet',
			'filename_ocrmap':
			{
				'LYGDAA':'db_csv_2_ocr_map_lygdaa'
			}
		}
	},
    'ocr_table_file_maps':{
		'db_csv_2_ocr_map_lygdaa':{
			'table':'dallas_test',
			'fileregex': 'LYGDAA'},
		'db_csv_2_ocr_map_afft' :
		{'table':'ca_packet',
			'fileregex': 'afft'},
		
		'db_csv_2_ocr_map_enroll': 
		{'table':'ca_packet',
			'fileregex': 'enroll'}
	},
'db_csv_2_ocr_map_afft' :
{
    'afft_Address': {'Type': 'MultiSelection','ocr':[
        {'ocr_key' : 'NO', 'PageNo': 1, 'TopPos': 1, 'Type': 'String'},
        {'ocr_key': 'YES', 'PageNo': 1, 'TopPos': 1, 'Type': 'String'} ]
        },
    'afft_City_state_zip': {'Type':'Form', 'ocr':[ {'ocr_key':'City', 'PageNo': 1, 'TopPos': 1, 'Type': 'String'} ]},
    'afft_ssn':{'ocr_key':'My Social Security', 'PageNo': 1, 'TopPos': 1, 'Type': 'String'},
    'afft_dob':{'ocr_key':'My Date of Birth', 'PageNo': 1, 'TopPos': 1, 'Type': 'Date'},
    'during_the_time_period_i_was_exposed_to_roundup_less_than_':{'ocr_key':'10 Hours per year on average', 'PageNo': 1, 'TopPos': 1, 'Type': 'Selection'},
    'during_the_time_period_i_was_exposed_to_roundup_10_20_hour':{'ocr_key':'- 20 Hours per year on average', 'PageNo': 1, 'TopPos': 1, 'Type': 'Selection'},
    'during_the_time_period_i_was_exposed_to_roundup_20_40_hour':{'ocr_key':'40 Hours per year on average', 'PageNo': 1, 'TopPos': 1, 'Type': 'Selection'},
    'during_the_time_period_i_was_exposed_to_roundup_over_40_ho':{'ocr_key':'Over 40 Hours', 'PageNo': 1, 'TopPos': 1, 'Type': 'Selection'}
}}

class Geometry:
    def __init__(self, boundingBox):
        self._boundingBox = boundingBox

    @property
    def boundingBox(self):
        return self._boundingBox

class BoundingBox:
    def __init__(self, top):
        self._top = top

    @property
    def top(self):
        return self._top


class Form:
    def __init__(self):
        self._fields = []
        
    def addField(self, field): 
        self._fields.append(field)
        
    @property
    def fields(self):
        return self._fields


class FieldKey:
    def __init__(self, key, geometry):
        self._geometry = geometry
        self._text = key

    def __str__(self):
        return self._text

    @property
    def geometry(self):
        return self._geometry

    def key(self):
        return self._text



class Field:
    def __init__(self, key, value):
        self._key = key
        self._value = value
        
    def __str__(self):
        s = "\nField\n==========\n"
        k = ""
        v = ""
        if(self._key):
            k = str(self._key)
        if(self._value):
            v = str(self._value)
        s = s + "Key: {}\nValue: {}".format(k, v)
        return s
        
    @property
    def key(self):
        return self._key
        
    @property
    def value(self):
        return self._value

f1 = Form()
f1.addField(Field(FieldKey('state',Geometry(BoundingBox(0.995))),'ny'))
f1.addField(Field(FieldKey('city',Geometry(BoundingBox(0.995))),'buffalo'))
f1.addField(Field(FieldKey('Last',Geometry(BoundingBox(0.463))),'Abbe'))
f1.addField(Field(FieldKey('First',Geometry(BoundingBox(0.463))),'Toby'))
f1.addField(Field(FieldKey('First',Geometry(BoundingBox(0.756))),'Grant'))
f1.addField(Field(FieldKey('Last',Geometry(BoundingBox(0.756))),'Davis'))

def collapeYESNO(dict):
    dict['newcolumn'] = 'yes'
    Current_Citizenship_Status_Yes = dict.pop('Claimant Last Name', None)
    print(Current_Citizenship_Status_Yes)

def GetFromTheTop(fieldlist, pos, page):

    sorted_field = sorted(fieldlist, key=lambda x: x.key.geometry.boundingBox.top, reverse=False)

    
    print('first one is',sorted_field[pos].value)
    return sorted_field[pos]

def writeToDisk(dictList):
    print(' ----------------------  ')
    print('trying to write file')
#    csv_columns = ['Claimant First Name','Claimant Last Name']
    csv_columns = dictList[0].keys()
    try:
        with open('c:/src/egit/pdf/testme.csv','w') as csvfile:
            writer = csv.DictWriter(csvfile,fieldnames=csv_headers)
            writer.writeheader()
            for data in dictList:
                print(data)
                writer.writerow(data)
            print('SUCCESS: wrote testme.csv')
    except Exception as e:
        print('error writing csv local: '+ e)


all_kvp = []

#csv_2_ocr_map = {'Claimant First Name': 'First', 'Claimant Last Name': 'Last'}
csv_2_ocr_map = {
'Claimant First Name': {'ocr_key':'First', 'PageNo': 1, 'TopPos': 1},
'Claimant Last Name': {'ocr_key':'Last', 'PageNo': 1, 'TopPos': 1}
}

#x = any(y for y in f1.fields if y.key == 'Last')
#[y for y in f1.fields if y.key == 'Last']
"""
for field in f1.fields:
    print(f"orc key: {field.key} | map key: {csv_2_ocr_map['Claimant First Name']['ocr_key']} ")

    if(str(field.key) == str(csv_2_ocr_map['Claimant First Name']['ocr_key'])):
        print('match')
"""
dictrow = {}
pages = [1,2,3,4]

for page in pages:
    print('------')
    print('Page: '+str(page))
    print('------')
    lineNo = 1
    for csv_key in csv_2_ocr_map:
        lineNo += 1
#        print('csv_key is: '+csv_key+' | ocr key is: '+csv_2_ocr_map[csv_key]['ocr_key']+' | located at: '+
#        str(csv_2_ocr_map[csv_key]['TopPos']))
    #    print('csv_key is: '+csv_key+' | ocr key is: '+csv_key['ocr_key']+' | located at: '+ str(csv_2_ocr_map[csv_key]['geometry']['boundingBox']['top']))
    #    print(csv_2_ocr_map.values())
    #    print('looking for: '+csv_2_ocr_map[csv_key]['ocr_key'])
        x = filter(lambda x: str(x.key) == str(csv_2_ocr_map[csv_key]['ocr_key']) and csv_2_ocr_map[csv_key]['PageNo'] == page, f1.fields)
        l = list(x)
        if(len(l)>1):
            correctField = GetFromTheTop(l,0, page)
            print('adding: '+correctField.value+ ' to dictrow')
            dictrow[csv_key] = correctField.value
'''
    print('------')
    print('printing dict')
    print('------')
    print(dictrow)
    all_kvp.append(dictrow)
    print(lineNo)
'''
print('printing dict after method to add')
collapeYESNO(dictrow)
json_object = json.dumps(dictrow)
dictrow['jsondata'] = json_object
#print(dictrow)


print('--- printing ocr map1 ---')
print(f'ocr_map1 is of type {type(ocr_map1)}')
json_object = json.loads(json.dumps(ocr_map1))
print('--- print ocr_table_file_maps object ---')
print(json_object['ocr_table_file_maps'])
print('------')
print('--- print db_csv_2_ocr_map_lygdaa object ---')
print(json_object['ocr_table_file_maps']['db_csv_2_ocr_map_lygdaa'])
print('------')
print('--- print fileregex object ---')
print(json_object['ocr_table_file_maps']['db_csv_2_ocr_map_lygdaa']["fileregex"])
print('------')
print(json_object['db_csv_2_ocr_map_afft']['afft_City_state_zip']['ocr'])
print(f'number of objects in ocr of afft_City_state_zip is: ')
print(len(json_object['db_csv_2_ocr_map_afft']['afft_City_state_zip']['ocr']))
print(f"the type is: {json_object['db_csv_2_ocr_map_afft']['afft_City_state_zip']['Type']}")


print('----')
json_dict = json.loads(json.dumps(ocr_map1))
print(type(json_dict))
for snippet in json_dict["s3_prefix_table_map"]["dallas"]["filename_ocrmap"]:
    print(f'snippet is {snippet}')
    print(json_dict["s3_prefix_table_map"]["dallas"]["filename_ocrmap"][snippet])
#prefix = str.startswith(s3Name,beg=0,end=sindex)
#print(f'prefix is: {prefix}')





#print('---- testing date of birth ----')
#dob = '01 12611952'
#print(type(dob))
#parsedob = dob[0:3]
#print(parsedob)
#parsed_date = parse(dob)
#print(parsed_date)
#print(type(parsed_date))
#print(parsed_date.strftime('%m/%d/%Y'))
#writeToDisk(all_kvp)



#    print(len(list(x)))

#    for ocritem in l:
#S        print('the csv column is: '+ csv_key + ' and values is: ' + ocritem.value)
        #mydict[emap[key]] = str(eitem.value)





#print('printing mydict')	
#print(mydict)
#print('printing first item in emap')
#print(list(emap)[0])
#print(x)

