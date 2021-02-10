from ewriter import TestOCR
from trp import Document


#TestOCR.find(TestOCR,'Email')
testOcr = TestOCR()
response = testOcr.ocrResults()
doc = Document(response)
searchvalue = 'I confirm that'

onefieldsearch = 'Yes'
pageno = 0

page = doc.pages[0]
es = filter(lambda x: searchvalue in str(x.key), page.form.fields)
lFields = list(es)
print(f"i found {str(len(lFields))} field objects in page")
for field in lFields:
    fvalueId, fcontent, fblock = 'None', ['None'], 'None'
    if(field.value != None):
        fvalueId = field.value.id
        fcontent = field.value.content
        fblock = field.value.block
    if(field is not None):
        print(f'Name: {field.key} with has a Key value set, entity type KEY ID: {field.key.id}')
        print(f'Key value set ID with entity type VALUE ID : {fvalueId} and text {field.value} ')
        print(f' block is {fblock}')
        print(field)
        #onefieldsearch = field.key

print(f'searching by key {onefieldsearch}')
onefield = page.form.getFieldByKey(str(onefieldsearch))
print(f'one field found is {onefield}')

stringvalue, confidence = testOcr.getValueByKey('Email',1)
print(stringvalue)
print(confidence)
print('')
print('')
print('')

testOcr.getDocValues()
