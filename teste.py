import csv

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
            writer = csv.DictWriter(csvfile,fieldnames=csv_columns)
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
    for csv_key in csv_2_ocr_map:
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
    print('------')
    print('printing dict')
    print('------')
    print(dictrow)
    all_kvp.append(dictrow)

writeToDisk(all_kvp)


#    print(len(list(x)))

#    for ocritem in l:
#S        print('the csv column is: '+ csv_key + ' and values is: ' + ocritem.value)
        #mydict[emap[key]] = str(eitem.value)





#print('printing mydict')	
#print(mydict)
#print('printing first item in emap')
#print(list(emap)[0])
#print(x)
