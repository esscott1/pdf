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
        return self._gemetry

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

mydict = {}

#csv_2_ocr_map = {'Claimant First Name': 'First', 'Claimant Last Name': 'Last'}
csv_2_ocr_map = {'Claimant First Name': {'ocr_key':'First','geometry':{'top':0.995}}, 
'Claimant Last Name': {'ocr_key':'Last','geometry':{'top':0.995}}}

f1 = Form()
f1.addField(Field(FieldKey('state',Geometry(BoundingBox(0.995))),'ny'))
f1.addField(Field(FieldKey('city',Geometry(BoundingBox(0.995))),'buffalo'))
f1.addField(Field(FieldKey('Last',Geometry(BoundingBox(0.995))),'Scottie P'))
f1.addField(Field(FieldKey('First',Geometry(BoundingBox(0.995))),'Scott'))
f1.addField(Field(FieldKey('Last',Geometry(BoundingBox(0.995))),'Magic'))
#x = any(y for y in f1.fields if y.key == 'Last')
#[y for y in f1.fields if y.key == 'Last']

for field in f1.fields:
    print(f"orc key: {field.key} | map key: {csv_2_ocr_map['Claimant First Name']['ocr_key']} ")
#    print(csv_2_ocr_map['Claimant First Name']['ocr_key'])
    if(str(field.key) == str(csv_2_ocr_map['Claimant First Name']['ocr_key'])):
        print('match')

for csv_key in csv_2_ocr_map:
    print('csv_key is: '+csv_key+' | ocr key is: '+csv_2_ocr_map[csv_key]['ocr_key']+' | located at: '+
	str(csv_2_ocr_map[csv_key]['geometry']['top']))
#    print(csv_2_ocr_map.values())
#    print('looking for: '+csv_2_ocr_map[csv_key]['ocr_key'])
    x = filter(lambda x: str(x.key) == str(csv_2_ocr_map[csv_key]['ocr_key']), f1.fields)
    l = list(x)
    if(len(l)>1):
        print('figure which one')

    print(len(l))
#    print(len(list(x)))

    for ocritem in l:
        print('the csv column is: '+ csv_key + ' and values is: ' + ocritem.value)
        #mydict[emap[key]] = str(eitem.value)

print('printing mydict')	
#print(mydict)
print('printing first item in emap')
#print(list(emap)[0])
#print(x)
