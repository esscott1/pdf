

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
mydict = {}
field1 = Field('first',Field(Key)
mydict[]
colors = [1,2,3]
for color in colors:
    print(color)