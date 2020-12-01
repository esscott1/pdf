
def GetNew():
    d = {'col2':'val2'}
    return d

dict1 = {'col1':'val1'}
print(dict1['col1'])

dict2 = GetNew()
dict1.update(dict2)
print(dict1['col2'])