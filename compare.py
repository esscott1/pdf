import os
from shutil import copyfile


remotepath = 'K:/15GMI100-TJH/Incoming_Scans/TORT/FLINT+'
remotefiles = []
basefiles = []
basepath = 'C:/SampleData/FLINT+'
for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
                basefiles.append(entry)
                
#print(basefiles)

for entry in os.listdir(remotepath):
        if os.path.isfile(os.path.join(remotepath, entry)):
                remotefiles.append(entry)
                if entry not in basefiles:
                    print(f'{entry} not local, coping now')
                    copyfile(remotepath+'/'+entry, basepath+'/new/'+entry)
