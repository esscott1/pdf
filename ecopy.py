import os
import img2pdf
import boto3
import sys
import argparse
from shutil import copyfile

uploadcounter = 0
parser = argparse.ArgumentParser()
parser.add_argument('-p','--path', required=True, help='path to search for document')
parser.add_argument('-r', '--recursive', default='false', choices=['true', 'false'], help='true = will recursively search.  Default is false')
parser.add_argument('-s','--startswith', default='', help='finds files where the name starts with the string provided' )
parser.add_argument('-l','--limit', type=int, help='an integer that will limit the number of files processed.')
parser.add_argument('-t', '--target', help='none s3 bucket target location to copy files to' )
parser.add_argument('--verbose', action='store_const', const=0, help='sets the output logging level')
parser.add_argument('--silent', action='store_const', const=50, help='sets the output logging level')
parser.add_argument('--version', action='version', version='Version 2.0')
parser.add_argument('--dryrun', action='store_const', const=100, help='will find files but NOT upload to S3 for processing')

args = parser.parse_args()


def eprint(msg, sev = 0):
    if(args.verbose is not None):
        print(msg)
        return
    if(args.silent is not None):
        return
    else:
        if(sev > 0):
            print(msg)


def cleanfilename(name):
    result = name.replace(' ','_')
    result = result.replace(',','-')
    result = result.replace('(','-')
    result = result.replace(')','-')
    result = result.replace("'","")
    result = result.replace("+","_")
    return result

def copyto(path,startswith,target):
    basepath = path
    
    counter = 0
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            if (('.pdf' in entry) or ('.tif' in entry)) and (startswith in entry):
                eprint(basepath+'/'+entry, 20)
                fname = path+'/'+entry
                convertedfilename = cleanfilename(entry)
                if(args.dryrun is None):
                    copyfile(fname,target+'/'+convertedfilename)
                    eprint(f"copied {fname} to {target+'/'+convertedfilename}",20)
                else:
                    eprint(f"Dry Run: would have copied {fname} to {target+'/'+convertedfilename}",20)
                counter += 1
    return counter

eprint('--- listing files ---', 20)
eprint(f'--- target is {args.target} ---', 20)
eprint(f'--- Dry run is: {args.dryrun}',20)
#path = str(sys.argv[1])
#recurse = str(sys.argv[2])
#print (f'passed in arguement {path}')
if(args.recursive == 'true'):
    r = True
else:
    r = False
if(args.dryrun is not None):
    print("running a Dryrun.. NO files will be submitted for OCR processing")
count = copyto(args.path, args.startswith, args.target)
eprint(f'uploaded {count} files',20)