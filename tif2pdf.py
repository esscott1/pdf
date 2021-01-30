import os
import img2pdf
import boto3
import sys
import argparse

uploadcounter = 0
parser = argparse.ArgumentParser()
parser.add_argument('-p','--path', required=True, help='path to search for document')
parser.add_argument('-r', '--recursive', default='false', choices=['true', 'false'], help='true = will recursively search.  Default is false')
parser.add_argument('-b','--bucket', default='textract-uploadbucket-bas6dcsjr2d2', help='the name of the s3 bucket to send files to.  {optional} Default is set consistent with OCR system')
parser.add_argument('-f','--folder', default='', help='folder in the s3 bucket to copy the files to.')
parser.add_argument('-s','--startswith', default='', help='finds files where the name starts with the string provided' )
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

eprint(args,0)
eprint(args.path,0)
eprint(args.recursive,0)
eprint(args.verbose,0)

def uploadtoocr(path, recurse, bucket, keyprefix, startswith):
    global uploadcounter
    uploadcounter = 0
    if(recurse == None):
        recurse = False
    eprint(f'recursion is set to {recurse}',0)
    basepath = path
    if(recurse == False):
        eprint(f'searching {basepath} non-recursively',0)
        for entry in os.listdir(basepath):
            if os.path.isfile(os.path.join(basepath, entry)):
                if (('.pdf' in entry) or ('.tif' in entry)) and (startswith in entry):
                    eprint(basepath+'/'+entry, 0)
                    uploadtos3(basepath, entry, bucket, keyprefix)
                    uploadcounter =uploadcounter+ 1
    else:
        eprint(f'searching {basepath} recursively',0)
        for r, d, f in os.walk(basepath):
            filecount = sum('.pdf' in files for files in f)
            filecounttif = sum('.tif' in files for files in f)
            eprint(f'found {filecount} pdf and {filecounttif} tif files in {r}',20)
            for file in f:
                if (('.pdf' in file) or ('.tif' in file)) and (startswith in file):
                    eprint(f'asking to upload {r}/{file}',0)
                    uploadtos3(r, file, bucket, keyprefix)
                    uploadcounter = uploadcounter+ 1
    return uploadcounter

def convert_tif_2_pdf(path, filename):
    '''
    returns a cleaned filename, no path, of the PDF with __tif__ at the end
    '''
    pdfname = filename
    pdfname = pdfname[:pdfname.find('.pdf')-3]+'__tif__.pdf'
    pdfname = cleanfilename(pdfname)
    eprint(f'the renamed filename is {pdfname}',0)

    with open(path+'/'+pdfname,"wb") as f:
        f.write(img2pdf.convert(path+'/'+filename))
    eprint(f'wrote the new file {path+"/"+pdfname}',0)
    return pdfname

def cleanfilename(name):
    result = name.replace(' ','_')
    result = result.replace(',','-')
    return result


def uploadtos3(path, filename, bucket, keyprefix):

    fname = path+'/'+filename
    convertedfilename = cleanfilename(filename)
    foundtif = False
    if(filename.find('.tif')>0):
        tif2pdfname = convert_tif_2_pdf(path, filename)
        fname = path+'/'+tif2pdfname
        foundtif = True
    key = convertedfilename
    if(keyprefix is not None):
        key = keyprefix+'/'+key
    if(args.dryrun is None):
        s3client = boto3.client('s3')
        eprint(f'uploading to bucket: {bucket} with key: {key} from file: {fname}',0)
        s3client.upload_file(Bucket=bucket, Key=key,Filename=fname)
    eprint(f'uploaded {fname} to s3 bucket {args.bucket}',20)
#    if(foundtif):
#        os.remove(path+'/'+convertedfilename) # removing the pdf that was created by 
#        eprint(f'deleted PDF that was generated from Tif, filename that was deleted {path+"/"+convertedfilename}')

eprint('--- listing files ---', 20)
#path = str(sys.argv[1])
#recurse = str(sys.argv[2])
#print (f'passed in arguement {path}')
if(args.recursive == 'true'):
    r = True
else:
    r = False
if(args.dryrun is not None):
    print("running a Dryrun.. NO files will be submitted for OCR processing")
count = uploadtoocr(args.path, r, args.bucket, args.folder, args.startswith)
eprint(f'uploaded {count} files',20)


