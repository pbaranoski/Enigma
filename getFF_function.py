import boto3
import os
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import subprocess
import sys

def write_sp_info_2_log(sp_info):
        
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    if sp_info.stdout != "":
        print("\n%s", sp_info.stdout) 
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    if sp_info.stderr != "":
        print("\n%s", sp_info.stderr) 
    print(f"{sp_info.returncode=}")  


def getRC(sp_info):

    return sp_info.returncode


def getS3FileKeysList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix):
    
    S3ExtFldrNPrefix = f"{s3BktFldr}{sFilenamePrefix}"
    #rootLogger.info(f"{S3ExtFldrNPrefix=}")
    print(f"{S3ExtFldrNPrefix=}")
    
    lstKeys = [ obj.key for obj in s3_resource.Bucket(s3BUCKET).objects.filter(Prefix=S3ExtFldrNPrefix)]
    #rootLogger.info("lstKeys:\n" + "\n".join(lstKeys))
    print("lstKeys:\n" + "\n".join(lstKeys))
    
    # return filenames only 
    
    return lstKeys


def getFilenamesFromS3Keys(lstKeys, s3BktFldr):

    lstFilenames = []
    
    for sKey in lstKeys:
        print(f"{sKey=}")
        sFilename = sKey.replace(s3BktFldr,"")
        lstFilenames.append(sFilename) 

    return lstFilenames
    

def DownloadFileProgress(bytes_transferred):
    
    global giTotalDownloadBytesTransferred
    
    giTotalDownloadBytesTransferred += bytes_transferred
    
    #rootLogger.info(f"Total bytes transferred: {giTotalDownloadBytesTransferred} bytes")
    print(f"Total bytes transferred: {giTotalDownloadBytesTransferred} bytes")


def downloadFF(s3_client, s3BUCKET, s3ExtractFileKey, txtFFPathNFilename):    
   
    ################################################################
    #  NOTE: For large files --> 1 MB to 4MB is most efficient. 
    # 4 MB chunk size
    ################################################################
    iChunkSize = 4096*1024

    #rootLogger.info("Before downloading file from s3")
    print("Before downloading file from s3")
    
    ################################################################
    # Download s3 FF to linux. Download does not have 5GB limit.
    ################################################################
    with open(txtFFPathNFilename, "wb") as f:
        #rootLogger.info(f"Starting download of {s3ExtractFileKey} to {txtFFPathNFilename}")
        print(f"Starting download of {s3ExtractFileKey} to {txtFFPathNFilename}")
        
        # Reset NOF Upload Bytes transferred    
        global giTotalDownloadBytesTransferred
        giTotalDownloadBytesTransferred = 0
    
        s3_client.download_file(s3BUCKET, s3ExtractFileKey, txtFFPathNFilename, Callback=DownloadFileProgress)
        #rootLogger.info(f"The download of file {s3ExtractFileKey} to {txtFFPathNFilename} has completed.")
        print(f"The download of file {s3ExtractFileKey} to {txtFFPathNFilename} has completed.")
        
    
def deleteFileFromLinux(FilePathNFilename):                
    ################################################################
    # Delete linux file
    ################################################################
    print(f"Deleting file {FilePathNFilename} on linux server.")
    os.remove(FilePathNFilename)


def deleteFilesFromLinuxUsingPrefix(filePath, filenamePrefix):
    
    from pathlib import Path

    directory = Path(filePath)
    print(f"File Path: {directory}")
    print(f"{filenamePrefix=}")
    
    for f in directory.iterdir(): 
        if f.is_file() and f.name.startswith(filenamePrefix):
            print(f"File found = {f.name}")
            deleteFileFromLinux(f"{filePath}{f.name}")


def concatenate_files(sFilePath, lstInputFilenames, sOutputFile):

    import shutil

    print(f"{sFilePath=}")
    print(f"{lstInputFilenames=}")
    print(f"{sOutputFilename=}")
    
    
    with open(f"{sFilePath}{sOutputFilename}", "wb") as outfile:
        for sInputFilename in lstInputFilenames:
            with open(f"{sFilePath}{sInputFilename}", "rb") as infile:
                shutil.copyfileobj(infile, outfile)


def splitTextFileIntoMultipleFiles(sFilePath, sInputFilename, iNOFFiles, sOutputFilenamePrefix):

    #split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${txt_filename} ${DATADIR}/${txt_filename}_  2>> ${LOGNAME}

    print(f"{sFilePath=}")
    print(f"{sInputFilename=}")
    print(f"{iNOFFiles=}")
    print(f"{sOutputFilename=}")


    # Count total records
    with open(f"{sFilePath}{sInputFilename}", "r", encoding="utf-8") as f:
        #total_records = sum(1 for _ in f)
        iTotalInputRecs = sum(1 for line in f)
    print(f"{iTotalInputRecs=}")

    # Determine records per file
    records_per_file = iTotalInputRecs // iNOFFiles
    remainder = iTotalInputRecs % iNOFFiles

    
    lstOutputFiles = []

    # Split input file into multiple output files with suffix = "_{iNOFFile}"
    with open(f"{sFilePath}{sInputFilename}", "r", encoding="utf-8") as f:
        for i in range(iNOFFiles):
            # If last output file --> add remaining records to that file
            iOutputFileNOFRecs = records_per_file + (remainder if i == (iNOFFiles - 1) else 0)

            # Ex. outputFilename_1, outputFilename_2 
            sOutputSplitFilename = f"{sFilePath}{sOutputFilename}_{i+1}"
            with open(f"{sFilePath}{sOutputSplitFilename}", "w", encoding="utf-8") as out:
                for _ in range(iOutputFileNOFRecs):
                    line = f.readline()
                    if not line:
                        break
                    out.write(line)

            print(f"Created {sFilePath}{sOutputSplitFilename} with {iOutputFileNOFRecs} records")
            lstOutputFiles.append(sOutputSplitFilename)
            
    return lstOutputFiles
    

def unzipFile(sFilePath, sInputFilename):

    # sInputFilename filename has .gz extension

    import gzip
    import shutil

    print(f"{sFilePath=}")
    print(f"{sInputFilename=}")
    
    sUnzippedFilename = sInputFilename.replace(".gz","")
    print(f"{sUnzippedFilename=}")

    with gzip.open(f"{sFilePath}{sInputFilename}", "rb") as f_in:
        with open(f"{sFilePath}{sUnzippedFilename}", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return f"{sUnzippedFilename}"
    

def unzipFileSubprocess(sFilePath, sInputFilename):
    
    # sInputFilename filename has .gz extension
    
    print(f"{sFilePath=}")
    print(f"{sInputFilename=}")

    sp_info = subprocess.run(["gzip", "-df", f"{sFilePath}{sInputFilename}" ], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)  

    return f"{sInputFilename}"
    
    
    
def gzipFile(sFilePath, sInputFilename):

    import gzip
    import shutil
    
    print(f"{sFilePath=}")
    print(f"{sInputFilename=}")

    with open(f"{sFilePath}{sInputFilename}", "rb") as f_in:
        with gzip.open(f"{sFilePath}{sInputFilename}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            
    return f"{sInputFilename}.gz"
   

def gzipFileSubprocess(sFilePath, sInputFilename):

    print(f"{sFilePath=}")
    print(f"{sInputFilename=}")

    sp_info = subprocess.run(["gzip", "-f", f"{sFilePath}{sInputFilename}" ], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)  

    return f"{sInputFilename}.gz"


def UploadFileProgress(bytes_transferred):
    
    global giTotalUploadBytesTransferred
    
    giTotalUploadBytesTransferred += bytes_transferred
    
    print(f"Total bytes transferred: {giTotalUploadBytesTransferred} bytes")
    
    
def s3UploadFile(s3_client, sLocalPathNFilename, sBucket, sKeyPathNFilename):

    import mimetypes

    print(f"Upload {sLocalPathNFilename} to s3 bucket {sBucket} and key {sKeyPathNFilename} ")

    # Reset NOF Upload Bytes transferred    
    global giTotalUploadBytesTransferred
    giTotalUploadBytesTransferred = 0

    # override boto3 defaults for s3 upload and download file
    overrideTransferConfig = TransferConfig(
        multipart_chunksize=1024 * 1024 * 4,  # 4 MB chunks
        max_concurrency=10                    # parallel threads
    )

    # determine content type of file to upload to s3: is it "text/plain", "text/csv", "application/json", "application/gzip" 
    # determine content encoding: "gzip"     
    sContentType, sContentEncoding = mimetypes.guess_type(sLocalPathNFilename)
    print(sContentType)
    print(sContentEncoding)
    
    # build ExtraArgs dictionary object parameter
    dictExtraArgs = {}

    if sContentType:
        dictExtraArgs["ContentType"] = sContentType
    else:
        dictExtraArgs["ContentType"] = "application/octet-stream"

    if sContentEncoding:
        dictExtraArgs["ContentEncoding"] = sContentEncoding

    s3_client.upload_file(sLocalPathNFilename, sBucket, sKeyPathNFilename, Config=overrideTransferConfig, Callback=UploadFileProgress, ExtraArgs=dictExtraArgs )


def sortFileNRemoveDups(sFilePath, sInputFilename, sOutputFilename):

    print(f"{sFilePath=}")
    print(f"{sInputFilename=}")
    print(f"{sOutputFilename=}")

    sp_info = subprocess.run(["sort", "-u", f"{sFilePath}{sInputFilename}", "-o", f"{sFilePath}{sOutputFilename}"], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)  

  
  
global s3_client
s3_resource = boto3.resource('s3')
s3_client = boto3.client("s3")

DATADIR = "/app/IDRC/XTR/CMS/data/"
XTR_BUCKET  = "aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod"


sLocalPathNFilename = f"{DATADIR}sTestZipfile.txt"
sBucket = XTR_BUCKET
sKeyPathNFilename = f"xtr/DEV/Finder_Files/sTestZipfile.txt"

s3UploadFile(s3_client, sLocalPathNFilename, sBucket, sKeyPathNFilename)
    
    
sys.exit(0)
    

# test zip function
sInputFilename = "sTestZipfile.txt"
sZippedFilename = gzipFile(sFilePath = DATADIR, sInputFilename = sInputFilename)
print(f"{sZippedFilename=}")

# Test unzip function
sInputFilename = "sTestZipfile.txt.gz"
sUnzippedFilename = unzipFile(sFilePath = DATADIR, sInputFilename = sInputFilename)
print(f"{sUnzippedFilename=}")

print("\ntesting subprocess versions")

# test zip function
sInputFilename = "sTestZipfile.txt"
sZippedFilename = gzipFileSubprocess(sFilePath = DATADIR, sInputFilename = sInputFilename)
print(f"{sZippedFilename=}")

# Test unzip function subprocess
sInputFilename = "sTestZipfile.txt.gz"
sUnzippedFilename = unzipFileSubprocess(sFilePath = DATADIR, sInputFilename = sInputFilename)
print(f"{sUnzippedFilename=}")


sys.exit(0)

# Example sort

sInputFilename = "DOD_NPI_Ext_20260123.101554.txt"
sOutputFilename = "DOD_NPI_Ext_sorted.txt"
sortFileNRemoveDups(sFilePath = DATADIR, sInputFilename = sInputFilename, sOutputFilename = sOutputFilename)


lstSplitFiles = splitTextFileIntoMultipleFiles(sFilePath = DATADIR, sInputFilename = "SingleInputFile.txt", iNOFFiles = 4, sOutputFilename = "OutputSplitFile.txt" )
lstSplitFiles = ["OutputSplitFile.txt_1", "OutputSplitFile.txt_2", "OutputSplitFile.txt_3", "OutputSplitFile.txt_4"]

for sSplitFilename in lstSplitFiles:
    gzipFile(sFilePath = DATADIR, sInputFilename = sSplitFilename)

#s3BUCKET = "aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod"

#s3BktFldr =  "xtr/DEV/Finder_Files/"
#sFilenamePrefix = "TRICARE_FNDR"
#sFilenamePrefix = "NO_FNDR"

#lstFileKeys = getS3FileList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix)
#print(f"{lstFileKeys=}")

#iNOFFiles = len(lstFileKeys)
#print(f"{iNOFFiles=}")

#lstFilenames = getFilenamesFromS3Keys(lstFileKeys, s3BktFldr)
#print(f"{lstFilenames=}")

#s3ExtractFileKey = "xtr/DEV/Finder_Files/archive/TRICARE_FNDR_20230911_123000.txt"

#txtFFPathNFilename = "/app/IDRC/XTR/CMS/data/TRICARE_FNDR_20230911_123000.txt"
        
#downloadFF(s3_client, s3BUCKET, s3ExtractFileKey, txtFFPathNFilename)     


#DATADIR = "/app/IDRC/XTR/CMS/data/"
#lstInputFilenames = ["file1.txt","file2.txt","file3.txt"]
#sOutputFilename = "sConcatFile"

#concatenate_files(sPath, lstInputFilenames, sOutputFilename)
#concatenate_files(DATADIR, lstInputFilenames, sOutputFilename)


#filePath = "/app/IDRC/XTR/CMS/data/"
#filePrefix = "tmp"
#deleteFilesFromLinuxUsingWildcard(filePath, filePrefix)

#filePath = "/app/IDRC/XTR/CMS/data/"
#filePrefix = "temp_"
#deleteFilesFromLinuxUsingWildcard(filePath, filePrefix)
