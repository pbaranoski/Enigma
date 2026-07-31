######################################################################################
# Name: CommonFunctions.py
#
# Desc: Common functions module.
#
# NOTE: To use any of the functions in the CommonFunctions.py module, 
#       include at the top of your module with the other python import statements: 
#
#       "from CommonFunctions import * ".
#
#       In addition, after establishing the Logger in your python module, include the below code:
#
#        # Establish logger with CommonFunctions module.
#        setCommonFunctionLogger(rootLogger)  --> where "rootLogger" is the logger name in your python module.
#
# Modified: 
#
# Paul Baranoski 2026-03-31 Created Module.
# Paul Baranoski 2026-04-02 Added function getConfigFile.
# Paul Baranoski 2026-05-20 Modify Log message for s3UploadFile function.
# Paul Baranoski 2026-06-24 Added new function findRecsContainingSearchText which is like a grep on a file.
# Paul Baranoski 2026-07-27 Added new function deleteS3FilesUsingPrefix.
# Paul Baranoski 2026-07-28 Added new function sendEmail.
# Paul Baranoski 2026-07-30 Add filter to function getExtFiles4RequestList to not include files in "archive" folder.
######################################################################################

########################################################################################################
# IMPORTS
########################################################################################################
import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import os
import sys
import argparse
import re
import io

import subprocess

import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data"

from datetime import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


#############################################################
# Functions
#############################################################
def setCommonFunctionLogger(pRootLogger):

    # Pass the logger once instead of for each function
    global rootLogger
    rootLogger = pRootLogger


def write_sp_info_2_log(sp_info):
        
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    if sp_info.stdout != "":
        rootLogger.info("\n%s", sp_info.stdout) 
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    if sp_info.stderr != "":
        rootLogger.info("\n%s", sp_info.stderr) 
    rootLogger.info(f"{sp_info.returncode=}")  


def getRC(sp_info):

    return sp_info.returncode

    
def isValidDate(sDate2Validate, sFormat):

    try:

        datetime_obj = datetime.strptime(sDate2Validate, sFormat)
        return True
    
    except Exception as ex:
        rootLogger.info(f"Invalid date or date format: {ex}")
        
        return False    


def s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    # Copy object, then delete to "move" file.
    rootLogger.info(f"Moving {sSourceKey} to {sDestinationKey} in {sSourceBucket}.")

    # Note: copy_object has a 5 GB limit. 
    s3_client.copy_object(
        Bucket=sSourceBucket,
        CopySource={"Bucket": sSourceBucket, "Key": sSourceKey},
        Key=sDestinationKey
    )

    rootLogger.info(f"Deleting {sSourceKey} in {sSourceBucket} as part of move operation (Copy/delete).")

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)

   
def s3MoveLargeFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    from boto3.s3.transfer import S3Transfer, TransferConfig

    # Copy object, then delete to "move" file.
    rootLogger.info(f"Moving {sSourceKey} to {sDestinationKey} in {sSourceBucket}.")

    # Set configuration values for transfer
    config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,   # switch to multipart above 100 MB
        multipart_chunksize=100 * 1024 * 1024,   # each part = 100 MB
        max_concurrency=10,                      # threads
        use_threads=True
    )

    # Note: size limit is in the TBs
    s3_client.copy(
        {"Bucket": sSourceBucket, "Key": sSourceKey},
        sSourceBucket,
        sDestinationKey,
        Config=config )

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)


def getConfigFile(s3_client, S3BUCKET, s3ConfigFolder_n_filename):
    
    ##################################################################
    # Retrieve config file from S3 (copy)
    ##################################################################
    rootLogger.info("")
    rootLogger.info(f"Get Config file {s3ConfigFolder_n_filename} from S3")
    
    s3ConfigFile = s3_client.get_object(Bucket=S3BUCKET, Key=s3ConfigFolder_n_filename)

    if  s3ConfigFile == None:
        rootLogger.error(f"Config file {s3ConfigFolder_n_filename} is not in S3.")
       
        raise Exception(f"Config file {s3ConfigFolder_n_filename} is not in S3.")        


    ########################################################################
    # S3 Body is byte array. Convert byte array to utf-8 string. 
    # Splitlines recognizes "\r\n" as end-of-record markers     
    ########################################################################
    lstConfigRecs = s3ConfigFile["Body"].read().decode('utf-8').splitlines()
    rootLogger.info("\n%s\n", "\n".join(lstConfigRecs)) 
    
    return lstConfigRecs

    
def DownloadFileProgress(bytes_transferred):
    
    global giTotalDownloadBytesTransferred
    
    giTotalDownloadBytesTransferred += bytes_transferred
    
    rootLogger.info(f"Total bytes transferred: {giTotalDownloadBytesTransferred} bytes")


def downloadFileFromS3(s3_client, s3BUCKET, s3ExtractFileKey, txtFFPathNFilename):    
   
    ################################################################
    #  NOTE: For large files --> 1 MB to 4MB is most efficient. 
    # 4 MB chunk size
    ################################################################
    iChunkSize = 4096*1024

    rootLogger.info(f"Starting download of {s3ExtractFileKey} to {txtFFPathNFilename}")
    
    ################################################################
    # Download s3 FF to linux. Download does not have 5GB limit.
    ################################################################
    with open(txtFFPathNFilename, "wb") as f:
        # Reset NOF Download Bytes transferred    
        global giTotalDownloadBytesTransferred
        giTotalDownloadBytesTransferred = 0
    
        s3_client.download_file(s3BUCKET, s3ExtractFileKey, txtFFPathNFilename, Callback=DownloadFileProgress)
        rootLogger.info(f"The download of file {s3ExtractFileKey} to {txtFFPathNFilename} has completed.")


def UploadFileProgress(bytes_transferred):
    
    global giTotalUploadBytesTransferred
    
    giTotalUploadBytesTransferred += bytes_transferred
    
    rootLogger.info(f"Total bytes transferred: {giTotalUploadBytesTransferred} bytes")
    
    
def s3UploadFile(s3_client, sLocalPathNFilename, sBucket, sKeyPathNFilename):

    import mimetypes

    rootLogger.info(f"Upload {sLocalPathNFilename} to s3 bucket {sBucket} and key {sKeyPathNFilename} ")

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
    rootLogger.info(f"{sContentType=}")
    rootLogger.info(f"{sContentEncoding=}")
    
    # build ExtraArgs dictionary object parameter
    dictExtraArgs = {}

    if sContentType:
        dictExtraArgs["ContentType"] = sContentType
    else:
        dictExtraArgs["ContentType"] = "application/octet-stream"

    if sContentEncoding:
        dictExtraArgs["ContentEncoding"] = sContentEncoding

    s3_client.upload_file(sLocalPathNFilename, sBucket, sKeyPathNFilename, Config=overrideTransferConfig, Callback=UploadFileProgress, ExtraArgs=dictExtraArgs )


def archiveFinderFile(s3_client, sSourceBucket, sFinderFileBktFldr, FF):

    sSourceKey = f"{sFinderFileBktFldr}{FF}"
    sDestinationKey  = f"{sFinderFileBktFldr}archive/{FF}"

    #############################################################
    # Move Finder File in S3 to archive folder
    #############################################################
    rootLogger.info(f"Moving S3 Finder file {FF} to S3 archive folder.")

    s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey)


def getS3FileKeysList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix):

    rootLogger.info("")
    rootLogger.info(f"{s3BUCKET=}")
    rootLogger.info(f"{s3BktFldr=}")
    rootLogger.info(f"{sFilenamePrefix=}")

    S3ExtFldrNPrefix = f"{s3BktFldr}{sFilenamePrefix}"
    rootLogger.info(f"{S3ExtFldrNPrefix=}")

    lstKeys = [ obj.key for obj in s3_resource.Bucket(s3BUCKET).objects.filter(Prefix=S3ExtFldrNPrefix)]
    rootLogger.info("lstKeys:\n" + "\n".join(lstKeys))

    return lstKeys


def getFilenamesFromS3Keys(lstKeys, s3BktFldr):

    rootLogger.info(f"{s3BktFldr=}")
    
    lstFilenames = []
    
    for sKey in lstKeys:
        rootLogger.info(f"{sKey=}")
        sFilename = sKey.replace(s3BktFldr,"")
        lstFilenames.append(sFilename) 
        rootLogger.info(f"{sFilename=}")
        
    return lstFilenames


def s3GetMostRecentFileKeySubProcess(s3BUCKET, s3BktFldr, sFilenamePrefix):

    rootLogger.info("")
    rootLogger.info(f"{s3BUCKET=}")
    rootLogger.info(f"{s3BktFldr=}")
    rootLogger.info(f"{sFilenamePrefix=}")

    S3ExtFldrNPrefix = f"{s3BktFldr}{sFilenamePrefix}"
    rootLogger.info(f"{S3ExtFldrNPrefix=}")

    # [-1] --> get most recent item that matches prefix
    cmd = [
        "aws",
        "s3api",
        "list-objects-v2",
        "--bucket", s3BUCKET,
        "--prefix", S3ExtFldrNPrefix,
        "--query", 'sort_by(Contents,&LastModified)[-1].Key'
    ]

    # make the api call
    try:
        # use the s3 api to query for results 
        sp_info = subprocess.run(cmd, capture_output=True, text=True, check=True) 
        #write_sp_info_2_log(sp_info)         

    except Exception as ex:
        rootLogger.info(f"No file was found. {ex}")
        
        # re-raise exception
        raise   
    
    # get the most recent key
    # This is an example. --> stdout='"xtr/DEV/PSPS/archive/PSPS_Extract_Q6_20260423.140041.txt.gz"\n'
    sMostRecentS3Key = sp_info.stdout.strip().strip('"').strip("'")

    rootLogger.info(sp_info)
    rootLogger.info(f"{sMostRecentS3Key=}")
    
    return sMostRecentS3Key


def deleteFileFromLinux(FilePathNFilename):                
    ################################################################
    # Delete linux file
    ################################################################
    try:
        rootLogger.info(f"Deleting file {FilePathNFilename} on linux server.")
        os.remove(FilePathNFilename)
        
    except FileNotFoundError:
        rootLogger.warning(f"File {FilePathNFilename} not found.")
    
    except Exception as e:
        rootLogger.error(f"Error deleting file {{FilePathNFilename}}. {e}")
        # re-raise exception
        raise
        
def deleteFilesFromLinuxUsingPrefix(filePath, filenamePrefix):
    
    from pathlib import Path

    directory = Path(filePath)
    rootLogger.info(f"File Path: {directory}")
    rootLogger.info(f"{filenamePrefix=}")
    
    for f in directory.iterdir(): 
        if f.is_file() and f.name.startswith(filenamePrefix):
            rootLogger.info(f"File found = {f.name}")
            deleteFileFromLinux(f"{filePath}{f.name}")


def getRangeOfRecords(sFilenameNPath, iLineFrom, iLineTo):
        
    from itertools import islice
    from io import StringIO
    
    sioRangeOfRecs = StringIO("")

    rootLogger.info(f"{sFilenameNPath=}")
    rootLogger.info(f"{iLineFrom=}")
    rootLogger.info(f"{iLineTo=}")
        
    with open(sFilenameNPath, "r", encoding="utf-8") as f:
        # islice indexing is zero-based; and end-line is non-inclusive
        for line in islice(f, iLineFrom - 1, iLineTo):
            sioRangeOfRecs.write(line)

    return sioRangeOfRecs.getvalue()
   

def wc_l(sPathNFilename):

    rootLogger.info(f"{sPathNFilename=}")
    
    with open(sPathNFilename, "r", encoding="utf-8", errors="ignore") as f:
       return sum(1 for line in f)


def getFileByteCount(sPathNFilename):

    from pathlib import Path
    
    rootLogger.info(f"{sPathNFilename=}")
    
    path = Path(sPathNFilename)
    iByteCount = path.stat().st_size

    return iByteCount


def wc_cm_largefile(PathNFilename, chunk_size=1024*1024):  
    
    # 1 MB chunks
    byte_count = 0
    char_count = 0
    
    rootLogger.info(f"{PathNFilename=}")

    with open(PathNFilename, "rb") as f:
        while chunk := f.read(chunk_size):
            byte_count += len(chunk)
            char_count += len(chunk.decode("utf-8", errors="ignore"))

    return byte_count, char_count


def ls_using_filename_pattern(sPath, sFilenamePattern):

    from pathlib import Path

    rootLogger.info(f"{sPath=}")
    rootLogger.info(f"{sFilenamePattern=}")

    lstFilesMatchingPattern = [f.name for f in Path(sPath).glob(sFilenamePattern)  ]
    # sort the list of files
    lstFilesMatchingPattern.sort()

    return lstFilesMatchingPattern
    

def concatenate_files(sPath, lstInputFilenames, sOutputFilename):

    from pathlib import Path
    import shutil

    rootLogger.info(f"{sPath=}")
    rootLogger.info(f"{lstInputFilenames=}")
    rootLogger.info(f"{sOutputFilename=}")
    
    
    with open(f"{sPath}{sOutputFilename}", "wb") as outfile:
        for sInputFilename in lstInputFilenames:
            with open(f"{sPath}{sInputFilename}", "rb") as infile:
                shutil.copyfileobj(infile, outfile)
    

def getExtFiles4RequestList(s3_resource, sSourceBucket, S3KeyPrefix, sTimeStamp):  
   
    #S3ExtFndrFldrNPrefix = FINDER_FILE_BUCKET_FLDR + PREFIX
    rootLogger.info("")
    rootLogger.info(f"{sSourceBucket=}")
    rootLogger.info(f"{S3KeyPrefix=}")
    rootLogger.info(f"{sTimeStamp=}")

    #############################################################
    # Get list of S3 Keys to include in manifest.
    # NOTE: exclude files in "archive" folder.
    #############################################################
    rootLogger.info("Get list of Extract Files for Request. ")

    lstExtFiles4Request = [ obj.key for obj in s3_resource.Bucket(sSourceBucket).objects.filter(Prefix=S3KeyPrefix) if sTimeStamp in obj.key and not "/archive/" in obj.key ]

    return lstExtFiles4Request


def splitTextFileIntoMultipleFiles(sInputFileNPath, iNOFFiles, sOutputFileNPath):

    import shutil
    import os

    #split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${txt_filename} ${DATADIR}/${txt_filename}_  2>> ${LOGNAME}

    rootLogger.info(f"{sInputFileNPath=}")
    rootLogger.info(f"{iNOFFiles=}")
    rootLogger.info(f"Output path and filename prefix: {sOutputFileNPath}")


    # Count total records
    with open(sInputFileNPath, "r", encoding="utf-8") as f:
        #total_records = sum(1 for _ in f)
        iTotalInputRecs = sum(1 for line in f)
        
    rootLogger.info("")
    rootLogger.info(f"{iTotalInputRecs=}")

    # Determine records per file
    records_per_file = iTotalInputRecs // iNOFFiles
    remainder = iTotalInputRecs % iNOFFiles

    rootLogger.info(f"{records_per_file=}")
    
    # Check system disk usage before splitting the file
    rootLogger.info("")

    sOutputDir = os.path.dirname(sOutputFileNPath)
    usage = shutil.disk_usage(sOutputDir)
    rootLogger.info(f"Free: {usage.free / 1024**3:.1f} GB")

    
    lstOutputFilesNPaths = []

    # Split input file into multiple output files with suffix = "_{iNOFFile}"
    with open(sInputFileNPath, "r", encoding="utf-8") as f:
        for i in range(iNOFFiles):
            # If last output file --> add remaining records to that file
            iOutputFileNOFRecs = records_per_file + (remainder if i == (iNOFFiles - 1) else 0)

            # Ex. outputFilename_1, outputFilename_2 
            sOutputSplitFileNPath = f"{sOutputFileNPath}_{i+1}"
            with open(sOutputSplitFileNPath, "w", encoding="utf-8") as out:
                for iRecNo in range(iOutputFileNOFRecs):
                    line = f.readline()
                    if not line:
                        break
                    
                    # Check available space every 1 million records
                    if (iRecNo % 1000000) == 0:
                        usage = shutil.disk_usage(sOutputDir)
                        rootLogger.info(f"Remaining: {usage.free / 1024**3:.1f} GB")

                    out.write(line)

            rootLogger.info(f"Created {sOutputSplitFileNPath} with {iOutputFileNOFRecs} records")
            lstOutputFilesNPaths.append(sOutputSplitFileNPath)
            
    return lstOutputFilesNPaths


def splitTextFileIntoMultipleHCPCSFiles(sInputFilenameNPath, sOutputPathNFilenamePrefix):

    from contextlib import ExitStack
    
    rootLogger.info(f"{sInputFilenameNPath=}")
    rootLogger.info(f"{sOutputPathNFilenamePrefix=}")
    
    # Get timestamp to use for all Output HCPCS files
    tmstmp = datetime.now().strftime('%Y%m%d.%H%M%S')
    rootLogger.info(f"{tmstmp=}")

    # Define ranges and HCPCS output filenames
    ranges = [
        ("0000 ", "09999", f"{sOutputPathNFilenamePrefix}01_{tmstmp}.txt"),
        ("1000 ", "14999", f"{sOutputPathNFilenamePrefix}02_{tmstmp}.txt"),
        ("1500 ", "19999", f"{sOutputPathNFilenamePrefix}03_{tmstmp}.txt"),
        ("2000 ", "24999", f"{sOutputPathNFilenamePrefix}04_{tmstmp}.txt"),
        ("2500 ", "29999", f"{sOutputPathNFilenamePrefix}05_{tmstmp}.txt"),
        ("3000 ", "32999", f"{sOutputPathNFilenamePrefix}06_{tmstmp}.txt"),
        ("3300 ", "37999", f"{sOutputPathNFilenamePrefix}07_{tmstmp}.txt"),
        ("3800 ", "38999", f"{sOutputPathNFilenamePrefix}08_{tmstmp}.txt"),
        ("3900 ", "39999", f"{sOutputPathNFilenamePrefix}09_{tmstmp}.txt"),
        ("4000 ", "49999", f"{sOutputPathNFilenamePrefix}10_{tmstmp}.txt"),
        ("5000 ", "53999", f"{sOutputPathNFilenamePrefix}11_{tmstmp}.txt"),
        ("5400 ", "55999", f"{sOutputPathNFilenamePrefix}12_{tmstmp}.txt"),
        ("5600 ", "58999", f"{sOutputPathNFilenamePrefix}13_{tmstmp}.txt"),
        ("5900 ", "59999", f"{sOutputPathNFilenamePrefix}14_{tmstmp}.txt"),
        ("6000 ", "64999", f"{sOutputPathNFilenamePrefix}15_{tmstmp}.txt"),
        ("6500 ", "68999", f"{sOutputPathNFilenamePrefix}16_{tmstmp}.txt"),
        ("6900 ", "69999", f"{sOutputPathNFilenamePrefix}17_{tmstmp}.txt"),
        ("7000 ", "74999", f"{sOutputPathNFilenamePrefix}18_{tmstmp}.txt"),
        ("7500 ", "79999", f"{sOutputPathNFilenamePrefix}19_{tmstmp}.txt"),
        ("8000 ", "89999", f"{sOutputPathNFilenamePrefix}20_{tmstmp}.txt"),
        ("9000 ", "99199", f"{sOutputPathNFilenamePrefix}21_{tmstmp}.txt"),
        ("9920 ", "99999", f"{sOutputPathNFilenamePrefix}22_{tmstmp}.txt"),
        ("A000 ", "H9999", f"{sOutputPathNFilenamePrefix}23_{tmstmp}.txt"),
        ("J000 ", "Z9999", f"{sOutputPathNFilenamePrefix}24_{tmstmp}.txt"),
        ("UNK  ", "UNK  ", f"{sOutputPathNFilenamePrefix}25_{tmstmp}.txt"),
        ("     ", "     ", f"{sOutputPathNFilenamePrefix}26_{tmstmp}.txt")
    ]

    ###############################################################################
    # Read input file and write to appropriate Output HCPCS file.
    # The With is good for all output files so files are automatically closed.
    ###############################################################################
    with ExitStack() as stack:

        fpOutputFiles = []

        # Create list of FilePointers and HCPCS Ranges
        for low, high, filename in ranges:
            fp = stack.enter_context(open(filename, "w"))
            fpOutputFiles.append((low, high, fp))
        
        # Process input file and write to appropriate Output file
        with open(sInputFilenameNPath) as infile:

            for record in infile:

                # Extract HCPCS_CD from input record.
                sHCPCS_CD = record[0:5]
                #print(f"{sHCPCS_CD=}")

                # If HCPCS_CD is a tilda or "UNK  " --> write to "25_" file; get next input record
                if sHCPCS_CD == "~    " or sHCPCS_CD == "UNK  ":
                    fp = fpOutputFiles[24][2]
                    fp.write(record)
                    
                    # go to next input record
                    continue

                swOutFileFnd = 'N'
                # search list of output files that match HCPCS_CD range
                for low, high, fp in fpOutputFiles:
                    
                    if low <= sHCPCS_CD <= high:
                        fp.write(record)
                        swOutFileFnd = 'Y'
                        break

                # If HCPCS_CD doesn't match any of the ranges --> write to "26_" file; 
                if swOutFileFnd == 'N':
                    fp = fpOutputFiles[25][2]
                    fp.write(record)
                    
                    # go to next input record
                    continue
                        
                        
def unzipFile(sFilePath, sInputFilename):

    # sInputFilename filename has .gz extension

    import gzip
    import shutil

    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")
    
    sUnzippedFilename = sInputFilename.replace(".gz","")
    rootLogger.info(f"{sUnzippedFilename=}")

    with gzip.open(f"{sFilePath}{sInputFilename}", "rb") as f_in:
        with open(f"{sFilePath}{sUnzippedFilename}", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            
    # delete gzip file to duplicate default command line gzip logic where .gzip file . replaced by unzipped file
    deleteFileFromLinux(f"{sFilePath}{sInputFilename}")
    
    return f"{sUnzippedFilename}"
    

def unzipFileSubprocess(sFilePath, sInputFilename):
    
    # sInputFilename filename has .gz extension
    
    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")

    sp_info = subprocess.run(["gzip", "-df", f"{sFilePath}{sInputFilename}" ], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)  

    # delete gzip file to duplicate default command line gzip logic where .gzip file . replaced by unzipped file
    deleteFileFromLinux(f"{sFilePath}{sInputFilename}")

    sUnzippedFilename = sInputFilename.replace(".gz","")
    
    return f"{sUnzippedFilename}"
    
    
def gzipFile(sFilePath, sInputFilename):

    import gzip
    import shutil
    
    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")

    with open(f"{sFilePath}{sInputFilename}", "rb") as f_in:
        with gzip.open(f"{sFilePath}{sInputFilename}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
            
    return f"{sInputFilename}.gz"
   

def gzipFileSubprocess(sFilePath, sInputFilename):

    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")

    sp_info = subprocess.run(["gzip", "-f", f"{sFilePath}{sInputFilename}" ], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)  

    return f"{sInputFilename}.gz"


def sortFileNRemoveDups(sFilePath, sInputFilename, sOutputFilename):

    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")
    rootLogger.info(f"{sOutputFilename=}")

    sp_info = subprocess.run(["sort", "-u", f"{sFilePath}{sInputFilename}", "-o", f"{sFilePath}{sOutputFilename}"], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)  


def loadVariableWithFileContents(sFilePath, sInputFilename):

    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")    

    with open(f"{sFilePath}{sInputFilename}", "r", encoding="utf-8") as f:
        sContent = f.read()
    
    return sContent    


def createCSVFileFromDelimitedFile(sPath, sInputFilename, sOutputFilename, sInputFileDelimiter):
    
    import csv

    rootLogger.info(f"{sPath=}")
    rootLogger.info(f"{sInputFilename=}")
    rootLogger.info(f"{sOutputFilename=}")
    rootLogger.info(f"{sInputFileDelimiter=}")
    
    # the 'newline="" ' parameter says leave the file's newline character as-is (\r\n or \n) 
    with open(f"{sPath}{sInputFilename}", "r", newline="", encoding="utf-8") as infile, \
         open(f"{sPath}{sOutputFilename}", "w", newline="", encoding="utf-8") as outfile:

        reader = csv.reader(infile, delimiter=sInputFileDelimiter)
        writer = csv.writer(outfile, delimiter=",")

        for row in reader:
            writer.writerow(row)


def findRecsContainingSearchText(sPath: str, sInputFilename: str, sSearchString: str) -> list:             
    # In essence a grep on a file using sSearchString

    rootLogger.info(f"{sPath=}")
    rootLogger.info(f"{sInputFilename=}")
    rootLogger.info(f"{sSearchString=}")
    
    lstRecsContainingSearchText = []
    
    with open(f"{sPath}{sInputFilename}", "r", newline="", encoding="utf-8") as infile:
        line = infile.readline()
        for line in infile:
            if line.find(sSearchString) >= 0:
                lstRecsContainingSearchText.append(line)
            
    return lstRecsContainingSearchText        


def deleteS3FilesUsingPrefix(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix):             
            
    rootLogger.info("")
    rootLogger.info(f"{s3BUCKET=}")
    rootLogger.info(f"{s3BktFldr=}")
    rootLogger.info(f"{sFilenamePrefix=}")
    
    s3_client = boto3.client("s3")

    # Get list of s3 Keys that match prefix
    lstS3KeysUsingPrefix = getS3FileKeysList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix)
    
    lstS3Keys2Delete = []
    
    for s3PartsFilenameKey in lstS3KeysUsingPrefix:
        lstS3Keys2Delete.append({'Key': s3PartsFilenameKey})

    # Mass delete of objects. This is faster than deleting one at a time.
    if lstS3Keys2Delete:
        rootLogger.info("s3 Keys to Delete: \n%s\n", "\n".join(lstS3KeysUsingPrefix))
        s3_client.delete_objects(Bucket=s3BUCKET, Delete={'Objects': lstS3Keys2Delete})


def sendEmail(sender, receivers, SUBJECT, MSG): 

    import sendEmail as EmailDr
    
    import io
    from contextlib import redirect_stdout, redirect_stderr

    buf = io.StringIO()

    with redirect_stdout(buf),redirect_stderr(buf):
        EmailDr.sendEmailNamedParms(sender=sender, receivers=receivers, subject=SUBJECT, messageText=MSG)

    # Write captured stdout to our log file
    rootLogger.info(buf.getvalue())    
 
            