#!/usr/bin/env python
########################################################################################################
# Name: LOAD_TRICARE_FNDR_FILE_Driver.py
# DESC: This script uploads theTRICARE  Monthly finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MNNUP_FF table.
#       Create to download Finder Files from S3:/Finder_Files bucket and load into Finder file table.
#
# Vijay Mandavilli   2026-02-23 Create Module.
#
########################################################################################################
# IMPORTS
########################################################################################################

import os
os.environ["TESTING"] = "Y"

# Our common module with variable constants
from SET_XTR_ENV import *

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

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog

S3BUCKET = rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"
COMBINED_TRICARE_FNDR_FILE= "TricareCombinedFinderFile"
SORTED_COMBINED_TRICARE_FNDR_FILE= "TricareCombinedFinderFileSorted.txt"

DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

s3BUCKET = rf"{XTR_BUCKET}"
s3BktFldr =  rf"{FINDER_FILE_BUCKET_FLDR}"
sFilenamePrefix = "TRICARE_FNDR"
sFilenamePrefixCombnd = "TricareCombinedFinderFile.txt"


#############################################################
# Functions
#############################################################
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


def s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    # Copy object, then delete to "move" file.
    
    s3_client.copy_object(
        Bucket=sSourceBucket,
        CopySource={"Bucket": sSourceBucket, "Key": sSourceKey},
        Key=sDestinationKey
    )

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)


def getConfigFile(s3_client, S3BUCKET, s3ConfigFolder_n_filename):
    
    ##################################################################
    # Retrieve config file from S3 (copy)
    ##################################################################
    rootLogger.info("")
    rootLogger.info(f"Get Config file {s3ConfigFolder_n_filename} from S3")
    
    s3ConfigFile = s3_client.get_object(Bucket=S3BUCKET, Key=s3ConfigFolder_n_filename)

    if  s3ConfigFile == None:
        ## Send Failure email
        SUBJECT = f"LOAD_TRICARE_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
        MSG = f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
        
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
        raise Exception(f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed.")        


    ########################################################################
    # S3 Body is byte array. Convert byte array to utf-8 string. 
    # Splitlines recognizes "\r\n" as end-of-record markers     
    ########################################################################
    lstConfigRecs = s3ConfigFile["Body"].read().decode('utf-8').splitlines()
    rootLogger.info("\n%s\n", "\n".join(lstConfigRecs)) 
    
    return lstConfigRecs

            
def getS3FileList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix):
    
    rootLogger.info("")
    rootLogger.info(f"{s3BUCKET=}")
    rootLogger.info(f"{s3BktFldr=}")
    rootLogger.info(f"{sFilenamePrefix=}")

    S3ExtFldrNPrefix = f"{s3BktFldr}{sFilenamePrefix}"
    rootLogger.info(f"{S3ExtFldrNPrefix=}")
    #print(f"{S3ExtFldrNPrefix=}")
    
    lstKeys = [ obj.key for obj in s3_resource.Bucket(s3BUCKET).objects.filter(Prefix=S3ExtFldrNPrefix)]
    rootLogger.info("lstKeys:\n" + "\n".join(lstKeys))
    #print("lstKeys:\n" + "\n".join(lstKeys))
    
    return lstKeys

def DownloadFileProgress(bytes_transferred):
    
    global giTotalDownloadBytesTransferred
    
    giTotalDownloadBytesTransferred += bytes_transferred
    
    rootLogger.info(f"Total bytes transferred: {giTotalDownloadBytesTransferred} bytes")
    #print(f"Total bytes transferred: {giTotalDownloadBytesTransferred} bytes")


def downloadFF(s3_client, s3BUCKET, s3ExtractFileKey, txtFFPathNFilename):    
#def downloadFF(s3ExtractFileKey, txtFFPathNFilename):   

    ################################################################
    #  NOTE: For large files --> 1 MB to 4MB is most efficient. 
    # 4 MB chunk size
    ################################################################
    iChunkSize = 4096*1024

    rootLogger.info("Before downloading file from s3")
    #print("Before downloading file from s3")
    
    ################################################################
    # Download s3 FF to linux. Download does not have 5GB limit.
    ################################################################
    with open(txtFFPathNFilename, "wb") as f:
        rootLogger.info(f"Starting download of {s3ExtractFileKey} to {txtFFPathNFilename}")
        #print(f"Starting download of {s3ExtractFileKey} to {txtFFPathNFilename}")
        
        # Reset NOF Upload Bytes transferred    
        global giTotalDownloadBytesTransferred
        giTotalDownloadBytesTransferred = 0
    
        s3_client.download_file(s3BUCKET, s3ExtractFileKey, txtFFPathNFilename, Callback=DownloadFileProgress)
        #s3_client.download_file(s3ExtractFileKey, txtFFPathNFilename, Callback=DownloadFileProgress)
        rootLogger.info(f"The download of file {s3ExtractFileKey} to {txtFFPathNFilename} has completed.")
        #print(f"The download of file {s3ExtractFileKey} to {txtFFPathNFilename} has completed.")
        
def deleteFileFromLinux(FilePathNFilename):  

    ################################################################
    # Delete linux file
    ################################################################
    rootLogger.info(f"Deleting file {FilePathNFilename} on linux server.")
    os.remove(FilePathNFilename)


def deleteFilesFromLinuxUsingPrefix(filePath, filenamePrefix):
    
    from pathlib import Path

    directory = Path(filePath)
    rootLogger.info(f"File Path: {directory}")
    rootLogger.info(f"{filenamePrefix=}")
    
    for f in directory.iterdir(): 
        if f.is_file() and f.name.startswith(filenamePrefix):
            rootLogger.info(f"File found = {f.name}")
            deleteFileFromLinux(f"{filePath}{f.name}")


def getFilenamesFromS3Keys(lstKeys, s3BktFldr):

    lstFilenames = []
    
    for sKey in lstKeys:
        rootLogger.info(f"{sKey=}")
        sFilename = sKey.replace(s3BktFldr,"")
        lstFilenames.append(sFilename) 

    return lstFilenames


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


def sortFileNRemoveDups(sFilePath, sInputFilename, sOutputFilename):

    rootLogger.info(f"{sFilePath=}")
    rootLogger.info(f"{sInputFilename=}")
    rootLogger.info(f"{sOutputFilename=}")

    sp_info = subprocess.run(["sort", "-u", f"{sFilePath}{sInputFilename}", "-o", f"{sFilePath}{sOutputFilename}"], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info) 


def main_processing_loop():

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        global rootLogger
        
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}{TESTLOG}LOAD_TRICARE_FNDR_FILE_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)

        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")
       
        ##########################################
        # Were the correct NOF parameters sent? 
        ##########################################
        iNOFParms = len(sys.argv) - 1
        if not (iNOFParms == 0 or iNOFParms ==  1):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")

            
        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
        
        ##########################################
        # Set S3 Bucket-- 
        ##########################################

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        #################################################################
        # Section 1: LOAD_TRICARE_FNDR_FILE_Driver.py logic
        #################################################################    

        rootLogger.info(f"{s3BktFldr=}")  
        rootLogger.info(f"{sFilenamePrefix=}") 
        rootLogger.info(f"{sFilenamePrefixCombnd=}")

        #############################################################
        # Remove any residual TRICARE Finder files in data directory.  **check with Paul
        #############################################################
        rootLogger.info("Remove any residual Finder Files in data directory.")
        
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, sFilenamePrefix)
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, COMBINED_TRICARE_FNDR_FILE)

        #################################################################################
        # Find TRICARE Finder Files in S3
        #################################################################################
        rootLogger.info("Find TRICARE Finder Files in S3.")

        lstFileKeys = getS3FileList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix)
        rootLogger.info(f"{lstFileKeys=}")

        # if zero files found --> end script
        iNOFFiles = len(lstFileKeys)
        rootLogger.info(f"{iNOFFiles=}")

        if iNOFFiles == 0:
            ## Send Failure email	- commented below code, as the email will be sent from TRICARE_Extracts_Driver.py
            #SUBJECT=f"LOAD_TRICARE_FNDR_FILE_Driver.py script - No Finder files found ({ENVNAME})"
            #MSG=f"No TRICARE finder files found in S3 folder {FINDER_FILE_BUCKET_FLDR}{sFilenamePrefix}."

            #sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            #write_sp_info_2_log(sp_info)  

            sys.exit(4)


        #################################################################################
        # Copy TRICARE Finder Files in S3 to linux data dir (Could be as many as 20).
        #################################################################################
        rootLogger.info(f"Copy S3 TRICARE Finder Files to linux data directory")
##\/
        lstFilenames = getFilenamesFromS3Keys(lstFileKeys, FINDER_FILE_BUCKET_FLDR)
    
        for S3FinderFilename in lstFilenames:
            #aws s3 cp S3filename linux-filename
            downloadFF(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{S3FinderFilename}", f"{DATA_DIR}{S3FinderFilename}" ) 
            rootLogger.info(f"{S3FinderFilename=} Copy from {FINDER_FILE_BUCKET_FLDR} to {DATA_DIR} directory")   
##/\    
        #downloadFF({s3_client},{S3BUCKET},f"{S3BUCKET}{PREFIX}",f"{DATA_DIR}{PREFIX}")
        #(f"{S3BUCKET}{PREFIX}",f"{DATA_DIR}{PREFIX}")


        #################################################################################
        # Create single combined/sorted Tricare Finder file in data directory.
        #################################################################################

        rootLogger.info(f"Concatenate TRICARE Finder files into single file {COMBINED_TRICARE_FNDR_FILE}")

        concatenate_files(DATA_DIR,lstFilenames,sFilenamePrefixCombnd)

        rootLogger.info(f"Sort combined Finder File and remove duplicate entries")
        sortFileNRemoveDups(DATA_DIR,sFilenamePrefixCombnd,SORTED_COMBINED_TRICARE_FNDR_FILE)

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["DATADIR"] = DATA_DIR
        os.environ["SORTED_COMBINED_TRICARE_FNDR_FILE"] = SORTED_COMBINED_TRICARE_FNDR_FILE


        #############################################################
        # Execute Python code to Load Finder File
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_TRICARE_FNDR_FILE.py program")


        try:
            sp_info = subprocess.run(['python3', 'LOAD_TRICARE_FNDR_FILE.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 


        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling LOAD_TRICARE_FNDR_FILE.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"LOAD_TRICARE_FNDR_FILE - Failed ({ENVNAME})"
            MSG=f"LOAD_TRICARE_FNDR_FILE.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script LOAD_TRICARE_FNDR_FILE.py completed successfully.")

        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email.")

        # Send Success email	
        SUBJECT=f"LOAD_TRICARE_FNDR_FILE_Driver.py script - completed ({ENVNAME}{TESTEMAIL})"
        MSG=f"LOAD_TRICARE_FNDR_FILE_Driver.py script completed."
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, TRICARE_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    
        #############################################################
        # script clean-up
        #############################################################
        rootLogger.info("Delete temporary files")
        
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, sFilenamePrefix)
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, COMBINED_TRICARE_FNDR_FILE)


        #############################################################
        # Move Finder File to archive folder -- Required only for Finder shell script file
        #############################################################

        # Move finder file in S3 to archive folder.
        #rootLogger.info(f"Move finder file in S3 to archive folder: s3://{FINDER_FILE_BUCKET_FLDR}archive/{PREFIX}")
        
        #s3ExtSourceKey = FINDER_FILE_BUCKET_FLDR + PREFIX
        #s3ExtDestinationKey = FINDER_FILE_BUCKET_FLDR + "archive/" + PREFIX
        
        #s3MoveFile2NewFolder(s3_client, XTR_BUCKET, s3ExtSourceKey, s3ExtDestinationKey)

  
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script LOAD_TRICARE_FNDR_FILE_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )

    except Exception as e:
        print (f"Exception occured in LOAD_TRICARE_FNDR_FILE_Driver.py\n {e}")

        rootLogger.error("Exception occured in LOAD_TRICARE_FNDR_FILE_Driver.py.")
        rootLogger.error("\n%s", str(e))

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()