#!/usr/bin/bash
#
######################################################################################
# Name:  HCPP_Extract_Driver
#
# Desc: Performs Extract of HCPP data. 
#
# Input: Finder file that includes a Contract Number, Extract Year, and Contractor
#        Ex. Entry in Finder file: "H3503 2018 Bland" 	
#
# Author     : Viren Khanna	
# Created    : 03/08/2026
#
######################################################################################

########################################################################################################
# Set TESTING status 
########################################################################################################
import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *

########################################################################################################
# IMPORTS
########################################################################################################
import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import logging
import sys
import argparse
import re
import io

import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data"

from datetime import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog

########################################################################################################
# CONSTANTS
########################################################################################################
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"
DATA_DIR = "/app/IDRC/XTR/CMS/data/"


s3BktFldr =  FINDER_FILE_BUCKET_FLDR
sFilenamePrefix = "HCPP_Finder_File"



#############################################################
# Functions
#############################################################
def write_sp_info_2_log(sp_info):
        
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    rootLogger.info("\n%s", sp_info.stdout) 
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    rootLogger.info("\n%s", sp_info.stderr) 
    rootLogger.info(f"{sp_info.returncode=}")  


def getRC(sp_info):

    return sp_info.returncode

    
def s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    # Copy object, then delete to "move" file.
    rootLogger.info(f"Moving {sSourceKey} to {sDestinationKey} in {sSourceBucket}.")
    
    s3_client.copy_object(
        Bucket=sSourceBucket,
        CopySource={"Bucket": sSourceBucket, "Key": sSourceKey},
        Key=sDestinationKey
    )

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)


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
        # Reset NOF Upload Bytes transferred    
        global giTotalDownloadBytesTransferred
        giTotalDownloadBytesTransferred = 0
    
        s3_client.download_file(s3BUCKET, s3ExtractFileKey, txtFFPathNFilename, Callback=DownloadFileProgress)
        rootLogger.info(f"The download of file {s3ExtractFileKey} to {txtFFPathNFilename} has completed.")


def archiveFinderFile(s3_client, sSourceBucket, FF):

    sSourceKey = f"{FINDER_FILE_BUCKET_FLDR}{FF}"
    sDestinationKey  = f"{FINDER_FILE_BUCKET_FLDR}archive/{FF}"

    #############################################################
    # Move Finder File in S3 to archive folder
    #############################################################
    rootLogger.info(f"Moving S3 Finder file {FF} to S3 archive folder.")

    s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey)

    #############################################################
    # Delete Finder File in Linux
    #############################################################
    rootLogger.info("")
    rootLogger.info(f"Delete request/finder file {DATA_DIR}{FF} from linux data directory.")

    os.remove(f"{DATA_DIR}{FF}")


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

    lstFilenames = []
    
    for sKey in lstKeys:
        rootLogger.info(f"{sKey=}")
        sFilename = sKey.replace(s3BktFldr,"")
        lstFilenames.append(sFilename) 

    return lstFilenames


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


def getRangeOfRecords(sFilenameNPath, iLineFrom, iLineTo):
        
    from itertools import islice
    sioRangeOfRecs = StringIO()

    with open(sFilenameNPath, "r", encoding="utf-8") as f:
        # islice indexing is zero-based; and end-line is non-inclusive
        for line in islice(f, iLineFrom - 1, iLineTo):
            sioRangeOfRecs.write(line)

    return sioRangeOfRecs.getvalue()
    

def wc_l(filename):

    with open(filename, "r", encoding="utf-8", errors="ignore") as f:
       return sum(1 for line in f)
       

def getExtFiles4RequestList(s3_resource, sSourceBucket, S3KeyPrefix, sTimeStamp):  
   
    #S3ExtFndrFldrNPrefix = FINDER_FILE_BUCKET_FLDR + PREFIX
    rootLogger.info("")
    rootLogger.info(f"{sSourceBucket=}")
    rootLogger.info(f"{S3KeyPrefix=}")
    rootLogger.info(f"{sTimeStamp=}")

    #############################################################
    # Get list of S3 files to include in manifest.
    #############################################################
    rootLogger.info("Get list of Extract Files for Request. ")

    lstExtFiles4Request = [ obj.key for obj in s3_resource.Bucket(sSourceBucket).objects.filter(Prefix=S3KeyPrefix) if obj.find(sTimeStamp) != -1 ]

    return lstExtFiles4Request


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}HCPP_Extract_{TMSTMP}.log" 

    
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        #global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nHCPP_Extract_Driver.py started at {TMSTMP}")

        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")

        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP


        ##########################################
        # Display S3 Buckets
        ##########################################
        rootLogger.info("")

        rootLogger.info(f"{XTR_BUCKET=}")
        rootLogger.info(f"{HCPP_BUCKET_FLDR=}")
        rootLogger.info(f"{FINDER_FILE_BUCKET_FLDR=}")
        rootLogger.info(f"{s3BktFldr=}")  
        rootLogger.info(f"{sFilenamePrefix=}") 
        

        #################################################################################
        # Find HCPP Finder Files in S3
        #################################################################################
        rootLogger.info("Find HCPP Finder Files in S3.")

        lstFileKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, s3BktFldr, sFilenamePrefix)
        rootLogger.info(f"{lstFileKeys=}")

        # if zero files found --> end script
        iNOFFiles = len(lstFileKeys)
        rootLogger.info(f"{iNOFFiles=}")

        if iNOFFiles == 0:
            #Send Failure email	- commented below code, as the email will be sent from HCPP_Extracts_Driver.py
            SUBJECT=f"HCPP_Extract_Driver.py script - No Finder files found ({ENVNAME}{TESTEMAIL})"
            MSG=f"No HCPP finder files found in S3 folder {FINDER_FILE_BUCKET_FLDR}{sFilenamePrefix}."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(4)



        #################################################################################
        # # Copy HCPP Finder File to linux.
        #################################################################################
        #lstRequestFileKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_BUCKET_FLDR, sFilenamePrefix)
        # Convert list of s3 File Keys to list of S3 Filenames
        lstFilenames = getFilenamesFromS3Keys(lstFileKeys, FINDER_FILE_BUCKET_FLDR)
       
        for S3FinderFilename in lstFilenames:
            rootLogger.info("")
            rootLogger.info(f"DOwnloading S3 Finder File From {FINDER_FILE_BUCKET_FLDR}{S3FinderFilename} to linux ")

            downloadFileFromS3(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{S3FinderFilename}", f"{DATA_DIR}{S3FinderFilename}")


            ##############################################
            # Process records in Finder File
            ##############################################
            
            ##############################################
            # Process each record in Finder File
            ##############################################
            with open(f"{DATA_DIR}{S3FinderFilename}", "r", encoding="UTF-8") as f:
                
                for sExtRecord in f:

                    rootLogger.info("")
                    rootLogger.info("----------------------------")
                        
                    # Remove leading and trailing spaces and end-of-record markers
                    sExtRecord = sExtRecord.strip()
                    rootLogger.info(f"{sExtRecord=}")
                    
                    # skip blank lines - zero length
                    if sExtRecord == "":
                        continue

                    # skip "blank lines" containing only spaces and commas 
                    if sExtRecord.replace(" ","").replace(",","") == "":
                        rootLogger.info("Skip blank record")
                        continue

                    ##############################################
                    # NOF fields not correct for record?
                    # --> Reject file
                    ##############################################
                    lstFields = sExtRecord.split(",")
                    iNOF_FIELDS = len(lstFields)
                        
                    if iNOF_FIELDS != 3:
                        rootLogger.info("")
                        rootLogger.info(f"Request file {S3FinderFilename} has incorrectly formatted records. Incorrect number of fields {iNOF_FIELDS} found instead of 3. ")
                            
                        # Send Failure email	
                        SUBJECT=f"HCPP Extract - Failed ({ENVNAME}{TESTEMAIL})"
                        MSG=f"Request file {S3FinderFilename} has incorrectly formatted records. Incorrect NOF fields {iNOF_FIELDS} instead of 3. Request file has been rejected. Please correct and re-submit file."
        
                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                        rootLogger.info(sp_info) 

                        # Set BAD_FILE_SW to True
                        BAD_FILE_SW = True
                            
                        # Exit for loop that is processing records in file
                        break   

                    ##############################################
                    # Extract HCPP record fields; remove leading/trailing spaces
                    ##############################################
                    rootLogger.info("Extract record fields")
                        
                    CONTRACT_NUM = lstFields[0].strip()
                    rootLogger.info(f"{CONTRACT_NUM=}")

                    EXT_YR = lstFields[1].strip()
                    rootLogger.info(f"{EXT_YR=}")

                    CONTRACTOR = lstFields[2].strip()
                    rootLogger.info(f"{CONTRACTOR=}")


                    ##############################################
                    # Export fields for python extract code.
                    ##############################################	
                    os.environ["CONTRACT_NUM"] = CONTRACT_NUM                
                    os.environ["EXT_YR"] = EXT_YR
                    os.environ["CONTRACTOR"] = CONTRACTOR
                    os.environ["TMSTMP"] = TMSTMP

                    ##############################################
                    # Extract HCPP records for Extract record.
                    ##############################################
                    rootLogger.info(f"Start execution of HCPP_Extract.py program ")
                    rootLogger.info(f"Extract HCPP data for {CONTRACT_NUM=} for {EXT_YR=} and {CONTRACTOR=} ")

                    sp_info = subprocess.run(['python3', 'HCPP_Extract.py'], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info)  

                    ##############################################
                    # End-For loop: Processing request file
                    ##############################################
                    
                #############################################################
                # Move Finder File to archive folder -- Required only for Finder python script file
                #############################################################

                rootLogger.info(f"Moving finder file {S3FinderFilename} to archive folder ")

                
                s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{S3FinderFilename}", f"{FINDER_FILE_BUCKET_FLDR}archive/{S3FinderFilename}")
                rootLogger.info(f"{S3FinderFilename=} to S3 archive folder" )


                #############################################################
                # Remove any residual HCPP Finder files in data directory.  
                #############################################################
                rootLogger.info("Remove any residual Finder Files in data directory.")
            
                deleteFilesFromLinuxUsingPrefix(DATA_DIR, S3FinderFilename)


        #############################################################
        # Get list of S3 files and record counts for success email.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Get S3 Extract file list and record counts")
        
        # log file contents need to be converted to string
        S3Files = getExtractFilenamesAndCounts(rootLogger, LOGNAME)  


        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")

        # Send Success email	
        SUBJECT=f"HCPP_Extract_Driver.py  - Completed ({ENVNAME}{TESTEMAIL})"
        MSG=f"HCPP Extract has completed successfully.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, HCPP_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    

        ####################################################################
        # Start - EFT extract file process
        ####################################################################  

        # S3 Bucket + s3 folder path
        # References to Blue Button should be changed to extract you are working on
        HCPP_BUCKET = rf"{XTR_BUCKET}/{HCPP_BUCKET_FLDR}"
         
        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT HCPP Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', HCPP_BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"HCPP Extract EFT process  - Failed ({ENVNAME})"
            MSG= f"HCPP Extract EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

  
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script HCPP_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )

  except Exception as e:
        print (f"Exception occured in HCPP_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in HCPP_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in HCPP_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in HCPP_Extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()