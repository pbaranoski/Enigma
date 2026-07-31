#!/usr/bin/env python
########################################################################################################
# Name: TRICARE_Extract_Driver.py
# DESC:   This script executes the full python script for TRICARE extract load
#
# Vijay Mandavilli   2026-03-02 Create Module.
# Paul Baranoski     2026-06-09 Remove hard-coded common functions and add import of CommonFunctions.
#                               Uncomment initial "TRICARE_Extract_Driver.py started at ".
#                               Modify rootLogger(sp_info) to  write_sp_info_2_log(sp_info) to have 
#                               new-lines be recognized.
########################################################################################################
# IMPORTS
########################################################################################################

import os
os.environ["TESTING"] = "N"

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
from CommonFunctions import *

sFilenamePrefix = "TRICARE_EXTRACT"


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Functions
#############################################################


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}TRICARE_extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nTRICARE_Extract_Driver.py started at {TMSTMP}")

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(rootLogger) 
        
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
        if not (iNOFParms == 0):
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
        # Set S3 Bucket
        ##########################################
      
        rootLogger.info(f"TRICARE bucket={TRICARE_BUCKET_FLDR}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        ###########################################
        # Section 1: TRICARE_EXTRACT_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting TRICARE_Extract_Driver.py logic ---")


        #################################################################################
        # Remove any residual TRICARE Extract files in data directory.
        #################################################################################

        rootLogger.info("Remove any residual Finder Files in data directory.")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, sFilenamePrefix)


        #############################################################
        # Execute Script to load TRICARE Finder File table in SF
        #############################################################
        
        rootLogger.info("Execute script LOAD_TRICARE_FNDR_FILE_Driver.py")

        # Execute Python code to load TRICARE finder file into SF
        rootLogger.info("")
        rootLogger.info("Start execution of Load_TRICARE_FNDR_FILE_Driver.py program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_TRICARE_FNDR_FILE_Driver.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

        except subprocess.CalledProcessError as e:
            if e.returncode == 4:
                rootLogger.info("LOAD_TRICARE_FNDR_FILE.py ended. No Finder Files found.")
                rootLogger.info(f"\nEnded at {TMSTMP}" )
                ## Send Failure email	
                SUBJECT=f"TRICARE_Extract_Driver.py ended. No Finder Files found. ({ENVNAME}{TESTEMAIL})"
                MSG=f"TRICARE_Extract_Driver.py  ended. No Finder Files found."
 
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                sys.exit(4)

            rootLogger.error(f"Calling Python script LOAD_TRICARE_FNDR_FILE_Driver.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"TRICARE_Extract_Driver.py   - Failed ({ENVNAME}{TESTEMAIL})"
            MSG=f"TRICARE extract Driver has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script LOAD_TRICARE_FNDR_FILE_Driver.py completed successfully.")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        #############################################################
        # Execute Python code to produce TRICARE extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of TRICARE_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'TRICARE_extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script TRICARE_Extract.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"TRICARE_Extract_Driver.py  - Failed ({ENVNAME}{TESTEMAIL})"
            MSG=f"TRICARE extract Driver has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script TRICARE_Extract.py completed successfully.")

  
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
        SUBJECT=f"Weekly TRICARE extract ({ENVNAME}{TESTEMAIL})"
        MSG=f"The Extract for the creation of the weekly TRICARE file from Snowflake has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, TRICARE_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    


        #############################################################
        # Download S3 extract file to linux (for EFT processing)
        #############################################################

        rootLogger.info(f"Copy S3 TRICARE Extract File to linux data directory")
        # split returns a list; and the first item in the list is the filename
        gz_filename = S3Files.split() [0]
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{TRICARE_BUCKET_FLDR}{gz_filename}", f"{DATA_DIR}{gz_filename}" ) 


        #############################################################
        # unzip gz file on linux; get new filename
        #############################################################

        rootLogger.info("Unzip ${gz_filename} file on linux" )

        txt_Filename = unzipFile(DATA_DIR,gz_filename)


        #############################################################
        # Split extract file into 4 files for EFT process
        #############################################################

        rootLogger.info(f"Split file {txt_Filename} into 4 files ")
        lstSplitFilesNPath = splitTextFileIntoMultipleFiles(sInputFileNPath = f"{DATA_DIR}{txt_Filename}", iNOFFiles = 4,sOutputFileNPath=f"{DATA_DIR}{txt_Filename}")


        #############################################################
        # zip file on linux; get new filename; upload split files from Linux to S3
        #############################################################

        rootLogger.info(f"{lstSplitFilesNPath=} split files on linux" )
        rootLogger.info(f"zip and upload split files." )
        
        for sFileToZipNPath in lstSplitFilesNPath:
            gz_SplitFilename = gzipFile(DATA_DIR, os.path.basename(sFileToZipNPath))
            rootLogger.info(f"{sFileToZipNPath=} on linux" )
            
            s3UploadFile(s3_client, f"{DATA_DIR}{gz_SplitFilename}", XTR_BUCKET, f"{TRICARE_BUCKET_FLDR}{gz_SplitFilename}" )     
        

        #for sFileToZip in lstSplitFilesNPath:
            #gzipFile(DATA_DIR, os.path.basename(sFileToZip))
            #rootLogger.info(f"{sFileToZip=} on linux" )


        #############################################################
        # Move Finder File to archive folder -- Required only for Finder python script file
        #############################################################

        sFFilenamePrefix = "TRICARE_FNDR"
        lstFileKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_BUCKET_FLDR, sFFilenamePrefix)
        rootLogger.info(f"{lstFileKeys=}")

        for sFileToArchive in lstFileKeys:
            s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{os.path.basename(sFileToArchive)}", f"{FINDER_FILE_BUCKET_FLDR}archive/{os.path.basename(sFileToArchive)}")
            rootLogger.info(f"{sFileToArchive=} to S3 archive folder" )


        #############################################################
        # Move TRICARE Extract file to archive folder.
        #############################################################

        # Move finder file in S3 to archive folder.
        rootLogger.info(f"Moving S3 TRICARE Extract file to S3 archive folder:  {TRICARE_BUCKET_FLDR}archive/{gz_filename}")
        
        s3ExtSourceKey = f"{TRICARE_BUCKET_FLDR}{gz_filename}"
        s3ExtDestinationKey = f"{TRICARE_BUCKET_FLDR}archive/{gz_filename}"
        
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, s3ExtSourceKey, s3ExtDestinationKey)
        # Remove file from linux

        # Delete Extract and split files    
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, sFilenamePrefix)

        ####################################################################
        # Start - EFT extract file process
        ####################################################################  

        # S3 Bucket + s3 folder path
        # References to Blue Button should be changed to extract you are working on
        TRICARE_BUCKET = rf"{XTR_BUCKET}/{TRICARE_BUCKET_FLDR}"
        
        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT TRICARE Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', TRICARE_BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"TRICARE Extract SSN EFT process  - Failed ({ENVNAME}{TESTEMAIL})"
            MSG= f"TRICARE Extract SSN EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

  
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script TRICARE_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in TRICARE_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in TRICARE_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in TRICARE_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in TRICARE_Extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()