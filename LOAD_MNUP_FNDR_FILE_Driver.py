#!/usr/bin/sh
############################################################################################################
# Script Name: LOAD_MNUP_FNDR_FILE_Driver.py
# Description: This script uploads the MNUP Monthly finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MNNUP_FF table.
#
# Author     : Paul Baranoski	
# Created    : 11/23/2022
#
# Paul Baranoski 2023-08-03 Update to download Finder File from S3:/Finder_Files bucket.
# Paul Baranoski 2024-01-10 Add $ENVNAME to SUBJECT line of Emails. 
# Paul Baranoski 2024-03-22 Replace MNUP_EMAIL_SENDER with CMS_EMAIL_SENDER.
#                           Rework logic for getting Count of Finder Files, and getting Finder File filename in S3.  
# Paul Baranoski 2024-07-11 Change return code when no finder files found to 12. If 0, the rest of the script
#                           executes which it should not do. And, we are expecting a finder file to be present.
# Paul Baranoski 2026-06-25 Convert bash script to python.
############################################################################################################


import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *

import boto3 
import logging
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta

import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

S3BUCKET = rf"{XTR_BUCKET}/{MNUP_BUCKET_FLDR}"
PREFIX = "MNUP_FNDR"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}LOAD_MNUP_FNDR_FILE_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nLOAD_MNUP_FNDR_FILE_Driver.py started at {TMSTMP}")

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(rootLogger) 

        
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
       
        ##########################################
        # Were the correct NOF parameters sent?
        ##########################################
        iNOFParms = len(sys.argv) - 1
        if not (iNOFParms == 0 ):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")
            

        ##########################################
        # Display Bucket values
        ##########################################
        rootLogger.info(f"{S3BUCKET=}")
        rootLogger.info(f"{FINDER_FILE_SSA_BUCKET_FLDR=}")
        

        #################################################################################
        # Find MNNUP Finder Files in S3
        #################################################################################
        lstFFKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_SSA_BUCKET_FLDR, PREFIX)
        NOF_FILES = len(lstFFKeys)


        #################################################################################
        # if zero FF found --> end script; Multiple FF --> end script.
        #################################################################################
        rootLogger.info("")
        rootLogger.info(f"{NOF_FILES} MNUP Finder files found in S3.")

        if NOF_FILES == 0:
            rootLogger.info("")
            rootLogger.info(f"No MNUP Finder files found in {FINDER_FILE_SSA_BUCKET_FLDR}{PREFIX}.") 
            
            # Send Failure email	
            SUBJECT = f"LOAD_MNUP_FNDR_FILE_Driver.py script - Failed ({ENVNAME})"
            MSG = f"No MNUP Finder Files found in {FINDER_FILE_SSA_BUCKET_FLDR}{PREFIX}."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)

        # if more than one finder file found --> error --> which file to process?
        elif NOF_FILES > 1:    
            rootLogger.info("")
            rootLogger.info(f"More than one MNUP Finder file found in {FINDER_FILE_SSA_BUCKET_FLDR}{PREFIX}.")
            
            # Send Failure email	
            SUBJECT = f"LOAD_MNUP_FNDR_FILE_Driver.py script - Failed ({ENVNAME})"
            MSG = f"More than one MNUP Finder Files found in {FINDER_FILE_SSA_BUCKET_FLDR}{PREFIX}."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)
 	

        #################################################################################
        # Extract just the filename from the S3 filename information
        #################################################################################
        
        lstFinderFilenames = getFilenamesFromS3Keys(lstFFKeys, FINDER_FILE_SSA_BUCKET_FLDR)
        
        LOAD_MNUP_FINDER_FILE = lstFinderFilenames[0]

        rootLogger.info("")
        rootLogger.info(f"MNUP Finder file found: {LOAD_MNUP_FINDER_FILE}")

        # Export environment variables for Python code
        os.environ["LOAD_MNUP_FINDER_FILE"] = LOAD_MNUP_FINDER_FILE


        #############################################################
        # Execute Python code to load Finder File to MNUP FF table.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_MNUP_FNDR_FILE.py program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_MNUP_FNDR_FILE.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling LOAD_MNUP_FNDR_FILE_Driver.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"LOAD_MNUP_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
            MSG=f"Python script LOAD_MNUP_FNDR_FILE_Driver.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script LOAD_MNUP_FNDR_FILE_Driver.py completed successfully.")
        

        #################################################################################
        # Move finder file in S3 to archive folder. 
        #################################################################################
        rootLogger.info("")
        rootLogger.info(f"Move processed finder file {LOAD_MNUP_FINDER_FILE} to S3 Finder File SSA archive folder.")

        archiveFinderFile(s3_client, XTR_BUCKET, FINDER_FILE_SSA_BUCKET_FLDR, LOAD_MNUP_FINDER_FILE)
            

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Python program LOAD_MNUP_FNDR_FILE_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in LOAD_MNUP_FNDR_FILE_Driver.py\n {e}")

        rootLogger.error("Exception occured in LOAD_MNUP_FNDR_FILE_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in LOAD_MNUP_FNDR_FILE_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in LOAD_MNUP_FNDR_FILE_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop() 
        
