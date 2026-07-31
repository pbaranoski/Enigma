#!/usr/bin/sh
############################################################################################################
# Name:   LOAD_STS_HOS_HHA_FNDR_FILE_Driver.py
# DESC:   This python program loads the STS_HOS_HHA finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.STS_HOS_HHA_FF table.
#
# Created: Viren Khanna 
# Modified: 02/07/2025
#
# Viren Khanna   2025-02-07 Create script to load STS_HOS_HHA_FF table
# Paul Baranoski 2026-04-08 Converted from bash to python.
############################################################################################################

########################################################################################################
# Set TESTING status 
########################################################################################################
import os
os.environ["TESTING"] = "N"

# This switch is needed to prevent Request Email addresses from being include in error and success emails and manifest files.
swInTESTMode = os.getenv("TESTING","N") 

# Our common module with variable constants
from SET_XTR_ENV import *


########################################################################################################
# IMPORTS
########################################################################################################
import boto3 
import logging
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *


DATADIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

PREFIX = "POS_File_iQIES"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}LOAD_STS_HOS_HHA_FNDR_FILE_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nLOAD_STS_HOS_HHA_FNDR_FILE_Driver.py started at {TMSTMP}")

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
        rootLogger.info(f"Get s3 Client/resource objects")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")

        #############################################################
        # Display some variable values
        #############################################################
        rootLogger.info(f"{FINDER_FILE_BUCKET_FLDR=}")
        rootLogger.info(f"{PREFIX=}")
            
        #################################################################################
        # Find STS HOS/HHA Finder Files in S3
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Find S3 STS HOS HHA Finder Files in S3.")

        # Get all filenames in S3 bucket that match filename prefix
        lstKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_BUCKET_FLDR, PREFIX)
        
        # Get count of NOF Finder Files
        NOF_FILES = len(lstKeys)

        rootLogger.info("")
        rootLogger.info(f"{NOF_FILES} STS HOS HHA Finder files found in S3.")

        if NOF_FILES == 0:
            rootLogger.info("")
            rootLogger.info(f"No Finder files found for {FINDER_FILE_BUCKET_FLDR}{PREFIX}.")
            
            # Send Failure email	
            SUBJECT = f"LOAD_STS_HOS_HHA_FNDR_FILE.sh script - Failed ({ENVNAME}{TESTEMAIL}) "
            MSG = f"No Finder Files found for {FINDER_FILE_BUCKET_FLDR}{PREFIX}."
            
            try:
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)

                # end gracefully
                sys.exit(4)  
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error("\n%s", e.output)

                sys.exit(12)  
           
        # if more than one finder file found --> error --> which file to process?	
        elif NOF_FILES > 1:
            rootLogger.info("")
            rootLogger.info(f"More than one Finder files found for {FINDER_FILE_BUCKET_FLDR}{PREFIX}.")
            
            # Send Failure email	
            SUBJECT = f"LOAD_STS_HOS_HHA_FNDR_FILE.sh script - Failed ({ENVNAME}{TESTEMAIL}) "
            MSG = f"More than one Finder files found for {FINDER_FILE_BUCKET_FLDR}{PREFIX}."

            try:
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)

                # Hard error - this is an issue that needs to be resolved.
                sys.exit(12) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error("\n%s", e.output)

                sys.exit(12)   

        #################################################################################
        # Extract Finder filename from S3 Key
        #################################################################################
        lstFilenames = getFilenamesFromS3Keys(lstKeys, FINDER_FILE_BUCKET_FLDR)
        
        filename = lstFilenames[0]
        STS_HOS_HHA_FINDER_FILE = filename
        
        rootLogger.info("")
        rootLogger.info(f"STS HOS HHA Finder file found: {filename}") 


        #############################################################
        # Execute Python code to load Finder File to STS HOA HHA FF table.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_STS_HOS_HHA_FNDR_FILE.py program")

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["DATADIR"] = DATADIR
        os.environ["STS_HOS_HHA_FINDER_FILE"] = STS_HOS_HHA_FINDER_FILE

        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        try:
            sp_info = subprocess.run(['python3', 'LOAD_STS_HOS_HHA_FNDR_FILE.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)   

        rootLogger.info("")
        rootLogger.info("Python script LOAD_STS_HOS_HHA_FNDR_FILE.py completed successfully. ")


        #################################################################################
        # MOVE S3 STS HOA HHA Finder File to archive folder when loaded into table.
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Move STS HOA HHA Finder file to archive folder after successful load into table")

        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{filename}", f"{FINDER_FILE_BUCKET_FLDR}archive/{filename}")


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script LOAD_STS_HOS_HHA_FNDR_FILE_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in LOAD_STS_HOS_HHA_FNDR_FILE_Driver.py\n {e}")

        rootLogger.error("Exception occured in LOAD_STS_HOS_HHA_FNDR_FILE_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT=f"LOAD_STS_HOS_HHA_FNDR_FILE_Driver.py  - Failed ({ENVNAME}{TESTEMAIL})"
        MSG=f"Exception occured in LOAD_STS_HOS_HHA_FNDR_FILE_Driver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)  
        

if __name__ == "__main__":
    
    main_processing_loop()
