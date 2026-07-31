#!/usr/bin/env python
########################################################################################################
# Name: MEDPAC_Driver.py
#
# Desc: MEDPAC HOSPICE Annual Extract
#
# Author     : Joshua Turner	
# Created    : 1/27/2023
#
# Modified:
# Joshua Turner 	2023-01-27 	New script.
# Viren Khanna  	2025-08-22 	Added box Receiptent
# Viren Khanna          2026-07-15      Created Module
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
from CommonFunctions import *

S3BUCKET = rf"{XTR_BUCKET}/{MEDPAC_BUCKET_FLDR}"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}MEDPAC_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nMEDPAC_Driver.py started at {TMSTMP}")

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
      
        rootLogger.info(f" MEDPAC bucket={MEDPAC_BUCKET_FLDR}")

        ###########################################
        # Section 1: MEDPAC_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting MEDPAC_Driver.py logic ---")
        
        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################
        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")

        if iNOFParms == 0:
            lstParms = sys.argv

            now = datetime.now()
            YEAR = str(datetime.now().year)
            CURR_DATE = datetime.now().strftime('%Y%m%d')
            
        rootLogger.info(YEAR)

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["YEAR"] = YEAR


        #############################################################
        # Execute Python code to produce extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of MEDPAC_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'MEDPAC_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script MEDPAC_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"MEDPAC File Extract FAILED ({ENVNAME})"
            MSG=f"MEDPAC File extract has failed in MEDPAC_Extract.py."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script MEDPAC_Extract.py completed successfully.")


        #############################################################
        # Get list of S3 files and record counts for success email.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Get S3 Extract file list and record counts")
        
        # log file contents need to be converted to string
        S3Files = getExtractFilenamesAndCounts(rootLogger, LOGNAME)  



        ###########################################################################################
        # Call combineS3Files.sh to combine all file parts
        ###########################################################################################
        rootLogger.info("")
        rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 
        MEDPAC_BUCKET = f"{XTR_BUCKET}/{MEDPAC_BUCKET_FLDR}"
        rootLogger.info(f"{MEDPAC_BUCKET_FLDR=} ")
        MEDPAC_FILE=f"MEDPAC_Y{YEAR}_FILE_{TMSTMP}.csv.gz"

        sConcatFilename = MEDPAC_FILE
        rootLogger.info(f"{sConcatFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', MEDPAC_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"MEDPAC File S3 file concatenation FAILED({ENVNAME})"
            MSG=f"MEDPAC File extract has failed in the CombineS3Files step of MEDPAC_Driver.py"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info( f"Create Manifest file for : {MEDPAC_FILE}")

        #####################################################
        # bucket/s3folder --> points to location of extract file.
        #                 --> S3 folder is key token to config file to determine if manifest file is in HOLD status
        #          TMSTMP --> uniquely identifies extract file(s)
        #       BoxEmails --> manifest file recipients
        #####################################################
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=MEDPAC_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=MEDPAC_EMAIL_BOX_RECIPIENT )

        except Exception as e:

            SUBJECT=f"Create Manifest file in MEDPAC_Driver.py - Failed  ({ENVNAME})"
            MSG=f"MEDPAC File extract has failed in the CreateManifestFile step of MEDPAC_Driver.py"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            raise

        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")
       
        SUBJECT=f"MEDPAC ANNUAL EXTRACT : {CURR_DATE} ({ENVNAME}{TESTEMAIL})" 
        MSG=f"THE ANNUAL MEDPAC EXTRACTS HAVE BEEN COMPLETED.\n\n======================================================================\n\nFile Name						No of Records\n=========================================	=======================\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, MEDPAC_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Sending Success email in MEDPAC_Extract_Driver.py - Failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script MEDPAC_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in MEDPAC_Driver.py\n {e}")

        rootLogger.error("Exception occured in MEDPAC_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in MEDPAC_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in MEDPAC_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()