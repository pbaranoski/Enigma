#!/usr/bin/env python
########################################################################################################
# Name: VARTN_Driver.py
#
# Desc: VA Return File Annual Extract
#
# Author     : Joshua Turner	
# Created    : 1/17/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  ----------------------------------------------------------------------
# Joshua Turner 	    2023-01-17   New script.
# Joshua Turner         2023-11-08   Updated for Box delivery - added call to create manifest file 
# Paul Baranoski        2023-11-28   Add parameter (S3 manifest folder override) in call to CreateManifestFile.sh 
#                                    Add ENVNAME to email Subject line.
# Sean Whitelock        2024-09-24   Updated the parameter for S3 manifest folder override call.
# Paul Baranoski        2024-11-05   Modified ending line to be "Ended at.." because Dashboard script is looking for that to know if extract ended successfully.  
# Paul Baranoski        2024-12-23   Add this line to re-migrate code due to "SSM agent on Jenkins server" was down.
# Paul Baranoski        2026-06-10   Modified 'echo "" >> echo "Creating Manifest file for: ${VARTN_FILE}" >> ${LOGNAME}'
#                                    to 'echo "Creating Manifest file for: ${VARTN_FILE}" >> ${LOGNAME}'.
#                                    This was causing a file to be created in scripts/run with that string as the filename.
# Viren Khanna          2026-06-10   Created Module
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

S3BUCKET = rf"{XTR_BUCKET}/{VARTN_BUCKET_FLDR}"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}VARTN_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nVARTN_Driver.py started at {TMSTMP}")

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
      
        rootLogger.info(f" VARTN bucket={VARTN_BUCKET_FLDR}")

        ###########################################
        # Section 1: VARTN_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting VARTN_Driver.py logic ---")
        
        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################
        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")

        if iNOFParms == 0:
            lstParms = sys.argv

            now = datetime.now()
            YEAR = str(datetime.now().year - 1)
            
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
        rootLogger.info("Start execution of VARTN_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'VARTN_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script VARTN_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"VA Return File Extract FAILED ({ENVNAME})"
            MSG=f"TVA Return File extract has failed in VARTN_Extract.py."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script VARTN_Extract.py completed successfully.")


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
        VARTN_BUCKET = f"{XTR_BUCKET}/{VARTN_BUCKET_FLDR}"
        rootLogger.info(f"{VARTN_BUCKET_FLDR=} ")
        VARTN_FILE=f"VARETURN_Y{YEAR}_FILE_{TMSTMP}.txt.gz"

        sConcatFilename = VARTN_FILE
        rootLogger.info(f"{sConcatFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', VARTN_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"VA Return File S3 file concatenation FAILED({ENVNAME})"
            MSG=f"VA Return File extract has failed in the CombineS3Files step of VARTN_Driver.py"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info( f"Create Manifest file for : {VARTN_FILE}")

        #####################################################
        # bucket/s3folder --> points to location of extract file.
        #                 --> S3 folder is key token to config file to determine if manifest file is in HOLD status
        #          TMSTMP --> uniquely identifies extract file(s)
        #       BoxEmails --> manifest file recipients
        #####################################################
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=VARTN_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=VARTN_EMAIL_BOX_RECIPIENT )

        except Exception as e:

            SUBJECT=f"Create Manifest file in VARTN_Driver.py - Failed  ({ENVNAME})"
            MSG=f"VA Return File extract has failed in the CreateManifestFile step of VARTN_Driver.py"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            raise

        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")
       
        SUBJECT=f"VA RETURN ANNUAL EXTRACT : {YEAR} ({ENVNAME}{TESTEMAIL})" 
        MSG=f"THE ANNUAL VA RETURN EXTRACTS HAVE BEEN COMPLETED.\n\n======================================================================\n\nFile Name						No of Records\n=========================================	=======================\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, VARTN_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Sending Success email in VARTN_Extract_Driver.py - Failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script VARTN_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in VARTN_Driver.py\n {e}")

        rootLogger.error("Exception occured in VARTN_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in VARTN_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in VARTN_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()