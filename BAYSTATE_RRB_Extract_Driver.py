#!/usr/bin/env python
########################################################################################################
# Name: BAYSTATE_RRB_Extract.sh
# Desc: This script executes the RRB extraction python script for MEDPAR Baystate
#
# # Vijay Mandavilli   2026-02-03 Create Module.
# Paul Baranoski       2026-06-08 Added CommonFunctions import and removed hard-coded common functions.
#                                 Added first log message which Dashboard needs.
#
########################################################################################################
# IMPORTS
########################################################################################################

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

S3BUCKET = rf"{XTR_BUCKET}/{MEDPAR_BAYSTATE_BUCKET_FLDR}"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}BAYSTATE_RRB_Extract_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nBAYSTATE_RRB_Extract_Driver.py started at {TMSTMP}")

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
        # Set S3 Bucket--
        ##########################################
      
        rootLogger.info(f"MEDPAR BAYSTATE bucket={MEDPAR_BAYSTATE_BUCKET_FLDR}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        ####################################################
        # Section 1: BAYSTATE_RRB_EXTRACT_Driver.py logic
        #####################################################    

        rootLogger.info("--- Starting BAYSTATE_RRB_Extract_Driver.py logic ---")

        #################################################################################
        # Set exctract filename parameters based on the current month.
        # There two runs in the year:
        #    January - file to be named with FY01{PRIOR_YY}
        #    March   - file to be named with FY03{CURR_YY}
        #################################################################################

        rootLogger.info("")
        rootLogger.info("Determine exctract filename parameters for extract query")

        # Get current month
        MONTH = int(datetime.now().strftime("%m"))
        today = date.today()

        # Compute YEAR and FNAME_SUFFIX based on rules
        if MONTH < 3:
            # Use last year
            YEAR = int((today.year - 1) % 100)
            FNAME_SUFFIX = f"FY01{YEAR:02d}"
        else:
            YEAR = int(today.year % 100)
            FNAME_SUFFIX = f"FY03{YEAR:02d}"

        ###########################################################
        # Display extract filename parameters to use.
        ###########################################################

        rootLogger.info(f"{FNAME_SUFFIX=}")
        rootLogger.info(f"{MONTH=}")
        rootLogger.info(f"{today=}")

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["FNAME_SUFFIX"] = FNAME_SUFFIX

        #############################################################
        # Execute Python code to produce Baystate RRB extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of BAYSTATE_RRB_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'BAYSTATE_RRB_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script BAYSTATE_RRB_Extract.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"BAYSTATE_RRB_Extract_Driver.py  - Failed ({ENVNAME})"
            MSG=f"MEDPBAR Baystate RRB extract has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script BAYSTATE_RRB_Extract.py completed successfully.")

  
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
        SUBJECT=f"BAYSTATE_RRB_Extract_Driver.py  - Completed ({ENVNAME}{TESTEMAIL})"
        MSG=f"MEDPAR Baystate RRB Extract has completed successfully.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
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
        MEDPAR_BAYSTATE_BUCKET = rf"{XTR_BUCKET}/{MEDPAR_BAYSTATE_BUCKET_FLDR}"
        
        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT MEDPAR Baystate RRB Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', MEDPAR_BAYSTATE_BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"MEDPAR BAYSTATE Extract RRB EFT process  - Failed ({ENVNAME})"
            MSG= f"MEDPAR BAYSTATE Extract RRB EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

  
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script BAYSTATE_RRB_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in BAYSTATE_RRB_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in BAYSTATE_RRB_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()