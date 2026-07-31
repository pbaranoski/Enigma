#!/usr/bin/env python
########################################################################################################
# Name:  ASC_PTB_Extract_Driver.py
#
# Desc: ASC (Ambulatory Surgical Center PTB extract. Designed to run in Annually in April 
#
# Execute as python3 ASC_PTB_Extract.py   
#   
# Paul Baranoski   2025-07-25 Create Module.
# Paul Baranoski   2025-08-27 Modify logic to give rootLogger obj a logger name to ensure separate logger when calling functions in imported code.
# Paul Baranoski   2025-09-26 Modify subprocess.run to subprocess.run which allows to capture stderr as well as stdout. 
#                             Add write_sp_info_2_log function and companion logging import module LoggerStandard. 
# Paul Baranoski   2025-10-20 Subprocess.run was missing "capture_output=True, text=True, check=True" function parameters when executing ProcessEFT.sh.
# Paul Baranoski   2026-04-30 Changed NIGMA_EMAIL_FAILURE_RECIPIENT to ASC_PTB_EMAIL_SUCCESS_RECIPIENT.
# Viren Khanna    2026-06-02  Updated Module.
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
import pandas as pd

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

ASC_PTB_BUCKET = rf"{XTR_BUCKET}/{ASC_PTB_BUCKET_FLDR}"


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Constants
#############################################################
# Parm Dates to be in YYYYMMDD format

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}ASC_PTB_Extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        # rootLogger.info(f"\nPSPS_NPI_Extract_Driver.py started at {TMSTMP}")

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


        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")
       

        #################################################################################
        # Create Date parameters for Extract
        #     CLM_EFCT_DT Begin and End date parameters. Ex. 20210101 and 20220331
        #     CLM_LINE_FROM_DT (Prior Year YYYY)
        #################################################################################
        rootLogger.info("")

        CURR_YYYY = (datetime.today()).strftime('%Y')
        PRIOR_YYYY = (datetime.today() + timedelta(days=-365)).strftime('%Y')
    
        CLM_EFCT_DT_BEG = f"{PRIOR_YYYY}0101"
        CLM_EFCT_DT_END = f"{CURR_YYYY}0331"
        CLM_LINE_FROM_DT_YYYY = f"{PRIOR_YYYY}"

        rootLogger.info(f"{CLM_EFCT_DT_BEG=}")
        rootLogger.info(f"{CLM_EFCT_DT_END=}")
        rootLogger.info(f"{CLM_LINE_FROM_DT_YYYY=}")

 
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["CLM_EFCT_DT_BEG"] = CLM_EFCT_DT_BEG
        os.environ["CLM_EFCT_DT_END"] = CLM_EFCT_DT_END
        os.environ["CLM_LINE_FROM_DT_YYYY"] = CLM_LINE_FROM_DT_YYYY
        os.environ["CURR_YYYY"] = CURR_YYYY
        os.environ["PRIOR_YYYY"] = PRIOR_YYYY
                
                
        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of ASC_PTB_Extract.py program")
        
        
        try:
            sp_info = subprocess.run(['python3', 'ASC_PTB_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ASC_PTB_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"ASC_PTB_Extract_Driver.py - Failed ({ENVNAME})"
            MSG=f"ASC_PTB_Extract_Driver.py has failed. "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script ASC_PTB_Extract_Driver.py completed successfully.")

  
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
       
        SUBJECT=f"ASC PTB extract ({ENVNAME}{TESTEMAIL})" 
        MSG=f"The Extract for the creation of the ASC PTB file from Snowflake has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ASC_PTB_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Sending Success email in ASC_PTB_Extract_Driver.py - Failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    


        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT ASC PTB Extract Files ")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', ASC_PTB_BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"ASC PTB extract EFT process  - Failed ({ENVNAME})"
            MSG= f"ASC PTB Extract EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script ASC_PTB_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in ASC_PTB_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in ASC_PTB_Extract_Driver.py.")
        rootLogger.error(e)

        # Send Failure email	
        SUBJECT = f"Exception occured in ASC_PTB_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in ASC_PTB_Extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()