#!/usr/bin/bash
#
######################################################################################
# Name:  DEA_PECOS_Driver.py
#
# Desc: Extract DEA PECOS (Provider Enrollment, Chain, and Ownership System) data. 
#	
# Author     : Viren Khanna	
# Created    : 03/19/2026
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


EFT_FILEMASK = "P#EFT.ON.DEAPECOS.DYYMMDD.THHMMSS"


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
    
def validate_dt(sDate2Validate):


    try:

        datetime_obj = datetime.strptime(sDate2Validate, "%Y-%m-%d")
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"DEA_PECOS_Driver.py - Failed ({ENVNAME})"
        MSG=f"DEA PECOS extract has failed. "
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 
        
        sys.exit(12)
            
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

        LOGNAME = f"{LOG_DIR}{TESTLOG}DEA_PECOS_{TMSTMP}.log" 

    
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        #global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDEA_PECOS_Driver.py started at {TMSTMP}")

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
        rootLogger.info(f"{PECOS_BUCKET_FLDR=}")
        rootLogger.info(f"{EFT_FILEMASK=}")
        

        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of DEA_PECOS.py program")

        try:
            sp_info = subprocess.run(['python3', 'DEA_PECOS.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling DEA_PECOS.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"DEA PECOS Extract - Failed ({ENVNAME})"
            MSG=f"Python script DEA_PECOS.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script DEA_PECOS.py completed successfully.")

       
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
        rootLogger.info("Send success email.")
        rootLogger.info(f"{S3Files=}")

        # Send Success email	
        SUBJECT=f" Monthly PECOS extract- completed ({ENVNAME}{TESTEMAIL})"
        MSG=f" The Extract for the creation of the monthly DEA PECOS file from Snowflake has completed.\n\nEFT version of the below file was created using the following file mask {EFT_FILEMASK}.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PECOS_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
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
        PECOS_BUCKET = rf"{XTR_BUCKET}/{PECOS_BUCKET_FLDR}"
        
        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT PECOS Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', PECOS_BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"PECOS Extract EFT process - Failed ({ENVNAME})"
            MSG= f"PECOS Extract EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script DEA_PECOS_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


  except Exception as e:
        print (f"Exception occured in DEA_PECOS_Driver.py\n {e}")

        rootLogger.error("Exception occured in DEA_PECOS_Driver.py.")
        rootLogger.error("\n%s", str(e))

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()
