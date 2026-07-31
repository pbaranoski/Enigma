#!/usr/bin/env python
########################################################################################################
# Name: PSPS_Extract_Driver.py
# DESC:   This script executes the full python script for TRICARE extract load
#
# Viren Khanna   2026-04-20 Create Module.
# Paul Baranoski 2026-06-09 Uncomment initial "PSPS_Extract_Driver.py started at ".
#                           Modify rootLogger(sp_info) to  write_sp_info_2_log(sp_info) to have 
#                           new-lines be recognized.
#
########################################################################################################
# IMPORTS
########################################################################################################

import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *

import boto3

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

S3BUCKET = rf"{XTR_BUCKET}/{PSPS_BUCKET_FLDR}"
s3BktFldr =  rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}PSPS_Extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPSPS_Extract_Driver.py started at {TMSTMP}")

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
      
        rootLogger.info(f"PSPS_BUCKET={PSA_BUCKET_FLDR}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        ###########################################
        # Section 1: PSPS_Extract_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting PSPS_Extract_Driver.py logic ---")
        
        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################
        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")

        if iNOFParms == 0:
            lstParms = sys.argv

            now = datetime.now()
            CUR_YR = now.year
            MONTH = now.month
            PRIOR_YR = str(int(CUR_YR) - 1)
            
        rootLogger.info(PRIOR_YR)
        rootLogger.info(f"{CUR_YR=}") 
        rootLogger.info(f"{MONTH=}") 
       
        MM = datetime.now().strftime("%m")

        if MM == "04" or MM == "05" or MM == "06":
         QTR = "Q1"
        elif MM == "07" or MM == "08" or MM == "09":
         QTR = "Q2"
        elif MM == "10" or MM == "11" or MM == "12":
         QTR = "Q3"
        elif MM == "01" or MM == "02" or MM == "03":
         QTR = "Q4"

        else:
            msg_lines = [
        "Extract is processed quarterly for months April, July, October, and January.\n"
        "Extract is not scheduled to run for this time period.\n"
        "Processing completed."
            ]

            rootLogger.info(msg_lines) 

        ## Send Failure email	
            SUBJECT=f"PSPS Extract did not run.  - Failed ({ENVNAME})"
            MSG=f"Extract is processed quarterly for months April, July, October, and January. Extract is not scheduled to run for this time period. "

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(4)    

        ############################################
        # Build parms for appropriate Qtr
        ############################################

        if QTR == "Q1":
            SERV_CYQ_BEG_DT = f"CY{CUR_YR}Q1"
            SERV_CYQ_END_DT = f"CY{CUR_YR}Q1"
        elif QTR == "Q2":
             SERV_CYQ_BEG_DT = f"CY{CUR_YR}Q1"
             SERV_CYQ_END_DT = f"CY{CUR_YR}Q2"
        elif QTR == "Q3":
             SERV_CYQ_BEG_DT = f"CY{CUR_YR}Q1"
             SERV_CYQ_END_DT = f"CY{CUR_YR}Q3"
        elif QTR == "Q4":
             SERV_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
             SERV_CYQ_END_DT = f"CY{PRIOR_YR}Q4"
        
        # For Q1 thru Q4 the SERV and PROC date ranges are the same
        PROC_CYQ_BEG_DT = SERV_CYQ_BEG_DT
        PROC_CYQ_END_DT = SERV_CYQ_END_DT

        rootLogger.info(f"PROC_CYQ_BEG_DT = {PROC_CYQ_BEG_DT}") 
        rootLogger.info(f"PROC_CYQ_END_DT = {PROC_CYQ_END_DT}") 
        rootLogger.info(f"SERV_CYQ_BEG_DT = {SERV_CYQ_BEG_DT}") 
        rootLogger.info(f"SERV_CYQ_END_DT = {SERV_CYQ_END_DT}") 

        EMAIL_MF_FILENAME1=f"P#IDR.XTR.PBAR.PSPS{QTR}(0)"
 
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["SERV_CYQ_BEG_DT"] = SERV_CYQ_BEG_DT
        os.environ["SERV_CYQ_END_DT"] = SERV_CYQ_END_DT
        os.environ["PROC_CYQ_BEG_DT"] = PROC_CYQ_BEG_DT
        os.environ["PROC_CYQ_END_DT"] = PROC_CYQ_END_DT
        os.environ["QTR"] = QTR


        #############################################################
        # Execute Python code to produce extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of PSPS_Extract.py program")
        rootLogger.info("Extract processing for appropriate Qtr between Q1-Q4. ")

        try:
            sp_info = subprocess.run(['python3', 'PSPS_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script PSPS_Extract.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"PSPS Extract (Q1-Q4) - Failed ({ENVNAME})"
            MSG=f"PSPS extract (Q1-Q4) has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script PSPS_Extract.py completed successfully.")


        ############################################
        # If Q5/6 processing --> continue
        # Else --> send email; create manifest file
        ############################################
        run_second_cycle = False

        if QTR == "Q1":
            SERV_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
            SERV_CYQ_END_DT = f"CY{PRIOR_YR}Q4"

            PROC_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
            PROC_CYQ_END_DT = f"CY{CUR_YR}Q1"
            QTR = "Q5" 
            run_second_cycle = True
            
        elif QTR == "Q2":
            SERV_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
            SERV_CYQ_END_DT = f"CY{PRIOR_YR}Q4"

            PROC_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
            PROC_CYQ_END_DT = f"CY{CUR_YR}Q2"
            QTR = "Q6"
            run_second_cycle = True
        else:
            rootLogger.info("Q3/Q4 detected. Skipping second cycle.")
            
        ############################################
        # Perform Qtr 5/6 processing 
        ############################################
        if run_second_cycle:
            rootLogger.info(f"Starting secondary run for {QTR}")
            os.environ["QTR"] = QTR

            rootLogger.info("")
            rootLogger.info("Extract processing for appropriate Qtr between Q5-Q6. ")
            rootLogger.info(f"PROC_CYQ_BEG_DT = {PROC_CYQ_BEG_DT}") 
            rootLogger.info(f"PROC_CYQ_END_DT = {PROC_CYQ_END_DT}") 
            rootLogger.info(f"SERV_CYQ_BEG_DT = {SERV_CYQ_BEG_DT}") 
            rootLogger.info(f"SERV_CYQ_END_DT = {SERV_CYQ_END_DT}") 

            os.environ["TMSTMP"] = TMSTMP
            os.environ["SERV_CYQ_BEG_DT"] = SERV_CYQ_BEG_DT
            os.environ["SERV_CYQ_END_DT"] = SERV_CYQ_END_DT
            os.environ["PROC_CYQ_BEG_DT"] = PROC_CYQ_BEG_DT
            os.environ["PROC_CYQ_END_DT"] = PROC_CYQ_END_DT
            os.environ["QTR"] = QTR


            EMAIL_MF_FILENAME2=f"P#IDR.XTR.PBAR.PSPS{QTR}(0)"

            #############################################################
            # Execute Python code to produce extract
            #############################################################
            rootLogger.info("")
            rootLogger.info("Start execution of PSPS_Extract.py program")
            rootLogger.info("Extract processing for appropriate Qtr between Q5-Q6.")

            try:
                sp_info = subprocess.run(['python3', 'PSPS_Extract.py'], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
            
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling Python script PSPS_Extract.py failed with return code {e.returncode}")
                rootLogger.error("\n%s", e.output)
            
                ## Send Failure email	
                SUBJECT=f"PSPS Extract (Q5-Q6) - Failed ({ENVNAME})"
                MSG=f"PSPS extract (Q5-Q6) has failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script PSPS_Extract.py completed successfully.")

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
        if run_second_cycle:
            filenames_msg = f"{EMAIL_MF_FILENAME1} and {EMAIL_MF_FILENAME2}"
        else:
            filenames_msg = f"{EMAIL_MF_FILENAME1}"
        
        # Send Success email	
        SUBJECT=f"PSPS Quarterly Extract  ({ENVNAME}{TESTEMAIL})"
        MSG=f"The PSPS Quarterly Extract has completed.\n\nA mainframe version of the below file will be created as {filenames_msg}.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PSPS_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
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
        S3BUCKET = rf"{XTR_BUCKET}/{PSPS_BUCKET_FLDR}"  
        
        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT PSPS Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"PSPS_Extract_Driver.py - Failed  ({ENVNAME})"
            MSG= f"EFT process in PSPS_Extract.sh failed"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PSPS_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in PSPS_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in PSPS_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in PSPS_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in PSPS_Extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()