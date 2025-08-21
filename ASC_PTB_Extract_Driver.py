#!/usr/bin/env python
########################################################################################################
# Name:  ASC_PTB_Extract_Driver.py
#
# Desc: ASC (Ambulatory Surgical Center PTB extract. Designed to run in Annually in April 
#
# Execute as python3 ASC_PTB_Extract.py   
#   
# Paul Baranoski   2025-07-25 Create Module.
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

# Our common module with variable constants
from SET_XTR_ENV import *

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

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
def setLogging(LOGNAME):

    # Configure root logger
    #logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(funcName)-22s %(message)s",
        encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
        #filename=f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"
        handlers=[
        logging.FileHandler(f"{LOGNAME}"),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)
 
    global rootLogger
    rootLogger = logging.getLogger() 
  
    os.chmod(LOG_DIR, 0o777)  # for Python3
    
    #logger.setLevel(logging.INFO)


def validate_dt(sDate2Validate):


    try:

        datetime_obj = datetime.strptime(sDate2Validate, "%Y-%m-%d")
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"blbtn_clm_ext_Driver.py - Failed ({ENVNAME})"
        MSG=f"Parameter date {sDate2Validate} is either an invalid date or not formatted correctly. Date must be in YYYY-MM-DD format. Process failed. "
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        rootLogger.info(sp_info) 
        
        sys.exit(12)
            

def build_week_dt_parms(): 

    #######################################################
    # Tell python these are global and not local variables
    #######################################################
    global wkly_strt_dt
    global wkly_end_dt

    rootLogger.info(f"In build_week_dt_parms function")
    
    # get (current date - 14 days) 
    dttmCalcDate = (datetime.today() + timedelta(days=-14))
    # get dow: Monday, Tuesday, etc.
    dow = dttmCalcDate.strftime('%A')
    wkly_strt_dt = dttmCalcDate.strftime('%Y-%m-%d')    

    # if current date is Monday --> skip loop
    # if not Monday --> find Monday prior to today )

    while dow != "Monday":

        dttmCalcDate = (dttmCalcDate + timedelta(days=-1))
        # get dow: Monday, Tuesday, etc.
        dow = dttmCalcDate.strftime('%A')
        wkly_strt_dt = dttmCalcDate.strftime('%Y-%m-%d')  
        rootLogger.info(f"{dow=}") 
        rootLogger.info(f"{wkly_strt_dt=}") 	   


    # find end of week from selected Monday (a week before the decrement value
    dttmCalcEndDate = (dttmCalcDate + timedelta(days=6))
    wkly_end_dt = dttmCalcEndDate.strftime('%Y-%m-%d')
    rootLogger.info(f"{wkly_end_dt=}")


def main_processing_loop():

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}ASC_PTB_Extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        setLogging(LOGNAME)
        rootLogger.info(f"\nASC_PTB_Extract_Driver.py started at {TMSTMP}")

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
        CLM_EFCT_DT_END = f"{CURR_YYYY}0301"
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
        rootLogger.info("Start execution of ASC_PTB_Extract_Driver.py program")
        
        
        try:
            sp_info = subprocess.check_output(['python3', 'ASC_PTB_Extract.py'], text=True)
            rootLogger.info(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ASC_PTB_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"ASC_PTB_Extract_Driver.py - Failed ({ENVNAME})"
            MSG=f"ASC_PTB_Extract_Driver.py has failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 

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
       
        SUBJECT=f"ASC PTB extract ({ENVNAME})" 
        MSG=f"The Extract for the creation of the ASC PTB file from Snowflake has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Sending Success email in ASC_PTB_Extract.sh - Failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    


        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT ASC PTB Extract Files ")
        
        try:
            sp_info = subprocess.check_output(['bash', 'ProcessFiles2EFT.sh', ASC_PTB_BUCKET ], text=True)
            rootLogger.info(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"ASC PTB extract EFT process  - Failed ({ENVNAME})"
            MSG= f"ASC PTB Extract EFT process has failed."

            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 

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

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()