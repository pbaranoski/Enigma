#!/usr/bin/env python
########################################################################################################
# Name:  blbtn_drug_prvdr_ext_Driver.py
#
# Desc: Extract Blue Button drug/provider data (IDR#BLB3/IDR#BLB4).  
#
# Execute as python3 blbtn_drug_prvdr_ext_Driver.py       (processing without override dates) 
#
#   
# Paul Baranoski   2025-07-21 Create Module.
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

BLBTN_BUCKET = rf"{XTR_BUCKET}/{BLBTN_BUCKET_FLDR}"


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

        datetime_str = datetime.strptime(sDate2Validate, "YYYY-MM-DD")
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"blbtn_drug_prvdr_ext_Driver.py - Failed ({ENVNAME})"
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

        LOGNAME = f"{LOG_DIR}blbtn_drug_prvdr_ext_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        setLogging(LOGNAME)
        rootLogger.info(f"\nblbtn_drug_prvdr_ext_Driver.py started at {TMSTMP}")

        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")
 
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP


        #############################################################
        # Execute Python code - Drug Extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of blbtn_drug_ext.py program")

        try:
            sp_info = subprocess.check_output(['python3', 'blbtn_drug_ext.py'], text=True)
            rootLogger.info(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling blbtn_drug_ext.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Weekly Blue Button Drug Extract - Failed ({ENVNAME})"
            MSG=f"The weekly Blue Button Drug extract has failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script blbtn_drug_ext.py completed successfully.")


        #############################################################
        # Execute Python code - Provider Extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of blbtn_prvdr_ext.py program")

        try:
            sp_info = subprocess.check_output(['python3', 'blbtn_prvdr_ext.py'], text=True)
            rootLogger.info(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling blbtn_prvdr_ext.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Weekly Blue Button Provider Extract - Failed ({ENVNAME})"
            MSG=f"The weekly Blue Button Provider extract has failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script blbtn_prvdr_ext.py completed successfully.")

  
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
       
        SUBJECT=f"Weekly Blue Button drug/provider extract ({ENVNAME})" 
        MSG=f"Weekly Blue Button drug/provider extract has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    


        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT Blue Button Drug/Provider Extract File ")
        
        try:
            sp_info = subprocess.check_output(['bash', 'ProcessFiles2EFT.sh', BLBTN_BUCKET ], text=True)
            rootLogger.info(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"Blue Button Drug/Provider Extract EFT process  - Failed ({ENVNAME})"
            MSG= f"Blue Button Drug/Provider Extract EFT process has failed."

            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script blbtn_drug_prvdr_ext_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in blbtn_drug_prvdr_ext_Driver.py\n {e}")

        rootLogger.error("Exception occured in blbtn_drug_prvdr_ext_Driver.py.")
        rootLogger.error(e)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()