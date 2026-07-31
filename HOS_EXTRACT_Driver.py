#!/usr/bin/env python
########################################################################################################
# Name:  HOS_EXTRACT_Driver.py
#
# Description: This script executes the python that creates the two HOS extracts for H and M contract types.
#              General script flow:
#              (1)  Extract for H contractor using HFILE finder file
#                (1.1) Combine all H file segments
#                (1.2) Create Box manifest file for HFILE extract
#
#              (2)  Extract for M contractor using MFILE finder file
#                (2.1)  Combine all M file segments
#                (2.2)  Create Box manifest file for MFILE extract
#
# Viren Khanna   2026-04-03 Create Module.
# Paul Baranoski 2026-04-20 Remove '$' from sConcatMFilename to allow "parts" to be combined.
########################################################################################################
import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *

import boto3 
import logging
import sys
import argparse
import glob

#import datetime
from datetime import datetime
from datetime import date,timedelta

import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog

HOS_BUCKET = rf"{XTR_BUCKET}/{HOS_BUCKET_FLDR}"


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Functions
#############################################################
def write_sp_info_2_log(sp_info):
        
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    rootLogger.info("\n%s", sp_info.stdout) 
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    rootLogger.info("\n%s", sp_info.stderr) 
    rootLogger.info(f"{sp_info.returncode=}")  
    
def validate_dt(sDate2Validate):


    try:

        datetime_obj = datetime.strptime(sDate2Validate, "%Y-%m-%d")
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"HOS_EXTRACT_Driver.py - Failed ({ENVNAME})"
        MSG=f"Parameter date {sDate2Validate} is either an invalid date or not formatted correctly. Date must be in YYYY-MM-DD format. Process failed. "
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

        LOGNAME = f"{LOG_DIR}{TESTLOG}HOS_Extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nHOS_EXTRACT_Driver.py started at {TMSTMP}")

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
        if not (iNOFParms == 0 or iNOFParms ==  1):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")

            
        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################
        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")

        if iNOFParms == 0:
            lstParms = sys.argv

            now = datetime.now()
            YEAR = now.year
            MONTH = now.month
            SDATE = f"{YEAR}-{MONTH:02}-01"
            
        rootLogger.info(SDATE)
        rootLogger.info(f"{YEAR=}") 
        rootLogger.info(f"{MONTH=}")  
        

        ###########################################################
        #  Set and export other parameters for the SQL
        ###########################################################
        
        # Passing Parameters to Python script
        HOS_FF_TABLE="HOSHFF"
        FILETYPE="HFILE"

        rootLogger.info(f"{HOS_FF_TABLE=}")
        rootLogger.info(f"{FILETYPE=}")
       
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["SDATE"] = str(SDATE)
        os.environ["YEAR"] = str(YEAR)
        os.environ["HOS_FF_TABLE"] = HOS_FF_TABLE
        os.environ["FILETYPE"] = FILETYPE
        os.environ["MONTH"] = str(MONTH)

        #############################################################
        # Call HOS_EXTRACT.py with the variables for 'H' type 
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of HOS_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'HOS_EXTRACT.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling HOS_Extract.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"HOS_Extract- Failed ({ENVNAME})"
            MSG=f"Python script HOS_Extract failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script HOS_Extract.py completed successfully.")

        ###########################################################################################
        # Combine 'H' File Type
        ###########################################################################################
        rootLogger.info("")
        rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 

        rootLogger.info(f"{HOS_BUCKET=} ")

        sConcatHFilename = f"HOS_XTR_Y{YEAR}_{FILETYPE}_{TMSTMP}.csv.gz"
        rootLogger.info(f"{sConcatHFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', HOS_BUCKET, sConcatHFilename ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Combining S3 files in HOS_EXTRACT_Driver.py - Failed ({ENVNAME})"
            MSG=f"Combining S3 files in HOS_EXTRACT_Driver.py has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Create Manifest file for {sConcatHFilename}.")

        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=HOS_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=HOS_EMAIL_BOX_RECIPIENT )

        except Exception as e:

            SUBJECT=f"Create Manifest file in HOS_EXTRACT_Driver.py - Failed ({ENVNAME})"
            MSG=f"Create Manifest file in HOS_EXTRACT_Driver.py  has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            # re-raise exception
            raise
        
        ###########################################################
        #  Set and export other parameters for the SQL
        ###########################################################
        
        # Passing Parameters to Python script
        HOS_FF_TABLE="HOSMFF"
        FILETYPE="MFILE"

        rootLogger.info(f"{HOS_FF_TABLE=}")
        rootLogger.info(f"{FILETYPE=}")
       
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["SDATE"] = str(SDATE)
        os.environ["YEAR"] = str(YEAR)
        os.environ["HOS_FF_TABLE"] = HOS_FF_TABLE
        os.environ["FILETYPE"] = FILETYPE
        os.environ["MONTH"] = str(MONTH)

        #############################################################
        # Call HOS_EXTRACT.py with the variables for 'M' type 
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of HOS_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'HOS_EXTRACT.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling HOS_Extract.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"HOS_Extract- Failed ({ENVNAME})"
            MSG=f"Python script HOS_Extract failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script HOS_Extract.py completed successfully.")

        ###########################################################################################
        # Combine 'M' File Type
        ###########################################################################################
        rootLogger.info("")
        rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 

        rootLogger.info(f"{HOS_BUCKET=} ")

        sConcatMFilename = f"HOS_XTR_Y{YEAR}_{FILETYPE}_{TMSTMP}.csv.gz"
        rootLogger.info(f"{sConcatMFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', HOS_BUCKET, sConcatMFilename ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Combining S3 files in HOS_EXTRACT_Driver.py - Failed ({ENVNAME})"
            MSG=f"Combining S3 files in HOS_EXTRACT_Driver.py has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Create Manifest file for {sConcatMFilename}.")

        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=HOS_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=HOS_EMAIL_BOX_RECIPIENT )

        except Exception as e:

            SUBJECT=f"Create Manifest file in HOS_EXTRACT_Driver.py - Failed ({ENVNAME})"
            MSG=f"Create Manifest file in HOS_EXTRACT_Driver.py  has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            # re-raise exception
            raise

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
        SUBJECT=f" Health Outcome Survey (HOS) for {SDATE} ({ENVNAME}{TESTEMAIL})"
        MSG=f" The extract for Health Outcome Survey has been completed \n\nThe following extract files were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, HOS_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script HOS_EXTRACT_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in HOS_EXTRACT_Driver.py\n {e}")

        rootLogger.error("Exception occured in HOS_EXTRACT_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in HOS_EXTRACT_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in HOS_EXTRACT_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()