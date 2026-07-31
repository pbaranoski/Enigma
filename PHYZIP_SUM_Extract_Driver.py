#!/usr/bin/bash
#
######################################################################################
# Name:  PHYZIP_SUM_Extracts_Driver.py
#       
# Execute as ./PHYZIP_SUM_Extracts_Driver.py $1
#
# $1 = Override Run date (YYYY-MM-DD format) - execute extract as if it was run on this date.
#    
# DESC:   This script extracts PHYZIP data to replace legacy Mainframe data extract.
#
# Created: Paul Baranoski 4/04/2025
# Modified: 
#
# Paul Baranoski 2025-04-04 Created program.
# Paul Baranoski 2025-04-11 Changed constant name PHYZIP_BOX_RECIPIENT to PHYZIP_BOX_RECIPIENTS.
# Paul Baranoski 2026-06-22 Convert from bash to python.
# Paul Baranoski 2026-06-26 Add sendEmail logic in catch-all ending Exception.
######################################################################################

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

S3BUCKET = rf"{XTR_BUCKET}/{PHYZIP_BUCKET_FLDR}"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}PHYZIP_SUM_Extracts_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPHYZIP_SUM_Extracts_Driver.py started at {TMSTMP}")

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(rootLogger) 

        
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
        # Create parameter date: Determine 1st day of current Month
        #                        or use override date.
        #############################################################
        if iNOFParms ==  1:
            lstParms = sys.argv
            ParmOverrideDate = lstParms[1]

            rootLogger.info("")
            rootLogger.info(f"NOF parameters for script: {iNOFParms}")
            rootLogger.info(f"{ParmOverrideDate=} ")
            
            if validate_dt(ParmOverrideDate):
                dttmParmOverrideDate = datetime.strptime(ParmOverrideDate, "%Y-%m-%d")
                FIRST_DAY_CUR_MONTH =  dttmParmOverrideDate.strftime("%Y-%m-01") 
                
            else:
                rootLogger.info(f"Override parameter date {ParmOverrideDate} is not a valid date. ")
                raise Exception(f"Override parameter date {ParmOverrideDate} is not a valid date. ")
            
        else:
            FIRST_DAY_CUR_MONTH = date.today().strftime("%Y-%m-01")

        # Display parameter date to use.
        rootLogger.info(f"{FIRST_DAY_CUR_MONTH=} ")  


        #############################################################
        # Get Current and Prior Year
        #############################################################
        CUR_YYYY = FIRST_DAY_CUR_MONTH[:4]
        PRIOR_YYYY = str(int(CUR_YYYY) - 1)

        rootLogger.info(f"{CUR_YYYY=}") 
        rootLogger.info(f"{PRIOR_YYYY=}")

        #############################################################
        # Build arrays of Extracts to execute
        #############################################################
        lstEXT_RUNS = []
        lstEXT_RUNS.append(f"{PRIOR_YYYY}Q1,{PRIOR_YYYY}-01-01,{PRIOR_YYYY}-03-31")
        lstEXT_RUNS.append(f"{PRIOR_YYYY}Q2,{PRIOR_YYYY}-04-01,{PRIOR_YYYY}-06-30")
        lstEXT_RUNS.append(f"{PRIOR_YYYY}Q3,{PRIOR_YYYY}-07-01,{PRIOR_YYYY}-09-30")
        lstEXT_RUNS.append(f"{PRIOR_YYYY}Q4,{PRIOR_YYYY}-10-01,{PRIOR_YYYY}-12-31")
        lstEXT_RUNS.append(f"{CUR_YYYY}Q1,{CUR_YYYY}-01-01,{CUR_YYYY}-03-31")
        lstEXT_RUNS.append(f"{CUR_YYYY}Q2,{CUR_YYYY}-04-01,{CUR_YYYY}-06-30")
        
        sExtRuns = '\n'.join(lstEXT_RUNS)
        rootLogger.info(f"lstEXT_RUNS=\n{sExtRuns}")   


        #############################################################
        # Example what extract files will look like and how
        #   they will be renamed as EFT files. And, how those
        #   files will be copied to P#IDR.XTR datasets.
        #############################################################

        #################################################################################
        # Loop thru lstEXT_RUNS reporting periods  
        #################################################################################
        for EXT_INFO in lstEXT_RUNS:

            #############################################################
            rootLogger.info("")
            rootLogger.info("*-----------------------------------*")

            rootLogger.info(f"{EXT_INFO=}")
            
            # FILE_LIT=24Q1
            lstFields = EXT_INFO.split(",") 
            
            # Skip the century for YYYY --> Ex. YYQ1
            FILE_LIT = lstFields[0][2:]
            EXT_FROM_DT = lstFields[1]
            EXT_TO_DT = lstFields[2]
            
            rootLogger.info(f"{FILE_LIT=}")
            rootLogger.info(f"{EXT_FROM_DT=}")
            rootLogger.info(f"{EXT_TO_DT=}")

            #############################################################
            # Make variables available for substitution in Python code
            #############################################################
            os.environ["TMSTMP"] = TMSTMP
            os.environ["FILE_LIT"] = FILE_LIT
            os.environ["EXT_FROM_DT"] = EXT_FROM_DT
            os.environ["EXT_TO_DT"] = EXT_TO_DT


            #############################################################
            # Execute Python code to Extract claims data.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Start execution of PHYZIP_SUM_Extract.py program")

            try:
                sp_info = subprocess.run(['python3', 'PHYZIP_SUM_Extract.py'], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling PHYZIP_SUM_Extract.py failed with return code {e.returncode}")
                rootLogger.error(e.stdout)
                rootLogger.error(e.stderr)
                
                ## Send Failure email	
                SUBJECT=f"PHYZIP_SUM_Extract.py - Failed ({ENVNAME})"
                MSG=f"Python script PHYZIP_SUM_Extract.py failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    
        

            rootLogger.info("")
            rootLogger.info("Python script PHYZIP_SUM_Extract.py completed successfully.")


        #############################################
        # End of Loop
        #############################################


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
        SUBJECT=f" PHYZIP_SUM_Extract_Driver.py - completed ({ENVNAME}{TESTEMAIL})"
        MSG=f" PHYZIP_SUM_Extract_Driver.py completed. \n\nThe following extract files were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PHYZIP_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)

            sys.exit(12)   
            

        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT PHYZIP Extract Files ")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"PHYZIP SUM Extract EFT process   - Failed ({ENVNAME})"
            MSG= f"PHYZIP SUM Extract EFT process  has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PHYZIP_SUM_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in PHYZIP_SUM_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in PHYZIP_SUM_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in PHYZIP_SUM_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in PHYZIP_SUM_Extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()
    
