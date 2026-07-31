#!/usr/bin/env python
########################################################################################################
# Name:  STS_MED_INS_MN_Rpts_Driver.py
#
# DESC:   This script extracts data for STS Medical Insureance Minnesota report - NOF Bills; Amt Reimbursed 
#         by period expense (legacy BB2A report)#
#
# Execute as python3 STS_MED_INS_MN_Rpts_Driver.py     (processing without override date) 
# Execute as python3 STS_MED_INS_MN_Rpts_Driver.py $1  (processing with override date --> YYYY-MM-DD format) 
#
# 			$1 = From_dt (YYYY-MM-DD)
#
#   
# Paul Baranoski   2025-08-29 Create Module.
# Paul Baranoski   2025-09-26 Modify subprocess.run to subprocess.run which allows to capture stderr as well as stdout. 
#                             Add write_sp_info_2_log function and companion logging import module LoggerStandard. 
# Paul Baranoski   2025-10-15 Modify e.output written to log file as ("\n%s", e.output) to preserve any new lines.
# Paul Baranoski   2025-10-20 Subprocess.run was missing "capture_output=True, text=True, check=True" function parameters when writing email for validating date.
# Paul Baranoski   2025-11-21 Correct syntax error on log message. Had extract right paren.
# Vijay Mandavilli 2026-05-12 Updated code with common functions and test logic.
# Paul Baranoski   2026-07-10 For subprocess.CalledProcessError Add code to write e.stdout and e.stderr to log file. 
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

import os
import subprocess

import CreateManifestFileDriver as CreManDr

# Our common module with variable constants
from SET_XTR_ENV import *

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import * 

S3BUCKET = rf"{XTR_BUCKET}/{STS_MED_INS_MN_BUCKET_FLDR}"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}STS_MED_INS_MN_Rpts_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nSTS_MED_INS_MN_Rpts_Driver.py started at {TMSTMP}")

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

        if iNOFParms == 1:
            lstParms = sys.argv

            ParmOverrideDate = lstParms[1]
            
            rootLogger.info("")
            rootLogger.info(f"{ParmOverrideDate=} ")            
            
            parmDt = datetime.strptime(ParmOverrideDate,"%Y-%m-%d")
            CUR_MM = parmDt.strftime("%m")
            CUR_YYYY = parmDt.year
            PRIOR_YYYY = CUR_YYYY - 1
        else:
            todayDt = date.today()
            
            CUR_MM = todayDt.strftime("%m")
            CUR_YYYY = todayDt.year
            PRIOR_YYYY =  CUR_YYYY - 1

        rootLogger.info(f"{CUR_MM=}") 
        rootLogger.info(f"{CUR_YYYY=}")  
        rootLogger.info(f"{PRIOR_YYYY=}") 


        # script should run second Friday of July or January
        if CUR_MM == "07":
            EXT_TO_DATE = f"{CUR_YYYY}-06-30"
            RUN_PRD = "JUN"
            
        elif CUR_MM == "01":
            EXT_TO_DATE = f"{PRIOR_YYYY}-12-31"
            RUN_PRD = "DEC"
        else:
            rootLogger.info("")
            rootLogger.info(f"Not a valid processing month {CUR_MM} ")
            
            # Send Failure email	
            SUBJECT=f"STS Medical Insurance Minnesota Report - Failed ({ENVNAME})"
            MSG=f"Not a valid processing month: {CUR_MM}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
            sys.exit(12)

        ###########################################################
        # Display extract dates to use.
        ###########################################################
        rootLogger.info(f"{EXT_TO_DATE=}")
        rootLogger.info(f"{RUN_PRD=}")  
        
        # Create YYYY range of 7 years
        EXT_TO_YYYY = EXT_TO_DATE [:4]
        EXT_FROM_YYYY = str(int(EXT_TO_YYYY) - 6)

        rootLogger.info(f"{EXT_TO_YYYY=}")
        rootLogger.info(f"{EXT_FROM_YYYY=}")

  
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["RUN_PRD"] = RUN_PRD
        os.environ["EXT_FROM_YYYY"] = EXT_FROM_YYYY
        os.environ["EXT_TO_DATE"] = EXT_TO_DATE


        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of STS_MED_INS_MN_Rpts.py program")

        try:
            sp_info = subprocess.run(['python3', 'STS_MED_INS_MN_Rpts.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling STS_MED_INS_MN_Rpts.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.stdout)
            rootLogger.error("\n%s", e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"STS Medical Insurance Minnesota Report - Failed ({ENVNAME})"
            MSG=f"Python script STS_MED_INS_MN_Rpts.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script STS_MED_INS_MN_Rpts_Driver.py completed successfully.")

  
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
        SUBJECT=f"STS MED INS MN Report - completed ({ENVNAME}{TESTEMAIL})"
        MSG=f"STS Medical Insurance Minnesota Report completed. \n\nThe following extract files were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, STS_MED_INS_MN_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    


        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Create Manifest file for STS Medical Insurance Minnesota Report. ")

        #####################################################
        # bucket/s3folder --> points to location of extract file.
        #                 --> S3 folder is key token to config file to determine if manifest file is in HOLD status
        #          TMSTMP --> uniquely identifies extract file(s)
        #       BoxEmails --> manifest file recipients
        #####################################################
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=STS_MED_INS_MN_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=STS_MED_INS_MN_BOX_RECIPIENT )

        except Exception as e:

            SUBJECT=f"Create Manifest file in STS_MED_INS_MN_Rpts_Driver.py - Failed ({ENVNAME})"
            MSG=f"Create Manifest file in STS_MED_INS_MN_Rpts_Driver.py  has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            raise


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script STS_MED_INS_MN_Rpts_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in STS_MED_INS_MN_Rpts_Driver.py\n {e}")

        rootLogger.error("Exception occured in STS_MED_INS_MN_Rpts_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in STS_MED_INS_MN_Rpts_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in STS_MED_INS_MN_Rpts_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()