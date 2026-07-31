#!/usr/bin/bash
############################################################################################################
# Name:  VAPTD_Driver.py
#
# Desc: VA Part D Quaterly Extract
#
# Author     : Joshua Turner	
# Created    : 12/19/2022
#
# Execute as python3 VAPTD_Driver.py     (processing without override date) 
# Execute as python3 VAPTD_Driver.py $1  (processing with override date --> YYYY-MM-DD format) 
#
# 			$1 = From_dt (YYYY-MM-DD)
#
# Modified:
# Joshua Turner    2022-12-19  New script.
# Paul Baranoski   2023-11-28  Add exit 12 and failure email when script run in incorrect time period. 
#                              Add call to CreateManifestFile.sh with S3 mainfest file override bucket.
#                              Add ENVNAME to email Subject line. 
# Paul Baranoski   2023-12-04  Add FilenameCounts.bash and update extract filename logic for email.
# Joshua Turner    2024-05-21  Changed date and filename parms for Q1 to be -le '05' in case FF is later than usual
#                              Modified manifest bucket to VA_PBM 
# Joshua Turner    2024-05-22  For a more standard process and for file count standards, I am splitting the finder file load for Q1 out
#                              of this script to it's own process. When scheduled; Q1 will execute the finder
#                              file load, then extract. Q2 - Q4 will execute just the extract portion. This will be controlled by rundeck
#                              or other scheduler tool 
# Paul Baranoski   2024-11-05  Modified ending line to be "Ended at.." because Dashboard script is looking for that to know if extract ended successfully.
# Paul Baranoski   2025-01-02  Modified 'echo "" >> "Creating Manifest file for: ${VAPTD_FILE}" >> ${LOGNAME}'
#                                    to 'echo "Creating Manifest file for: ${VAPTD_FILE}" >> ${LOGNAME}'. 
#                              The original statement was creating a bogus filename instead of writing message to log file.  
# Paul Baranoski   2026-04-23  Convert from bash to python.
# Paul Baranoski   2026-07-14  Modify error message to reflect new python name.
#                              Added logic to allow overrider parameter date so the program can be tested outside of the normal run times.
############################################################################################################

########################################################################################################
# Set TESTING status 
########################################################################################################
import os
os.environ["TESTING"] = "N"

# This switch is needed to prevent Request Email addresses from being include in error and success emails and manifest files.
swInTESTMode = os.getenv("TESTING","N") 

# Our common module with variable constants
from SET_XTR_ENV import *

########################################################################################################
# IMPORTS
########################################################################################################
import boto3 
import logging
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta
import time

import os
import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

DATADIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

#S3BUCKET = rf"{XTR_BUCKET}/{OFM_PDE_BUCKET_FLDR}"
FINDER_FILE_BUCKET = rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"



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

        LOGNAME = f"{LOG_DIR}{TESTLOG}VAPTD_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nVAPTD_Driver.py started at {TMSTMP}")

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
        # Establish Date Parameters  
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Determine Date Parameters")

        if iNOFParms ==  1:
            lstParms = sys.argv
            ParmOverrideDate = lstParms[1]
            rootLogger.info(f"Override parameter date {ParmOverrideDate}")

            CURR_YEAR = ParmOverrideDate[0:4]
            MONTH = ParmOverrideDate[5:7]            
        else:    
            dtTodayDt = date.today()
            CURR_YEAR = dtTodayDt.strftime("%Y")
            MONTH = dtTodayDt.strftime("%m")            


        if MONTH <= "05":
            PREV_YEAR = str(int(CURR_YEAR) - 1)
            START_DATE = f"{PREV_YEAR}-10-01"
            END_DATE = f"{PREV_YEAR}-12-31"
            QTR = f"FY{CURR_YEAR}Q1"
        elif MONTH == "06":
            START_DATE = f"{CURR_YEAR}-01-01"
            END_DATE = f"{CURR_YEAR}-03-31"
            QTR = f"FY{CURR_YEAR}Q2"
        elif MONTH == "09":
            START_DATE = f"{CURR_YEAR}-04-01"
            END_DATE = f"{CURR_YEAR}-06-30"
            QTR = f"FY{CURR_YEAR}Q3"
        elif MONTH == "12":
            START_DATE = f"{CURR_YEAR}-07-01"
            END_DATE = f"{CURR_YEAR}-09-30"
            QTR = f"FY{CURR_YEAR}Q4"
        else:
            rootLogger.error("Extract is processed quarterly for months March, June, September, and December. ")
            rootLogger.error("Extract is not scheduled to run for this time period. ")
            rootLogger.error("Processing completed.")

            # Send failure email
            SUBJECT = f"VAPTD_Driver.py - FAILED ({ENVNAME}{TESTEMAIL})"
            MSG = f"Extract is processed quarterly for months March, June, September, and December. Extract is not scheduled to run for this time period."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
            sys.exit(12)
		

        #############################################################
        # Display Date Parameters in log
        #############################################################
        rootLogger.info("")
        rootLogger.info("VAPTD Extract Processing will use the following dates:")
        rootLogger.info(f"{START_DATE=}")
        rootLogger.info(f"{END_DATE=}")
        rootLogger.info(f"{QTR=}")


        ###########################################################################################
        # Execute python script to extract VA Part D data and load the extract to S3 
        ###########################################################################################
        os.environ["START_DATE"] = START_DATE
        os.environ["END_DATE"] = END_DATE
        os.environ["QTR"] = QTR
        os.environ["TMSTMP"] = TMSTMP


        #############################################################
        # Execute Python code to extract data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of VAPTD_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'VAPTD_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling VAPTD_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"VAPTD_Driver.py extract - Failed ({ENVNAME}{TESTEMAIL})"
            MSG=f"Calling VAPTD_Driver.py failed with return code {e.returncode} "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)  
        
        # call was successful
        rootLogger.info("Python script OFM_PDE_Extract.py completed successfully. ")


        ####################################################################
        # Concatenate VA PTD S3 files into a single file 
        # NOTE: Multiple files with suffix "n_n_n.txt.gz" are created. 
        #       Will concatenate them into single file.
        #
        # Example --> blbtn_clm_ex_20220922.084321.txt.gz_0_0_0.txt.gz 
        #         --> blbtn_clm_ex_20220922.084321.txt.gz
        ####################################################################
        rootLogger.info("")
        rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 

        VAPTD_BUCKET = f"{XTR_BUCKET}/{VAPTD_BUCKET_FLDR}"
        rootLogger.info(f"{VAPTD_BUCKET=} ")
        
        VAPTD_FILE = f"MOA_VAPARTD_{QTR}_{TMSTMP}.csv.gz"
        sConcatFilename = VAPTD_FILE
        rootLogger.info(f"{sConcatFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', VAPTD_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)
            #sp_info = subprocess.run(['python3', 'CombineS3FilesDriver.py', VAPTD_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)

            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Combining S3 files in VAPTD_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
            MSG=f"Combining S3 files in VAPTD_Driver.py has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
            

        #############################################################
        # Get list of S3 files and record counts for success email.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Get S3 Extract file list and record counts")
        
        # Retrieve extract files and record counts from temp log file
        S3Files = getExtractFilenamesAndCounts(rootLogger, LOGNAME)   
    
        rootLogger.info(f"{S3Files=}")


        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")
       
        SUBJECT=f"VA Part D Extract Complete ({ENVNAME}{TESTEMAIL})" 
        MSG=f"VA Part D quarterly extract completed successfully.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, VAPTD_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)  


        ######################################################################
        # Create Manifest file (Supply ManifestFileFolder override parameter).
        ######################################################################
        rootLogger.info("")
        rootLogger.info(f"Creating manifest file for {VAPTD_FILE}. ")

        #####################################################
        # bucket/s3folder --> points to location of extract file.
        #          TMSTMP --> uniquely identifies extract file(s)
        #       BoxEmails --> manifest file recipients
        #####################################################
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=VAPTD_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=VAPTD_EMAIL_BOX_RECIPIENT, Manifest_folder=MANIFEST_VA_PBM_BUCKET_FLDR )

        except Exception as e:

            SUBJECT=f"Create Manifest file in VAPRD_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
            MSG=f"Create Manifest file in VAPRD_Driver.py  has failed. {e}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            sys.exit(12) 

                
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("VA Part D Quarterly Extract completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in VAPRD_Driver.py\n {e}")

        rootLogger.error("Exception occured in VAPRD_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT=f"VAPRD_Driver.py  - Failed ({ENVNAME}{TESTEMAIL})"
        MSG=f"Exception occured in VAPRD_Driver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()