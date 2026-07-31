#!/usr/bin/bash
############################################################################################################
# Name: RAND_FFS_PTA_HOS.py
# Desc: RAND FFS Part A Extract for HOS
#
# Author     : Joshua Turner	
# Created    : 3/3/2023
#
# Modified:
# Joshua Turner 	2023-03-03 new script.
# Paul Baranoski    2024-03-12 Modify logic for extract year to be 2 years prior to current year.
#                              Add call to create manifest file.
#                              Add ENVNAME to SUBJECT of all emails.
#                              Change process to get filename prefix for combine
# Paul Baranoski    2026-04-23 Convert from bash to python. Move "driver" logic out of SQL module into "driver module. 
#                              Called new SQL module RAND_PartA_Extract_v2.py. 
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

        LOGNAME = f"{LOG_DIR}{TESTLOG}RAND_FFS_PTA_HOS_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nRAND_FFS_PTA_HOS.py started at {TMSTMP}")

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
        rootLogger.info(f"Create Parameters")
       
        dtTodayDt = date.today()
        CURR_YEAR = dtTodayDt.strftime("%Y")
        EXT_YYYY = str(int(CURR_YEAR) - 2)
        CLM_TYPE_CD = "50"

        #############################################################
        # Display Parameters in log
        #############################################################
        rootLogger.info("")
        rootLogger.info("RAND FFS PTA HOS Processing will use the following parameters:")
        rootLogger.info(f"{EXT_YYYY=}")
        rootLogger.info(f"{CLM_TYPE_CD=}")


        #############################################################
        # Process Extract thru date ranges
        #############################################################
        start_date_parms = ['-01-01', '-04-01', '-07-01', '-10-01']
        end_date_parms = ['-03-31', '-06-30', '-09-30', '-12-31']
            
        for i in range(len(start_date_parms)):
            # Ex. 2026-01-01, 2026-03-31
            START_DATE = f"{EXT_YYYY}{start_date_parms[i]}"
            END_DATE = f"{EXT_YYYY}{end_date_parms[i]}"
            RNG = i + 1

            XTR_FILE_NAME = f"RAND_FFS_HOS_Y{EXT_YYYY}_P{RNG}_{TMSTMP}.csv.gz"
            
            rootLogger.info(f"{START_DATE=}")
            rootLogger.info(f"{END_DATE=}")
            rootLogger.info(f"{XTR_FILE_NAME=}")
 
            ###########################################################################################
            # Set environment variables
            ###########################################################################################
            os.environ["CLM_TYPE_CD"] = CLM_TYPE_CD
            os.environ["START_DATE"] = START_DATE
            os.environ["END_DATE"] = END_DATE

            os.environ["XTR_FILE_NAME"] = XTR_FILE_NAME          

            #############################################################
            # Execute Python code to extract data.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Start execution of RAND_FFS_PartA_Extract_v2.py program")

            try:
                sp_info = subprocess.run(['python3', 'RAND_FFS_PartA_Extract_v2.py'], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling RAND_FFS_PartA_Extract_v2.py failed with return code {e.returncode}")
                rootLogger.error(e.output)
                
                ## Send Failure email	
                SUBJECT=f"RAND_FFS_PTA_HOS.py extract - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"Calling RAND_FFS_PartA_Extract_v2.py failed with return code {e} "
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)  
            
            # call was successful
            rootLogger.info("Python script RAND_FFS_PartA_Extract_v2.py completed successfully. ")


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

            RAND_FFSPTAB_BUCKET = f"{XTR_BUCKET}/{RAND_FFSPTAB_BUCKET_FLDR}"
            rootLogger.info(f"{RAND_FFSPTAB_BUCKET=} ")
            
            sConcatFilename = XTR_FILE_NAME
            rootLogger.info(f"{sConcatFilename=}")

            try:
                sp_info = subprocess.run(['bash', 'CombineS3Files.sh', RAND_FFSPTAB_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)

                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
                rootLogger.error(e.output)
                
                ## Send Failure email	
                SUBJECT=f"Combining S3 files in RAND_FFS_PTA_HOS.py - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"Combining S3 files in RAND_FFS_PTA_HOS.py has failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)   

        # end-for


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
       
        SUBJECT=f"RAND FFS PTA HOS Extract Complete ({ENVNAME}{TESTEMAIL})" 
        MSG=f"RAND FFS PTA HOS Extract completed successfully.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RAND_FFS_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)  


        ######################################################################
        # Create Manifest file (Supply ManifestFileFolder override parameter).
        ######################################################################
        rootLogger.info("")
        rootLogger.info(f"Creating manifest files for RAND FFS PTA HOS Extract. ")

        #####################################################
        # bucket/s3folder --> points to location of extract file.
        #          TMSTMP --> uniquely identifies extract file(s)
        #       BoxEmails --> manifest file recipients
        #####################################################
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=RAND_FFSPTAB_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=RAND_FFS_BOX_RECIPIENTS, Manifest_folder=MANIFEST_HOLD_BUCKET_FLDR )

        except Exception as e:

            SUBJECT=f"Create Manifest file in RAND_FFS_PTA_HOS.py - Failed ({ENVNAME}{TESTEMAIL})"
            MSG=f"Create Manifest file in RAND_FFS_PTA_HOS.py  has failed. {e}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            sys.exit(12) 

                
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("RAND FFS PTA HOS Extract completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in RAND_FFS_PTA_HOS.py\n {e}")

        rootLogger.error("Exception occured in RAND_FFS_PTA_HOS.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT=f"RAND_FFS_PTA_HOS.py  - Failed ({ENVNAME}{TESTEMAIL})"
        MSG=f"Exception occured in RAND_FFS_PTA_HOS.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()
