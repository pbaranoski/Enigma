#!/usr/bin/bash
############################################################################################################
# Name:  MNUP_MED_NONUTIL_Monthly_ext._Driver.py
#
# Desc: MNUP Monthly Extract of Medical Non-Utilization for SSA
# !!!!! NOTE : - FOR PREVIOUS MONTH WE NEED TO RUN THIS EXTRACT BEFORE 27TH OF EACH MONTH OTHERWISE IT WILL NOT GIVE CORRECT DATE
# Author     Viren Khanna
# Created    : 07/23/2024
#
# Modified:
#
# Viren Khanna 2024-07-23 Created script.
# Viren Khanna 2026-07-02 Convert bash to python.
############################################################################################################


import os
os.environ["TESTING"] = "Y"

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


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

S3BUCKET = rf"{XTR_BUCKET}/{MNUP_MONTHLY_BUCKET_FLDR}"

PREFIX = "MNUP_MONTHLY_FNDR"
MANIFEST_FILE_HLQ = "MNUP_Monthly"
SFTP_DEST_FLDR = "MNTHLYMNUP"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}MNUP_MED_NONUTIL_Monthly_ext_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nMNUP_MED_NONUTIL_Monthly_ext._Driver.py started at {TMSTMP}")

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
        if not (iNOFParms == 0 ):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")
            

        ##########################################
        # Display Bucket values
        ##########################################
        rootLogger.info(f"{S3BUCKET=}")
        rootLogger.info(f"{FINDER_FILE_BUCKET_FLDR=}")
        rootLogger.info(f"{FINDER_FILE_SSA_BUCKET_FLDR=}")
        rootLogger.info(f"{SFTP_FOLDER=}")

        rootLogger.info(f"{MANIFEST_SSA_BUCKET_FLDR=}")


        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_MNUP_Monthly_FNDR_FILE_Driver program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_MNUP_Monthly_FNDR_FILE_Driver.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling LOAD_MNUP_Monthly_FNDR_FILE_Driver failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"LOAD_MNUP_Monthly_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
            MSG=f"LOAD_MNUP_Monthly_FNDR_FILE_Driver.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
        
        
        rootLogger.info("")
        rootLogger.info("bash script LOAD_MNUP_Monthly_FNDR_FILE_Driver completed successfully.")
            

        #################################################################################
        # Create Extract Date parameter
        #################################################################################
        rootLogger.info("") 
        rootLogger.info("Create date parameter for the Python Extract program.")

        CUR_YY_MM = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

        # Create date parameter for Prior Year
        CUR_YYYY = date.today().strftime("%Y")
        MNUP_YR = str(int(CUR_YYYY))
        PRIOR_MM = CUR_YY_MM[5:7] 
        #PRIOR_MM = str(12)
        PRIOR_YYYY = str(int(CUR_YYYY) - 1)
        #PRIOR_YYYY = str(int(CUR_YYYY) )

        rootLogger.info(f"{CUR_YYYY=}")
        rootLogger.info(f"{MNUP_YR=}")
        rootLogger.info(f"{PRIOR_YYYY=}")
        rootLogger.info(f"{PRIOR_MM=}")

        # Export environment variables for Python extracts code
        os.environ["TMSTMP"] = TMSTMP
        os.environ["MNUP_YR"] = MNUP_YR
        os.environ["PRIOR_YYYY"] = PRIOR_YYYY
        os.environ["PRIOR_MM"] = PRIOR_MM

        
        #############################################################
        # Execute Python code to extract data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of MNUP_MED_NONUTIL_Monthly_ext.py program")

        try:
            sp_info = subprocess.run(['python3', 'MNUP_MED_NONUTIL_Monthly_ext.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling MNUP_MED_NONUTIL_Monthly_ext.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"MNUP_MED_NONUTIL_Monthly_ext.py - Failed ({ENVNAME})"
            MSG=f"Python script MNUP_MED_NONUTIL_Monthly_ext.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script MNUP_MED_NONUTIL_Monthly_ext.py completed successfully.")


        ################################################################
        # Create EFT/SFTP Extract file - 
        # NOTE: Use override of S3 EFT Destination folder
        ################################################################
        rootLogger.info("")
        rootLogger.info("Start execution of ProcessFiles2EFT.sh program to SFTP MNUP Monthly Extract File")

        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET, SFTP_FOLDER], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"ProcessFiles2EFT.sh - Failed ({ENVNAME})"
            MSG=f"Python script ProcessFiles2EFT.sh failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script ProcessFiles2EFT.sh completed successfully.")
        

        #############################################################
        # Get SFTP Filename for success email.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Get SFTP filename for email.")
        
        EFT_LOGNAME = f"ProcessFiles2EFT_{TMSTMP}.log"
        sSearchString = "FINAL MF_FILENAME="
        
        lstRecs = findRecsContainingSearchText(LOG_DIR, EFT_LOGNAME, sSearchString)
        # Look at first record. Split string by "=". Get value after the "=" 
        if len(lstRecs) > 0:
            SFTP_FILENAME = lstRecs[0].split("=")[1].strip()
        else: 
            rootLogger.info("Could not find the SFTP Filename")
            SFTP_FILENAME = ""    

        rootLogger.info(f"{SFTP_FILENAME=}")


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
        SUBJECT=f" MNUP_MED_NONUTIL_Monthly_ext._Driver.py - completed ({ENVNAME}{TESTEMAIL})"
        MSG = f"The Medicare Non-Usage (MNUP) Monthly extract file has been created.\n\nAn SFTP version of the below file was created as {SFTP_FILENAME}.\n\nThe following file was created:\n\n{S3Files}"
            
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, MNUP_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)

            sys.exit(12)   


        ################################################################
        # Create Manifest file for SFTP of file
        ################################################################
        rootLogger.info("")
        rootLogger.info("Create Manifest file for SFTP of MNUP Annual Extract.  ")

        # Convert TMSTMP to EFT timestamp for call to CreateManifestFile.sh
        # Ex. 20260624.141251
        YYMMDD = TMSTMP[2:8] 
        HHMMSS = TMSTMP [9:15]
        EFT_TMSTMP = f"R{YYMMDD}.T{HHMMSS}"
        rootLogger.info(f"{EFT_TMSTMP=}")        


        ################################################
        # $1 = bucket/folder where file(s) referenced in manifest file are located 
        # $2 = timestamp of file(s) to include (how to find file in folder)
        # $3 = Manifest file email addresses
        # $4 = where to place manifest file
        # $5 = HLQ of manifest .json filename
        # $6 = the dataRequestID = Destination folder name
        ################################################
        rootLogger.info("")
        rootLogger.info("Start execution of CreateManifestSFTPFile.sh program to SFTP MNUP Extract File")

        try:
            # Place manifest file in MANIFEST_SSA_BUCKET or (commented out) in MANIFEST_HOLD_BUCKET_FLDR
            sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', f"{XTR_BUCKET}/{SFTP_BUCKET_FLDR}", EFT_TMSTMP, MNUP_EMAIL_BOX_RECIPIENT, f"{XTR_BUCKET}/{MANIFEST_SSA_BUCKET_FLDR}", MANIFEST_FILE_HLQ, SFTP_DEST_FLDR ], capture_output=True, text=True, check=True)
            #sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', f"{XTR_BUCKET}/{SFTP_BUCKET_FLDR}", EFT_TMSTMP, MNUP_EMAIL_BOX_RECIPIENT, f"{XTR_BUCKET}/{MANIFEST_HOLD_BUCKET_FLDR}", MANIFEST_FILE_HLQ, SFTP_DEST_FLDR ], capture_output=True, text=True, check=True)

            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CreateManifestSFTPFile.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"Create manifest file failed for MNUP Annual extract - Failed ({ENVNAME})"
            MSG=f"Create manifest file failed for MNUP Annual extract"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script CreateManifestSFTPFile.sh completed successfully.")


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Python program MNUP_MED_NONUTIL_Monthly_ext._Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in MNUP_MED_NONUTIL_Monthly_ext._Driver.py\n {e}")

        rootLogger.error("Exception occured in MNUP_MED_NONUTIL_Monthly_ext._Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in MNUP_MED_NONUTIL_Monthly_ext._Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in MNUP_MED_NONUTIL_Monthly_ext._Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()