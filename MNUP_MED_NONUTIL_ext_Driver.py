#!/usr/bin/bash
############################################################################################################
# Name:  MNUP_MED_NONUTIL_ext_Driver.py
#
# Desc: MNUP Annual Extract of Medical Non-Utilization for SSA
#
# Author     : Paul Baranoski	
# Created    : 11/23/2022
#
# Modified:
#
# Paul Baranoski 2022-11-23 Created script.
#
# Paul Baranoski 2023-08-03 Modify logic in getting extract filenames that include record counts. 
# Paul Baranoski 2023-08-03 Add EFT functionality.  
# Paul Baranoski 2023-09-25 Updated Email message to include EFT filename mask.   
# Paul Baranoski 2023-09-26 Modified email message to include "annual" (to distinguish it from the 
#                           future monthly version of extract).
# Paul Baranoski 2024-01-10 Add $ENVNAME to SUBJECT line of Emails. 
#                           Update call to ProcessFiles2EFT.sh to pass override S3 EFT folder.  
#                           Add call to CreateManifestFiles.sh to create manifest file and place in S3://manifest_files/SSA
# Paul Baranoski 2024-01-16 Change SFTP_FILES constant to SSA_RESP_BUCKET. Added additional documentation. Modified code for call the CreateManifestFile.sh
# Paul Baranoski 2024-03-22 Add call to LOAD_MNUP_FNDR_FILE.sh (So we can obsolete MNUP_Extract_Driver.sh
# Paul Baranoski 2024-07-11 Modified EFT_TMSTMP to pass to CreateConfigFile to be in format R${YYMMDD}.T${HHMMSS}
# Paul Baranoski 2026-06-24 Convert bash to python.
# Paul Baranoski 2026-07-09 Modify verbiage in error email.
# Paul Baranoski 2026-07-30 Modify to use CreateManifestSFTPFileDriver.py instead of CreateManifestSFTPFile.sh. Also, convert from subprocess call to include module.
############################################################################################################


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

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import CreateManifestSFTPFileDriver as CreMan
import LoggerStandard as EnigmaLog
from CommonFunctions import *

DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

S3BUCKET = rf"{XTR_BUCKET}/{MNUP_BUCKET_FLDR}"

PREFIX = "MNUP_FNDR"
MANIFEST_FILE_HLQ = "MNUP_ANNUAL"
SFTP_DEST_FLDR = "SSAMNUP"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}MNUP_MED_NONUTIL_ext_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nMNUP_MED_NONUTIL_ext_Driver.py started at {TMSTMP}")

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
        rootLogger.info("Start execution of LOAD_MNUP_FNDR_FILE_Driver.py program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_MNUP_FNDR_FILE_Driver.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling LOAD_MNUP_FNDR_FILE_Driver.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"LOAD_MNUP_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
            MSG=f"Bash script LOAD_MNUP_FNDR_FILE_Driver.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
        
        
        rootLogger.info("")
        rootLogger.info("bash script LOAD_MNUP_FNDR_FILE_Driver.py completed successfully.")
            

        #################################################################################
        # Create Extract Date parameter
        #################################################################################
        rootLogger.info("") 
        rootLogger.info("Create date parameter for the Python Extract program.")

        # Create date parameter for Prior Year
        CUR_YYYY = date.today().strftime("%Y")
        MNUP_YYYY = str(int(CUR_YYYY) - 1 )
        
        rootLogger.info(f"{CUR_YYYY=}")
        rootLogger.info(f"{MNUP_YYYY=}")

        # Export environment variables for Python extracts code
        os.environ["TMSTMP"] = TMSTMP
        os.environ["MNUP_YR"] = MNUP_YYYY

        
        #############################################################
        # Execute Python code to extract data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of MNUP_MED_NONUTIL_ext.py program")

        try:
            sp_info = subprocess.run(['python3', 'MNUP_MED_NONUTIL_ext.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling MNUP_MED_NONUTIL_ext.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"MNUP_MED_NONUTIL_ext.py - Failed ({ENVNAME})"
            MSG=f"Python script MNUP_MED_NONUTIL_ext.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script MNUP_MED_NONUTIL_ext.py completed successfully.")


        ################################################################
        # Create EFT/SFTP Extract file - 
        # NOTE: Use override of S3 EFT Destination folder
        ################################################################
        rootLogger.info("")
        rootLogger.info("Start execution of ProcessFiles2EFT.sh program to SFTP MNUP Extract File")

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
        SUBJECT=f" MNUP_MED_NONUTIL_ext_Driver.py - completed ({ENVNAME}{TESTEMAIL})"
        MSG = f"The Medicare Non-Usage (MNUP) annual extract file has been created.\n\nAn SFTP version of the below file was created as {SFTP_FILENAME}.\n\nThe following file was created:\n\n{S3Files}"
            
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
            #os.environ["TESTING"] = "Y"
            CreMan.createManifestFile(s3Folder=SFTP_BUCKET_FLDR, sRunTimeStamp=EFT_TMSTMP, sRecipEmails=MNUP_EMAIL_BOX_RECIPIENT, ManifestFldr=MANIFEST_SSA_BUCKET_FLDR, ManifestFileHLQ=MANIFEST_FILE_HLQ, SFTPDestFldr=SFTP_DEST_FLDR ) 

            # Place manifest file in MANIFEST_SSA_BUCKET or (commented out) in MANIFEST_HOLD_BUCKET_FLDR
            #sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', f"{XTR_BUCKET}/{SFTP_BUCKET_FLDR}", EFT_TMSTMP, MNUP_EMAIL_BOX_RECIPIENT, f"{XTR_BUCKET}/{MANIFEST_SSA_BUCKET_FLDR}", MANIFEST_FILE_HLQ, SFTP_DEST_FLDR ], capture_output=True, text=True, check=True)
            ##sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', f"{XTR_BUCKET}/{SFTP_BUCKET_FLDR}", EFT_TMSTMP, MNUP_EMAIL_BOX_RECIPIENT, f"{XTR_BUCKET}/{MANIFEST_HOLD_BUCKET_FLDR}", MANIFEST_FILE_HLQ, SFTP_DEST_FLDR ], capture_output=True, text=True, check=True)

            #write_sp_info_2_log(sp_info) 

        except Exception as e:    
            rootLogger.error(f"Calling createManifestSFTPFile failed with error: {e}") 
            
        #except subprocess.CalledProcessError as e:
        #    rootLogger.error(f"Calling CreateManifestSFTPFile.sh failed with return code {e.returncode}")
        #    rootLogger.error(e.stdout)
        #    rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"Create manifest file failed for MNUP Annual extract - Failed ({ENVNAME})"
            MSG=f"Create manifest file failed for MNUP Annual extract"

            sendEmail(CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG)
            
            #sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            #write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script CreateManifestSFTPFile.sh completed successfully.")


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Python program MNUP_MED_NONUTIL_ext_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in MNUP_MED_NONUTIL_ext_Driver.py\n {e}")

        rootLogger.error("Exception occured in MNUP_MED_NONUTIL_ext_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in MNUP_MED_NONUTIL_ext_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in MNUP_MED_NONUTIL_ext_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()