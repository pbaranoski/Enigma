#!/usr/bin/env python
########################################################################################################
# Name: OPMHI_LOAD_SSN_FNDR_FILE_Driver.py
# Description: This script uploads the OPM-HI SSN finder file to 
#              BIA_{ENV}.CMS_TARGET_XTR_{ENV}.OPMHI_SSN
#
#
# Viren Khanna   2026-02-20 Create Module.
# Paul Baranoski 2026-05-12 Add GSG decryption logic of finder file.
# Paul Baranoski 2026-06-30 Remove call to removeGnupg_home since the Finally in the decrypt try-block issues the same command.
#
########################################################################################################
# IMPORTS
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

import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

# functions for encrypting/decrypting files using gpg
import CommonFunctionsGPG as GPGFunctions

S3BUCKET = rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}OPMHI_LOAD_SSN_FNDR_FILE_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(rootLogger) 

        # Establish logger with CommonFunctionsGPG module.        
        GPGFunctions.setCommonFunctionLogger(rootLogger) 
        
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
        
        ##########################################
        # Set S3 Bucket-- 
        ##########################################
      
        S3BUCKET = FINDER_FILE_BUCKET_FLDR
        rootLogger.info(f"OPMHI SSN finder file Finder filer File bucket={FINDER_FILE_BUCKET_FLDR}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        #################################################################
        # Section 1: OPMHI_LOAD_SSN_FNDR_FILE_Driver.py logic
        #################################################################    

        rootLogger.info("--- Starting OPMHI_LOAD_SSN_FNDR_FILE_Driver.py logic ---")
        PREFIX= "CMS_OPM_SSN_FND"

        #############################################################
        # Is there a finder file to process?
        #############################################################
        S3ExtFndrFldrNPrefix = f"{FINDER_FILE_BUCKET_FLDR}{PREFIX}"
        rootLogger.info(f"{S3ExtFndrFldrNPrefix=}")
        
        lstKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_BUCKET_FLDR, PREFIX)

        # There are no finder files in S3 Extract folder 
        if len(lstKeys) == 0:
            rootLogger.info(f"No finder files to process in {S3ExtFndrFldrNPrefix} ")
        
            ## Send Failure email	
            SUBJECT=f"OPMHI_LOAD_SSN_FNDR_FILE_Driver.py script - Failed ({ENVNAME})"
            MSG=f"No Finder Files found to process ({ENVNAME})."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(0)    
    
  
        #############################################################
        # Extract filename (Extract the finder file name from the list)
        #############################################################
        OPMHI_SSN_FF_GPG =  lstKeys[0].split("/") [-1]
        rootLogger.info(f"{OPMHI_SSN_FF_GPG=}")
        
        OPMHI_SSN_FF = OPMHI_SSN_FF_GPG.replace(".gpg","")
        #OPMHI_SSN_FF = OPMHI_SSN_FF.replace(".PGP","")

        encrypted_file = f"{DATA_DIR}{OPMHI_SSN_FF_GPG}"
        decrypted_file = f"{DATA_DIR}{OPMHI_SSN_FF}"
        
        rootLogger.info(f"{encrypted_file=}")
        rootLogger.info(f"{decrypted_file=}")
        
        #############################################################
        # Download extract file from S3
        #############################################################
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{OPMHI_SSN_FF_GPG}", f"{DATA_DIR}{OPMHI_SSN_FF_GPG}" )

        #############################################################
        # GPG decrypt file
        #############################################################
        rootLogger.info(f"Get Secret Key: {OPMHI_DECRYPT_KEY_SECRET_NAME}. ")
        DecryptKey = GPGFunctions.get_secret(OPMHI_DECRYPT_KEY_SECRET_NAME, REGION)
        
        # To allow us to konw if key is a Private or public key
        sKeyHeader = DecryptKey.split("\\n")[0:1]
        rootLogger.info(f"{sKeyHeader=}")
        
        rootLogger.info("Import gpg Key. ")
        gnupg_home = GPGFunctions.import_gpg_key(DecryptKey)

        # List keys and list-packets for debugging
        #GPGFunctions.list_keys(gnupg_home)

        #GPGFunctions.list_packets(gnupg_home, encrypted_file)
       
        rootLogger.info("Decrypt finder file ")
        GPGFunctions.decrypt_file(gnupg_home, OPMHI_DECRYPT_PASSPHRASE, encrypted_file, decrypted_file)


        #############################################################
        # Upload decrypted finder file to S3
        #############################################################
        s3UploadFile(s3_client, f"{DATA_DIR}{OPMHI_SSN_FF}", XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{OPMHI_SSN_FF}")

        #############################################################
        # Move extract gpg file to archive folder.
        #############################################################
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{OPMHI_SSN_FF_GPG}", f"{FINDER_FILE_BUCKET_FLDR}archive/{OPMHI_SSN_FF_GPG}")


        #############################################################
        # Load FF into SF table.
        #############################################################
        rootLogger.info("Start execution of OPMHI_LOAD_SSN_FNDR_FILE.py program")

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["OPMHI_SSN_FF"] = OPMHI_SSN_FF

        #############################################################
        # Execute Python code to Load Finder File
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of OPMHI_LOAD_SSN_FNDR_FILE.py program")

        try:
            sp_info = subprocess.run(['python3', 'OPMHI_LOAD_SSN_FNDR_FILE.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 


        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling OPMHI_LOAD_SSN_FNDR_FILE.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"OPMHI loading SSN finder file has failed ({ENVNAME})"
            MSG=f"OPMHI_LOAD_SSN_FNDR_FILE.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script OPMHI_LOAD_SSN_FNDR_FILE.py completed successfully.")

        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email.")

        # Send Success email	
        SUBJECT=f"OPMHI_LOAD_SSN_FNDR_FILE_Driver.py script - completed ({ENVNAME}{TESTEMAIL})"
        MSG=f"OPMHI_LOAD_SSN_FNDR_FILE_Driver.py script completed."
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, OPMHI_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    


        #############################################################
        # Move extract gpg file to archive folder.
        #############################################################
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{OPMHI_SSN_FF}", f"{FINDER_FILE_BUCKET_FLDR}archive/{OPMHI_SSN_FF}")


        #############################################################
        # Clean-up files on linux server
        #############################################################
        deleteFileFromLinux(decrypted_file)
        deleteFileFromLinux(encrypted_file)

        
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script OPMHI_LOAD_SSN_FNDR_FILE_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in OPMHI_LOAD_SSN_FNDR_FILE_Driver.py\n {e}")

        rootLogger.error("Exception occured in OPMHI_LOAD_SSN_FNDR_FILE_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in OPMHI_LOAD_SSN_FNDR_FILE_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in OPMHI_LOAD_SSN_FNDR_FILE_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()