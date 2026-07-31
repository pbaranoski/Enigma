#!/usr/bin/env python
########################################################################################################
# Name:  OPMHI_PDE_Driver.py
#
# Description: This script executes the Part A SQL python script for OPM-HI HHA 
# 
# Viren Khanna   2026-02-12 Create Module.
# Paul Baranoski 2026-05-12 Add GSG encryption logic of extract file.
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
from CommonFunctions import *

# functions for encrypting/decrypting files using gpg
import CommonFunctionsGPG as GPGFunctions

OPMHI_PDE_BUCKET = rf"{XTR_BUCKET}/{OPMHI_PDE_BUCKET_FLDR}"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}OPMHI_PDE_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nOPMHI_PDE_Driver.py started at {TMSTMP}")

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


        #############################################################
        # Remove any residual files from a prior run from Data Directory
        #############################################################
        rootLogger.info(f" Removing any existing file from DATA_DIR")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, "FEHB_CMS_PTD")
        
        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################
        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")

        if iNOFParms == 0:
            lstParms = sys.argv

            now = datetime.now()
            CURR_YR = now.year
            CURR_MONTH = now.month
            CUR_DT = now.strftime("%Y-%m-%d")   
            CAL_QTR = ((CURR_MONTH - 1) // 3) + 1

        rootLogger.info(f"{CAL_QTR=}")

        if CAL_QTR == 1:
            EXT_YEAR = CURR_YR - 1
            START_DATE = f"{EXT_YEAR}-10-01"
            END_DATE = f"{EXT_YEAR}-12-31"
            EXT_QTR = "4"
        elif CAL_QTR == 2:
            EXT_YEAR = CURR_YR
            START_DATE = f"{CURR_YR}-01-01"
            END_DATE = f"{CURR_YR}-03-31"
            EXT_QTR = "1"
        elif CAL_QTR == 3:
            EXT_YEAR = CURR_YR
            START_DATE = f"{CURR_YR}-04-01"
            END_DATE = f"{CURR_YR}-06-30"
            EXT_QTR = "2"
        elif CAL_QTR == 4:
            EXT_YEAR = CURR_YR
            START_DATE = f"{CURR_YR}-07-01"
            END_DATE = f"{CURR_YR}-09-30"
            EXT_QTR = "3"

        rootLogger.info(f"{CURR_YR=}") 
        rootLogger.info(f"{CURR_MONTH=}")  
        rootLogger.info(f"{CUR_DT=}")  
        rootLogger.info(f"{CAL_QTR=}") 
        rootLogger.info(f"{START_DATE=}") 
        rootLogger.info(f"{END_DATE=}") 
        rootLogger.info(f"{EXT_YEAR=}") 
        rootLogger.info(f"{EXT_QTR=}") 
        

        ###########################################################
        #  Set and export other parameters for the SQL
        ###########################################################
        
        # Passing Parameters to Python script
        CTYP="PTD"
        STAGE_NAME="OPMHIPDE"
        START_DT = START_DATE.replace("-", "")
        END_DT = END_DATE.replace("-", "")
        CUR_DATE = CUR_DT.replace("-", "")

        rootLogger.info(f"{CTYP=}")
        rootLogger.info(f"{STAGE_NAME=}")
        rootLogger.info(f"{START_DT=}")
        rootLogger.info(f"{END_DT=}")
        rootLogger.info(f"{CUR_DATE=}")

        # BUILD EXTRACT FILENAME
        rootLogger.info(f" Build Extract Filename")
        EXT_FILENAME = f"FEHB_CMS_{CTYP}_{START_DT}_{END_DT}_{CUR_DATE}.txt.gz"

        rootLogger.info(f"{EXT_FILENAME=}")


        #############################################################
        # Remove extract file in s3 if it exists. 
        # NOTE: this is because customer did not want file with timestamp, but instead date.
        #############################################################
        rootLogger.info(f"Remove s3 file {OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME} if it already exists in s3.")
        s3_client.delete_object(Bucket=XTR_BUCKET, Key=f"{OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME}")
        rootLogger.info(f"Remove s3 file {OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME}.gpg if it already exists in s3.")
        s3_client.delete_object(Bucket=XTR_BUCKET, Key=f"{OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME}.gpg")
        
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["EXT_FILENAME"] = EXT_FILENAME
        os.environ["STAGE_NAME"] = STAGE_NAME
        os.environ["CURR_YR"] = str(CURR_YR)
        os.environ["CURR_MONTH"] = str(CURR_MONTH)
        os.environ["CUR_DT"] = CUR_DT
        os.environ["CAL_QTR"] = str(CAL_QTR)
        os.environ["START_DATE"] = START_DATE
        os.environ["END_DATE"] = END_DATE
        os.environ["EXT_YEAR"] = str(EXT_YEAR)
        os.environ["EXT_QTR"] = str(EXT_QTR)

        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of OPMHI_PDE_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'OPMHI_PDE_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling OPMHI_PDE_Extract.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"OPMHI_PDE_Extract- Failed ({ENVNAME})"
            MSG=f"Python script OPMHI_PDE_Extract failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script OPMHI_PDE_Extract.py completed successfully.")

        ###########################################################################################
        # Call combineS3Files.sh to combine all file parts
        ###########################################################################################
        rootLogger.info("")
        rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 

        rootLogger.info(f"{OPMHI_PDE_BUCKET=} ")

        sConcatFilename = f"FEHB_CMS_{CTYP}_{START_DT}_{END_DT}_{CUR_DATE}.txt.gz"
        rootLogger.info(f"{sConcatFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', OPMHI_PDE_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Combining S3 files in OPMHI_PDE_Driver.py - Failed ({ENVNAME})"
            MSG=f"Combining S3 files in OPMHI_PDE_Driver.py has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

  
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
        SUBJECT=f" OPMHI_PDE_Driver- completed ({ENVNAME}{TESTEMAIL})"
        MSG=f" OPMHI_PDE_Driver completed. \n\nThe following extract files were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, OPMHI_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    


        #############################################################
        # Download extract file from S3
        #############################################################
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME}", f"{DATA_DIR}{EXT_FILENAME}" )

        #############################################################
        # GPG encrypt file
        #############################################################
        rootLogger.info("Get Secret Key. ")
        EncryptKey = GPGFunctions.get_secret(OPMHI_CLAIMS_ENCRYPT_KEY_SECRET_NAME, REGION)

        rootLogger.info("Import gpg Key. ")
        gnupg_home = GPGFunctions.import_gpg_key(EncryptKey)
        
        rootLogger.info("get recipient. ")
        recipient = GPGFunctions.get_key_fingerprint(gnupg_home)
        rootLogger.debug(f"{recipient=}")

        ext_fileNPath = f"{DATA_DIR}{EXT_FILENAME}"
        gpg_fileNPath = f"{DATA_DIR}{EXT_FILENAME}.gpg"

        rootLogger.info("encrypt file ")
        GPGFunctions.encrypt_file(gnupg_home, ext_fileNPath, gpg_fileNPath, recipient)

        #############################################################
        # Upload encrypted extract file to S3
        #############################################################
        s3UploadFile(s3_client, f"{DATA_DIR}{EXT_FILENAME}.gpg", XTR_BUCKET, f"{OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME}.gpg")

        #############################################################
        # Move extract .gz file to archive folder so that it is not
        # included in the manifest file.
        #############################################################
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{OPMHI_PDE_BUCKET_FLDR}{EXT_FILENAME}", f"{OPMHI_PDE_BUCKET_FLDR}archive/{EXT_FILENAME}")

        #############################################################
        # Clean-up files on linux server
        #############################################################
        deleteFileFromLinux(ext_fileNPath)
        deleteFileFromLinux(gpg_fileNPath)
        
        #############################################################
        # SFTP file - 1) no EFT unzip and create text file.
        #             2) Send file as-is binary   
        #############################################################

        #############################################################
        # SFTP file - 1) no EFT unzip and create text file.
        #             2) Send file as-is binary   
        #############################################################

        ################################################
        # $1 = bucket/folder where file(s) referenced in manifest files are located 
        # $2 = timestamp of file(s) to include (how to find file in folder)
        # $3 = Manifest file email addresses
        # $4 = where to place manifest file (TESTING - hold folder)
        # $5 = HLQ of manifest .json filename
        # $6 = the dataRequestID = Destination folder name
        ################################################
        if os.environ["TESTING"] == "Y":
            MANIFEST_BUCKET_TO_USE = f"{XTR_BUCKET}/{MANIFEST_HOLD_BUCKET_FLDR}"
        else:
            MANIFEST_BUCKET_TO_USE = f"{XTR_BUCKET}/{MANIFEST_OPMHI_CLAIMS_BUCKET_FLDR}"

        rootLogger.info(f"Create Manifest file")        
        rootLogger.info(f"{MANIFEST_BUCKET_TO_USE=}")   
        
        MANIFEST_FILE_HLQ = "OPMHI_PDE"
        # Ex. CLAIMS_YYYYMM
        DEST_FLDR = f"CLAIMS_{TMSTMP[:6]}"

        try:
            sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', OPMHI_PDE_BUCKET, CUR_DATE, OPMHI_BOX_RECIPIENT, MANIFEST_BUCKET_TO_USE, MANIFEST_FILE_HLQ, DEST_FLDR ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.stdout)
            rootLogger.error("\n%s", e.stderr)

            SUBJECT=f"Create Manifest file in OPMHI_PDE_Driver.py - Failed ({ENVNAME})"
            MSG=f"Create Manifest file in OPMHI_PDE_Driver.py  has failed."
            
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            # re-raise exception
            raise


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script OPMHI_PDE_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in OPMHI_PDE_Driver.py\n {e}")

        rootLogger.error("Exception occured in OPMHI_PDE_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in OPMHI_PDE_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in OPMHI_PDE_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()