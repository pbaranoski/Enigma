#!/usr/bin/env python
########################################################################################################
# Name:   NYSPAP_Extract_Driver.py
#
# Desc: NYSPAP Extract Driver script: Loads LOAD_NYSPAP_FNDR_FILE.py and NYSPAP_Extract_Bene_Info.py.
#
# Created: Viren Khanna
# Modified: 11/28/2025
#
# Paul Baranoski 2026-02-03 Modify to Add "TESTING" functionality. Change log filename to not use
#                           the "driver" log filename as that name is excluded from Dashboard script.
# Paul Baranoski 2026-02-12 Turn off TESTING since this module has been approved to migrate to production.
########################################################################################################

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

from datetime import datetime
from datetime import date, timedelta

import os
import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog


NYSPAP_BUCKET= rf"{XTR_BUCKET}/{NYSPAP_BUCKET_FLDR}"


LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"
DATADIR = "/app/IDRC/XTR/CMS/data/"




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
        SUBJECT=f"NYSPAP_Driver.py - Failed ({ENVNAME})"
        MSG=f"Parameter date {sDate2Validate} is either an invalid date or not formatted correctly. Date must be in YYYY-MM-DD format. Process failed. "
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 
        
        sys.exit(12)


def s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    # Copy object, then delete to "move" file.
    
    s3_client.copy_object(
        Bucket=sSourceBucket,
        CopySource={"Bucket": sSourceBucket, "Key": sSourceKey},
        Key=sDestinationKey
    )

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}NYSPAP_Extract_Bene_Info_{TMSTMP}.log" 

    
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        #global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nNYSPAP_Extract_Driver.py started at {TMSTMP}")

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
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
 

        ##########################################
        # Set S3 Bucket-- Check with Paul
        ##########################################
      
        S3BUCKET = NYSPAP_BUCKET
        rootLogger.info(f"NYSPAP bucket={S3BUCKET}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        ###########################################
        # Section 1: LOAD_NYSPAP_FNDR_FILE.py logic
        ############################################    

    
        rootLogger.info("--- Starting LOAD_NYSPAP_FNDR_FILE.py logic ---")
        PREFIX = "NYSPAP_FNDR"
        
        #############################################################
        # Set S3 extract bucket/folder to process
        #############################################################
        S3ExtFndrFldrNPrefix = FINDER_FILE_BUCKET_FLDR + PREFIX
        rootLogger.info(f"{S3ExtFndrFldrNPrefix=}")

        
        # Get list of Extract filenames (with folder path) that area ONLY under the requested path. No "archive" folder filenames. No folder without ext filename: "xtr/PSPS/"
        lstKeys = [ obj.key for obj in s3_resource.Bucket(XTR_BUCKET).objects.filter(Prefix=S3ExtFndrFldrNPrefix)]
        rootLogger.info("lstKeys:\n" + "\n".join(lstKeys))

        # There are no finder files in S3 Extract folder 
        if len(lstKeys) == 0:
            rootLogger.info(f"No files to process in {S3ExtFndrFldrNPrefix} ")
        
            ## Send Failure email	
            SUBJECT=f"LOAD_NYSPAP_FNDR_FILE - Failed ({ENVNAME})"
            MSG=f"No Finder Files found to process ({ENVNAME})."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(0)    
    
  
        #############################################################
        # Extract filename (Extract the finder file name from the list)
        #############################################################
        LOAD_NYSPAP_FINDER_FILE =  lstKeys[0].split("/") [-1] 


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["LOAD_NYSPAP_FINDER_FILE"] = LOAD_NYSPAP_FINDER_FILE

        #############################################################
        # Execute Python code to Load Finder File
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_NYSPAP_FNDR_FILE.py program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_NYSPAP_FNDR_FILE.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 


        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling LOAD_NYSPAP_FNDR_FILE.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"LOAD_NYSPAP_FNDR_FILE - Failed ({ENVNAME})"
            MSG=f"LOAD_NYSPAP_FNDR_FILE.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script LOAD_NYSPAP_FNDR_FILE.py completed successfully.")


      
        ##############################################################
        # Section 2: NYSPAP_Extract_Bene_Info.py logic
        ##############################################################

        rootLogger.info("--- Starting NYSPAP_Extract_Bene_Info.py logic ---")

        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################

        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")


        today = date.today()
        first_of_this_month = today.replace(day=1)
        first_of_last_month = first_of_this_month - timedelta(days=1)
        BENE_RNG_DT = first_of_last_month.replace(day=1).strftime('%Y-%m-%d')

        ###########################################################
        # Display extract dates to use.
        ###########################################################

        
        rootLogger.info(f"BENE_RNG_DT={BENE_RNG_DT}")

        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["BENE_RNG_DT"] = BENE_RNG_DT

        #############################################################
        # Execute Python code to Extract data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of NYSPAP_Extract_Bene_Info.py program")

        try:
            sp_info = subprocess.run(['python3', 'NYSPAP_Extract_Bene_Info.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

        
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling NYSPAP_Extract_Bene_Info.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"NYSPAP Extract - Failed ({ENVNAME})"
            MSG=f"Python script NYSPAP_Extract_Driver.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    

        rootLogger.info("")
        rootLogger.info("Python script NYSPAP_Extract_Bene_Info.py completed successfully.")

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

        # Send Success email.
       
        SUBJECT = f"Monthly NYSPAP extract ({ENVNAME}{TESTEMAIL})"
        MSG = f"The Extract for the creation of the monthly NYSPAP file has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, NYSPAP_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    


        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Create Manifest file for NYSPAP script. ")

        #####################################################
        # bucket/s3folder --> points to location of extract file.
        #                 --> S3 folder is key token to config file to determine if manifest file is in HOLD status
        #          TMSTMP --> uniquely identifies extract file(s)
        #       BoxEmails --> manifest file recipients
        #####################################################
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=NYSPAP_BUCKET_FLDR, runToken=TMSTMP, BoxEmails=NYSPAP_BOX_RECIPIENT )

        except Exception as e:

            SUBJECT=f"Create Manifest file in NYSPAP_Driver.py - Failed ({ENVNAME})"
            MSG=f"Create Manifest file in NYSPAP_Driver.py has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            # re-raise exception
            raise


        #############################################################
        # Move Finder File to archive folder
        #############################################################

        # Move finder file in S3 to archive folder.
        rootLogger.info(f"Move finder file in S3 to archive folder: s3://{FINDER_FILE_BUCKET_FLDR}archive/{LOAD_NYSPAP_FINDER_FILE}")
        
        s3ExtSourceKey = FINDER_FILE_BUCKET_FLDR + LOAD_NYSPAP_FINDER_FILE
        s3ExtDestinationKey = FINDER_FILE_BUCKET_FLDR + "archive/" + LOAD_NYSPAP_FINDER_FILE
        
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, s3ExtSourceKey, s3ExtDestinationKey)


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################     -
        rootLogger.info("")
        rootLogger.info("NYSPAP_Extract_Driver.sh completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )
        sys.exit(0)

  except Exception as e:
        print (f"Exception occured in NYSPAP_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in NYSPAP_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        sys.exit(12)    


if __name__ == "__main__":

        main_processing_loop()
