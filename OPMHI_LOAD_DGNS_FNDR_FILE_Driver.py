#!/usr/bin/env python
########################################################################################################
# Name: OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py
# Description: This script uploads the OPM-HI ICD-10 diagnosis code finder file to 
#              BIA_{ENV}.CMS_TARGET_XTR_{ENV}.OPMHI_ICD10DGS_EXCL
#
#
# Viren Khanna   2026-02-20 Create Module.
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

S3BUCKET = rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"


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


def s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    # Copy object, then delete to "move" file.
    
    s3_client.copy_object(
        Bucket=sSourceBucket,
        CopySource={"Bucket": sSourceBucket, "Key": sSourceKey},
        Key=sDestinationKey
    )

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)


def getConfigFile(s3_client, S3BUCKET, s3ConfigFolder_n_filename):
    
    ##################################################################
    # Retrieve config file from S3 (copy)
    ##################################################################
    rootLogger.info("")
    rootLogger.info(f"Get Config file {s3ConfigFolder_n_filename} from S3")
    
    s3ConfigFile = s3_client.get_object(Bucket=S3BUCKET, Key=s3ConfigFolder_n_filename)

    if  s3ConfigFile == None:
        ## Send Failure email
        SUBJECT = f"OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
        MSG = f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
        
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
        raise Exception(f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed.")        


    ########################################################################
    # S3 Body is byte array. Convert byte array to utf-8 string. 
    # Splitlines recognizes "\r\n" as end-of-record markers     
    ########################################################################
    lstConfigRecs = s3ConfigFile["Body"].read().decode('utf-8').splitlines()
    rootLogger.info("\n%s\n", "\n".join(lstConfigRecs)) 
    
    return lstConfigRecs

    
def validate_dt(sDate2Validate):


    try:

        datetime_obj = datetime.strptime(sDate2Validate, "%Y-%m-%d")
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
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

        LOGNAME = f"{LOG_DIR}{TESTLOG}OPMHI_LOAD_DGNS_FNDR_FILE_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)

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
        # Set S3 Bucket-- 
        ##########################################
      
        S3BUCKET = FINDER_FILE_BUCKET_FLDR
        rootLogger.info(f"OPMHI ICD10DGS finder file Finder filer File bucket={FINDER_FILE_BUCKET_FLDR}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        #################################################################
        # Section 1: OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py logic
        #################################################################    

        rootLogger.info("--- Starting OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py logic ---")
        PREFIX= "OPMHI_ICD10DG"

        #############################################################
        # Set S3 extract bucket/folder to process
        #############################################################
        S3ExtFndrFldrNPrefix = FINDER_FILE_BUCKET_FLDR + PREFIX
        rootLogger.info(f"{S3ExtFndrFldrNPrefix=}")

        
        # Get list of Extract filenames (with folder path) that are ONLY under the requested path. 
        # No "archive" folder filenames. No folder without ext filename: 
        lstKeys = [ obj.key for obj in s3_resource.Bucket(XTR_BUCKET).objects.filter(Prefix=S3ExtFndrFldrNPrefix)]
        rootLogger.info("lstKeys:\n" + "\n".join(lstKeys))

        # There are no finder files in S3 Extract folder 
        if len(lstKeys) == 0:
            rootLogger.info(f"No files to process in {S3ExtFndrFldrNPrefix} ")
        
            ## Send Failure email	
            SUBJECT=f"OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py script - Failed ({ENVNAME})"
            MSG=f"No Finder Files found to process ({ENVNAME})."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(0)    
    
  
        #############################################################
        # Extract filename (Extract the finder file name from the list)
        #############################################################

        OPMHI_ICD10DGS_FF=  lstKeys[0].split("/") [-1] 
        rootLogger.info("Start execution of OPMHI_LOAD_DGNS_FNDR_FILE.py program")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["OPMHI_ICD10DGS_FF"] = OPMHI_ICD10DGS_FF

        #############################################################
        # Execute Python code to Load Finder File
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of OPMHI_LOAD_DGNS_FNDR_FILE.py program")


        try:
            sp_info = subprocess.run(['python3', 'OPMHI_LOAD_DGNS_FNDR_FILE.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 


        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling OPMHI_LOAD_DGNS_FNDR_FILE.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"OPMHI loading ICD-10 Diagnosis codes finder file has failed - Failed ({ENVNAME})"
            MSG=f"OPMHI_LOAD_DGNS_FNDR_FILE.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script OPMHI_LOAD_DGNS_FNDR_FILE.py completed successfully.")

        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email.")

        # Send Success email	
        SUBJECT=f"OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py script - completed ({ENVNAME}{TESTEMAIL})"
        MSG=f"OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py script completed."
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, OPMHI_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    



        #############################################################
        # Move Finder File to archive folder -- Required only for Finder shell script file
        #############################################################

        # Move finder file in S3 to archive folder.
        rootLogger.info(f"Move finder file in S3 to archive folder: s3://{FINDER_FILE_BUCKET_FLDR}archive/{OPMHI_ICD10DGS_FF}")
        
        s3ExtSourceKey = FINDER_FILE_BUCKET_FLDR + OPMHI_ICD10DGS_FF
        s3ExtDestinationKey = FINDER_FILE_BUCKET_FLDR + "archive/" + OPMHI_ICD10DGS_FF
        
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, s3ExtSourceKey, s3ExtDestinationKey)

  
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py\n {e}")

        rootLogger.error("Exception occured in OPMHI_LOAD_DGNS_FNDR_FILE_Driver.py.")
        rootLogger.error("\n%s", str(e))

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()