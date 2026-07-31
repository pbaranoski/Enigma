#!/usr/bin/env python
########################################################################################################
# Name: PSPS_Extract_Suppress_Driver.py
# DESC:   PSPS Extract for Q6 Suppresion file. 
#
# Execute as python3 PSPS_Extract_Suppress_Driver.py     (processing without override date) 
# Execute as python3 PSPS_Extract_Suppress_Driver.py $1  (processing with override date --> YYYY-MM-DD format) 
#
# 			$1 = From_dt (YYYY-MM-DD)
#
# Viren Khanna   2026-05-04 Create Module.
#
# Paul Baranoski  2026-07-14 Added code to clean-up linux - remove the txt, csv, and gz files as well
#                            as config file.
#                            Added logic to accept override parameter to test without adding hard-code logic.  
########################################################################################################
# IMPORTS
########################################################################################################

import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *

import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import os
import sys
import argparse
import re
import io
import pandas as pd

import subprocess

import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data"

from datetime import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

S3BUCKET = rf"{XTR_BUCKET}/{PSPS_BUCKET_FLDR}"
s3BktFldr =  rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"
EMAIL_MF_FILENAME = f"P#IDR.XTR.PBAR.PSPSQ6.SUPRESS(0)"
mapping_filename = f"PSPS_CSV_File_Mapping.csv"

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
        gz_filename = "PSPSQ6_SUPPRESS_{TMSTMP}.txt.gz"
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}{TESTLOG}PSPS_Extract_Suppress_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPSPS_Extract_Suppress_Driver.py started at {TMSTMP}")

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
        if not (iNOFParms == 0):
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
        # Set S3 Bucket
        ##########################################
      
        rootLogger.info(f" PSPS bucket={PSPS_BUCKET_FLDR}")


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP

        ###########################################
        # Section 1: PSPS_Extract_Suppress_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting PSPS_Extract_Suppress_Driver.py logic ---")


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

        if iNOFParms == 0:
            lstParms = sys.argv

            now = datetime.now()
            CUR_YR = now.year
            MONTH = now.month
            PRIOR_YR = str(int(CUR_YR) - 1)
        else:
            lstParms = sys.argv
            ParmOverrideDate = lstParms[1]
            rootLogger.info(f"Override parameter date {ParmOverrideDate}")

            CUR_YR = ParmOverrideDate[0:4]
            MONTH = ParmOverrideDate[5:7]              
            PRIOR_YR = str(int(CUR_YR) - 1)            
            
        rootLogger.info(PRIOR_YR)
        rootLogger.info(f"{CUR_YR=}") 
        rootLogger.info(f"{MONTH=}") 
       
        mm = f"{MONTH:02d}"

        if mm in ["07", "08", "09"]:
            SERV_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
            SERV_CYQ_END_DT = f"CY{PRIOR_YR}Q4"

            PROC_CYQ_BEG_DT = f"CY{PRIOR_YR}Q1"
            PROC_CYQ_END_DT = f"CY{CUR_YR}Q2"

        else:
            msg_lines = [
        "Extract is processed each July with Q6 data. ",
        "Extract is not scheduled to run for this time period. ",
        "Processing completed."
            ]

            rootLogger.info(msg_lines) 

        ## Send Failure email	
            SUBJECT=f"PSPS Extract did not run.  - Failed ({ENVNAME})"
            MSG=f"Extract is processed each July with Q6 data. Extract is not scheduled to run for this time period."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(4)    
 
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        rootLogger.info(f"PROC_CYQ_BEG_DT = {PROC_CYQ_BEG_DT}") 
        rootLogger.info(f"PROC_CYQ_END_DT = {PROC_CYQ_END_DT}") 
        rootLogger.info(f"SERV_CYQ_BEG_DT = {SERV_CYQ_BEG_DT}") 
        rootLogger.info(f"SERV_CYQ_END_DT = {SERV_CYQ_END_DT}") 
        
        os.environ["TMSTMP"] = TMSTMP
        os.environ["SERV_CYQ_BEG_DT"] = SERV_CYQ_BEG_DT
        os.environ["SERV_CYQ_END_DT"] = SERV_CYQ_END_DT
        os.environ["PROC_CYQ_BEG_DT"] = PROC_CYQ_BEG_DT
        os.environ["PROC_CYQ_END_DT"] = PROC_CYQ_END_DT


        #############################################################
        # Execute Python code to produce extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of PSPS_Extract_Suppress.py program")

        try:
            sp_info = subprocess.run(['python3', 'PSPS_Extract_Suppress.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script PSPS_Extract_Suppress.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"PSPS_Extract_Suppress_Driver.py  - Failed ({ENVNAME})"
            MSG=f"The PSPS Extract Suppress script has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script PSPS_Extract_Suppress.py completed successfully.")

  
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
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")

        # Send Success email	
        SUBJECT=f"PSPS Q6 Suppression Extract  ({ENVNAME}{TESTEMAIL})"
        MSG=f"The Extract for the creation of the PSPS Q6 Suppression file has completed.\n\nA mainframe version of the below file will be created as {EMAIL_MF_FILENAME}.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            sys.exit(12)    

        ####################################################################
        # Start - EFT extract file process
        ####################################################################  

        # S3 Bucket + s3 folder path
        # References to Blue Button should be changed to extract you are working on
        S3BUCKET = rf"{XTR_BUCKET}/{PSPS_BUCKET_FLDR}"
        
        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT PSPS Q6 Suppression Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"PSPS_Extract_Suppress_Driver.py - Failed  ({ENVNAME})"
            MSG= f"PSPS Q6 Suppression Extract EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        #############################################################
        # Download Extract file to Create CSV file from PSPS Q6 Suppression extract file
        #############################################################

        rootLogger.info(f"Copy S3 Extract File to linux data directory")
        #gz_filename = COPY_INTO_FILENAMES
        gz_filename = S3Files.split() [0]

        #downloadFileFromS3(s3_client, XTR_BUCKET, f"{S3BUCKET}archive/{gz_filename}", f"{DATA_DIR}{gz_filename}" ) 
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{PSPS_BUCKET_FLDR}archive/{gz_filename}", f"{DATA_DIR}{gz_filename}" )
        
        #############################################################
        # unzip gz file on linux; get new filename
        #############################################################

        rootLogger.info("Unzip {gz_filename} file on linux" )

        txt_filename = unzipFile(DATA_DIR,gz_filename)

        #############################################################
        # Download Extract mapping config file.
        #############################################################

        CONFIG_BUCKET = rf"{XTR_BUCKET}/{CONFIG_BUCKET_FLDR}"
        #rootLogger.info(f"Download S3 Extract mapping config file {mapping_filename} to linux")
        #gz_filename = COPY_INTO_FILENAMES
        mapping_filename = f"PSPS_CSV_File_Mapping.csv"

        downloadFileFromS3(s3_client, XTR_BUCKET, f"{CONFIG_BUCKET_FLDR}{mapping_filename}", f"{DATA_DIR}{mapping_filename}" ) 
 

        #############################################################
        # Create CSV file from Extract file
        #############################################################
        csv_filename = txt_filename.replace(".txt", ".csv")
        path_mapping = f"{DATA_DIR}{mapping_filename}"
        path_input = f"{DATA_DIR}{txt_filename}"
        path_output = f"{DATA_DIR}{csv_filename}"
       
        # Load config 
        df_cfg = pd.read_csv(path_mapping, header=None, names=['name', 'offset', 'length', 'extra_col'])
        df_cfg.columns = df_cfg.columns.str.strip().str.lower()
        df_cfg = df_cfg.rename(columns={'column-#name': 'name'})
        # --------------------------------------------

        # Build colspecs (start, end)
        # Now row.offset will work because we forced it to lowercase and stripped spaces
        lstColSpecs = [(row.offset, row.offset + row.length) for _, row in df_cfg.iterrows()]

        # Column names
        lstNames = df_cfg["name"].tolist()

        ## Got space issue error when tried to write entire file.
        ## Read fixed-width file
        ##df = pd.read_fwf(path_input, colspecs=lstColSpecs, names=lstNames)

        ## Write to CSV
        ##df.to_csv(path_output, index=False)
        
        # Read fwf in chunks. Returned value "reader" is an iterator object. 
        reader = pd.read_fwf(
            path_input,
            colspecs=lstColSpecs,
            names=lstNames,
            chunksize=100000
        )
        
        # iterative thru the chunks to create csv file
        first = True

        for chunk in reader:
            chunk.to_csv(
                path_output,
                mode="w" if first else "a",
                header=first,
                index=False
            )
            first = False


        rootLogger.info(f"Successfully created: {path_output}")


        #############################################################
        # zip file on linux; get new filename; upload split files from Linux to S3
        #############################################################

        rootLogger.info(f"Upload CSV extract file {csv_filename} to S3" )
            
        s3UploadFile(s3_client, f"{DATA_DIR}{csv_filename}", XTR_BUCKET, f"{DDOM_BUCKET_FLDR}{csv_filename}" )     


        #############################################################
        # Clean-up files on linux server
        #############################################################
        deleteFileFromLinux(f"{DATA_DIR}{csv_filename}")
        deleteFileFromLinux(f"{DATA_DIR}{txt_filename}")
        deleteFileFromLinux(f"{DATA_DIR}{gz_filename}")
        
        deleteFileFromLinux(f"{DATA_DIR}{mapping_filename}")

        
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PSPS_Extract_Suppress_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in PSPS_Extract_Suppress_Driver.py\n {e}")

        rootLogger.error("Exception occured in PSPS_Extract_Suppress_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in PSPS_Extract_Suppress_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in PSPS_Extract_Suppress_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()