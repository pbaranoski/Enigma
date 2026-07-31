#!/usr/bin/env python
########################################################################################################
# Name: PSPS_Split_files.py
# DESC:   Split PSPS file into multiple files by HCPCS code (IDR#PBA4/IDR#PBA6). 
#       Q4 or Q6 file must exist on Linux as .txt file. 
#
# Viren Khanna   2026-05-08 Create Module.
# Paul Baranoski 2026-06-17 Modify split logic to call new Common function which replaces awk script functionality.
#                           Modify code to write e.output to log on subprocess.run failure. Replaced code
#                           with writing e.stdout and e.stderr to log. 
#                           Change "Ended At" message to have current timestamp value.
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

        LOGNAME = f"{LOG_DIR}{TESTLOG}PSPS_Split_files_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        # rootLogger.info(f"\nPSPS_Split_files.py started at {TMSTMP}")

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
        # Section 1: PSPS_Split_files.py logic
        ############################################    

        rootLogger.info("--- Starting PSPS_Split_files.py logic ---")
        
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
            
        rootLogger.info(PRIOR_YR)
        rootLogger.info(f"{CUR_YR=}") 
        rootLogger.info(f"{MONTH=}") 
       
        MM = datetime.now().strftime('%m')

        if MM == '01' or MM == '02' or MM == '03':
            QTR = 'Q4'
            PREFIX = 'PSPS_Extract_Q4'
            PSPS_HCPCS_PREFIX = 'PSPS_HCPCS_Q4_PSPS'
        else:
            QTR = 'Q6'
            PREFIX = 'PSPS_Extract_Q6'
            PSPS_HCPCS_PREFIX = 'PSPS_HCPCS_Q6_PSPS'

        rootLogger.info(f"Processing for QTR={QTR}")
        rootLogger.info(f"S3 Filename prefix to search={PREFIX}" ) 
        rootLogger.info(f"Split filename prefix={PSPS_HCPCS_PREFIX}") 

        EMAIL_MF_FILENAME=f"P#IDR.XTR.PBAR.{QTR}.PSPSXX.DYYMMDD.THHMMSST"

        rootLogger.info(f"{EMAIL_MF_FILENAME=}" )

        #################################################################################
        # Remove any residual TRICARE Finder files in data directory.
        #################################################################################

        rootLogger.info("Remove any residual Finder Files in data directory.")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, PREFIX)
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, PSPS_HCPCS_PREFIX)

        ############################################
        # Determine most recent file
        ############################################
        
        rootLogger.info("Find most recent S3 Q4/Q6 file.")
        
        sMostRecentS3Key = s3GetMostRecentFileKeySubProcess(XTR_BUCKET, f"{PSPS_BUCKET_FLDR}archive/", PREFIX)

        rootLogger.info(f"{sMostRecentS3Key=}")

        # Remove the filepath from the key --> filename only
        gz_filename = sMostRecentS3Key.replace(f"{PSPS_BUCKET_FLDR}archive/","")
        
        rootLogger.info(f"{gz_filename=}")

        ############################################
        # Download most recent file from S3 to linux
        ############################################

        rootLogger.info(f"Copy most recent S3 Q4/Q6 file {gz_filename} to linux.")
        
        #gz_filename = filename.strip('"\n ')
        
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{PSPS_BUCKET_FLDR}archive/{gz_filename}", f"{DATA_DIR}{gz_filename}" ) 


        #############################################################
        # unzip gz file on linux; get new filename
        #############################################################

        rootLogger.info(f"Unzip {gz_filename} file on linux" )

        txt_Filename = unzipFile(DATA_DIR,gz_filename)

        ############################################
        # split extract file into 25 files by HCPCS
        ############################################

        rootLogger.info("Split extract file into 25 files by HCPCS code.")

        try:

            splitTextFileIntoMultipleHCPCSFiles(f"{DATA_DIR}{txt_Filename}", f"{DATA_DIR}{PSPS_HCPCS_PREFIX}")

            """
            cmd_awk = [
            f"{RUNDIR}splitByHCPCS.awk", 
            "-v", f"outfile={DATA_DIR}{PSPS_HCPCS_PREFIX}", 
            f"{DATA_DIR}{txt_Filename}"
            ]
    
        
            sp_info = subprocess.run(cmd_awk, capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            """

        except Exception as e:
            rootLogger.error("")
            rootLogger.error("awk script splitByHCPCS.awk failed.")
            rootLogger.error("Spliting PSPS file into separate files by HCPCS failed.")
            
            ## Send Failure email	
            SUBJECT=f"PSPS Split files - Failed ({ENVNAME})"
            MSG=f"The PSPS Split files awk script has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 


            sys.exit(12)


        #################################
        # Get list of split.txt files
        #################################

        rootLogger.info("Get list of .txt split files")
        sFilenamePattern = f"{PSPS_HCPCS_PREFIX}*.txt"
        lstFilesMatchingPattern = ls_using_filename_pattern(DATA_DIR, sFilenamePattern)

        ####################################################
        # Iterate thru files in directory
        ####################################################
        lstFilenamesNRecCounts4Email = []
        lstFilenamesNCounts4Dashboard = []

        for sFilename in lstFilesMatchingPattern:
            iRecCnt = wc_l(f"{DATA_DIR}{sFilename}")
            iByteCnt = getFileByteCount(f"{DATA_DIR}{sFilename}")

            ##########################################################################
            # # wc -l PSPS_HCPCS_Q6_PSPS*.txt | grep -v 'total' | awk '{print $2,$1}' | xargs printf "%s %'14d\n"
            # For Email: Ex. PSPS_HCPCS_Q6_PSPS01_20260511.101229.txt         69,616
            ##########################################################################
            # Combine filename and record count. Format record count to be 14 bytes, right-justified with commas as thousands separator 
            sFilenameNRecCount = f"{sFilename} {iRecCnt:>14,}"
            # Add filename And RecCount to list
            lstFilenamesNRecCounts4Email.append(sFilenameNRecCount) 

            ##########################################################################
            # DASHBOARD_INFO=`wc -lc ${PSPS_HCPCS_PREFIX}*.txt | grep -v 'total' | awk '{print $3,$1,$2}' | xargs printf "DASHBOARD_INFO:%s %s %s \n" `  2>> ${LOGNAME}
            # For Dashboard: Ex. DASHBOARD_INFO:PSPS_HCPCS_Q6_PSPS01_20260511.101229.txt 69616 9050080
            ##########################################################################
            # Combine filename, record count, byte count without formatting 
            sFilenameNCounts4Dashboard = f"DASHBOARD_INFO:{sFilename} {iRecCnt} {iByteCnt}"
            # Add filename And RecCount to list
            lstFilenamesNCounts4Dashboard.append(sFilenameNCounts4Dashboard) 

        #############################################################
        # zip file on linux; get new filename; upload split files from Linux to S3
        #############################################################    
        
        rootLogger.info(f"zip and upload split files." )
        
        for sFilename in lstFilesMatchingPattern:
            gz_SplitFilename = gzipFile(DATA_DIR, os.path.basename(sFilename))
            rootLogger.info(f"{sFilename=} on linux" )
            
            s3UploadFile(s3_client, f"{DATA_DIR}{gz_SplitFilename}", XTR_BUCKET, f"{PSPS_BUCKET_FLDR}{gz_SplitFilename}" )     
        
        ####################################################################
        # Send success email 
        ####################################################################          
        rootLogger.info("")
        rootLogger.info("Send success email with S3 Extract filename.")
        
        filenamesAndCounts = "\n".join(lstFilenamesNRecCounts4Email)
        rootLogger.info("\n%s",f"{filenamesAndCounts}")

        DASHBOARD_INFO = "\n".join(lstFilenamesNCounts4Dashboard)
        rootLogger.info("\n%s",f"{DASHBOARD_INFO}")

        # Send Success email	
        SUBJECT=f"PSPS Split Files Extract for {QTR} ({ENVNAME}{TESTEMAIL})"
        MSG=f"The PSPS Split Files Extract for {QTR} has completed.\n\nA mainframe version of the below file will be created as {EMAIL_MF_FILENAME}.\n\nThe following file(s) were created:\n\n{filenamesAndCounts}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)

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
        rootLogger.info("EFT PSPS HCPCS Split files")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT = f"PSPS_Split_files.py - Failed  ({ENVNAME})"
            MSG= f"PSPS HCPCS Split file EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        ############################################
        # script clean-up
        ############################################
        rootLogger.info("Remove any residual Finder Files in data directory.")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, PREFIX)

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PSPS_Split_files.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in PSPS_Split_files.py\n {e}")

        rootLogger.error("Exception occured in PSPS_Split_files.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in PSPS_Split_files.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in PSPS_Split_files.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()