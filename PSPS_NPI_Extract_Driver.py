#!/usr/bin/env python
########################################################################################################
# Name: PSPS_NPI_Extract_Driver.py
# Desc: Extract PBAR PSPS NPI data 
#       1) Script will run to create main extract file and split that file into the 25 HCPCS category files.
#       2) Main extract file will be place in archive folder.
#       3) After script completes, notify Sean Whitelock to run his script to create SAS file versions
#          of the 25 HCPCS category files. His program will move the non-SAS files to the archive directory.
#       4) After Sean Whitelock's script creates the 25 SAS files, run the ProcessFiles2EFT.sh script
#          to EFT the SAS files to the MF.
#       5) Verify that the SAS files land on the MF successfully.
#
# Viren Khanna   2026-05-24 Create Module.
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

S3BUCKET = rf"{XTR_BUCKET}/{PSPSNPI_BUCKET_FLDR}"
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

        LOGNAME = f"{LOG_DIR}{TESTLOG}PSPS_NPI_Extract_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPSPS_NPI_Extract_Driver.py started at {TMSTMP}")

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
      
        rootLogger.info(f" PSPS bucket={PSPSNPI_BUCKET_FLDR}")

        ###########################################
        # Section 1: PSPS_NPI_Extract_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting PSPS_NPI_Extract_Driver.py logic ---")
        
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
        
            ext_YYYY = datetime.now().strftime("%Y") 
            
        rootLogger.info(ext_YYYY)
        rootLogger.info(f"{CUR_YR=}") 
        rootLogger.info(f"{MONTH=}") 
       
        MM = datetime.now().strftime('%m')

        if MM < '07':
            ext_mon = "JAN"
        else:
            ext_mon = "JUL"

        rootLogger.info(f"Extract Month ={ext_mon}")

        #############################################################
        # Set filename variables for use by script
        #############################################################

        rootLogger.info(f"Set extract filename variables for use by script")

        PSPSNPI_S3FILE=f"PBAR_PSPSNPI_{ext_YYYY}_{ext_mon}_{TMSTMP}.txt.gz"
        UNZIPPED_PSPSNPI_S3FILE = PSPSNPI_S3FILE.replace(".gz", "")
        PBAR_HCPCS_SPLIT_FILE_MASK =f"PBAR_PSPS_NPI_{ext_YYYY}_{ext_mon}_P"
        PBAR_PSPS_NPI_ALL_FILES_MASK="PBAR_PSPS"

        rootLogger.info(f"PSPSNPI_S3FILE ={PSPSNPI_S3FILE}")
        rootLogger.info(f"UNZIPPED_PSPSNPI_S3FILE ={UNZIPPED_PSPSNPI_S3FILE}")
        rootLogger.info(f"PBAR_HCPCS_SPLIT_FILE_MASK ={PBAR_HCPCS_SPLIT_FILE_MASK}")
        rootLogger.info(f"PBAR_PSPS_NPI_ALL_FILES_MASK ={PBAR_PSPS_NPI_ALL_FILES_MASK}")



        #################################################################################
        # Remove any residual TRICARE Finder files in data directory.
        #################################################################################

        rootLogger.info("Remove any residual Finder Files in data directory.")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, PBAR_PSPS_NPI_ALL_FILES_MASK)


        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        os.environ["TMSTMP"] = TMSTMP
        os.environ["ext_YYYY"] = ext_YYYY
        os.environ["ext_mon"] = ext_mon


        #############################################################
        # Execute Python code to produce extract
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of PSPS_NPI_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'PSPS_NPI_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling Python script PSPS_NPI_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"PSPS NPI Extract - Failed ({ENVNAME})"
            MSG=f"The PSPS NPI extract has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script PSPS_NPI_Extract.py completed successfully.")


        ###########################################################################################
        # Call combineS3Files.sh to combine all file parts
        ###########################################################################################
        rootLogger.info("")
        rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 
        PSPSNPI_BUCKET = f"{XTR_BUCKET}/{PSPSNPI_BUCKET_FLDR}"
        rootLogger.info(f"{PSPSNPI_BUCKET_FLDR=} ")

        sConcatFilename = PSPSNPI_S3FILE
        rootLogger.info(f"{sConcatFilename=}")

        try:
            sp_info = subprocess.run(['bash', 'CombineS3Files.sh', PSPSNPI_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"PSPS NPI S3 files concatenation FAILED({ENVNAME})"
            MSG=f"PSPS NPI Extract has failed in PSPS NPI S3 files concatenation step."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        ############################################
        # Download most recent file from S3 to linux
        ############################################
        gz_filename = PSPSNPI_S3FILE
        rootLogger.info(f"Copy most recent S3 PSPS NPI file {gz_filename} to linux.")
        
        #gz_filename = filename.strip('"\n ')
        
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{PSPSNPI_BUCKET_FLDR}{gz_filename}", f"{DATA_DIR}{gz_filename}" ) 
    
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
        
            splitTextFileIntoMultipleHCPCSFiles(f"{DATA_DIR}{txt_Filename}", f"{DATA_DIR}{PBAR_HCPCS_SPLIT_FILE_MASK}")
            
            """
            cmd_awk = [
            f"{RUNDIR}splitByHCPCS.awk", 
            "-v", f"outfile={DATA_DIR}{PBAR_HCPCS_SPLIT_FILE_MASK}", 
            f"{DATA_DIR}{txt_Filename}"
            ]
        
            sp_info = subprocess.run(cmd_awk, capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            """

        except Exception as e:
            rootLogger.error("")
            rootLogger.error("awk script splitByHCPCS.awk failed.")
            rootLogger.error("Spliting PSPS NPI file into separate files by HCPCS failed.")
            
            ## Send Failure email	
            SUBJECT=f"PSPS NPI Split files - Failed ({ENVNAME})"
            MSG=f"The PSPS NPI Split files awk script has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 


            sys.exit(12)

        #################################
        # Get list of split.txt files
        #################################

        rootLogger.info("Get list of .txt split files")
        sFilenamePattern = f"{PBAR_PSPS_NPI_ALL_FILES_MASK}*.txt"
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
        # Copy split .gz files from linux to s3 archive folder
        #############################################################    
        
        rootLogger.info(f"zip and upload split files." )
        
        sFilenamezip = f"{PBAR_HCPCS_SPLIT_FILE_MASK}*.txt"
        lstFilesMatchingPattern = ls_using_filename_pattern(DATA_DIR, sFilenamezip)

        for sFilename in lstFilesMatchingPattern:
            gz_SplitFilename = gzipFile(DATA_DIR, os.path.basename(sFilename))
            rootLogger.info(f"{sFilename=} on linux" )
            
            s3UploadFile(s3_client, f"{DATA_DIR}{gz_SplitFilename}", XTR_BUCKET, f"{PSPSNPI_BUCKET_FLDR}archive/{gz_SplitFilename}" )

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
        SUBJECT=f"PSPS NPI {ext_mon} extract ({ENVNAME}{TESTEMAIL})"
        MSG=f"The Extract for the creation of the PSPS NPI {ext_mon} file from Snowflake has completed.\n\nThe following file(s) were created:\n\n{filenamesAndCounts}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PSPSNPI_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)

            sys.exit(12)    

        ############################################
        # script clean-up
        ############################################
        rootLogger.info("Remove any residual Finder Files in data directory.")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, PBAR_PSPS_NPI_ALL_FILES_MASK)

        ####################################################################
        # Start - EFT extract file process
        ####################################################################  

        # S3 Bucket + s3 folder path
        # References to Blue Button should be changed to extract you are working on
        S3BUCKET = rf"{XTR_BUCKET}/{PSPSNPI_BUCKET_FLDR}"

        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT PSPS NPI Extract File")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT = f"PSPS NPI Extract EFT process  - Failed  ({ENVNAME})"
            MSG= f"PSPS NPI Extract EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PSPS_NPI_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in PSPS_NPI_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in PSPS_NPI_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in PSPS_NPI_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in PSPS_NPI_Extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()