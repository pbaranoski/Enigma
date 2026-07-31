#!/usr/bin/bash
############################################################################################################
# Name:  DUALS_MedAdv_Ext.py
#
# Desc: Duals Quarterly PTA/PTB Medicare Advantage extract 
#
# NOTE: Extract will run for a year's worth of data with a 3-month lag. 
#  Ex1: Run June 30, 2025 for Jan 1-Mar 31 2025
#  Ex2: Run September 30, 2025 for April 1 - June 30, 2025
#
# NOTE2: Historical runs will use override dates entered in RunDeck
# 
# Execute as ./DUALS_MedAdv_Ext.sh $1 $2 (Where $1 and $2 are both optional)
#
# $1 = optional override EXT FROM DATE (YYYY-MM-DD format)   
# $2 = optional override EXT THRU DATE (YYYY-MM-DD format)
#
#
# ST_EXT_FNAME_MODEL=DUALS_MedAdv_MD_AH_202410_202412_${TMSTMP}.txt
#
# ST_EXT_FNAME_MODEL=DUALS_MedAdv_XX_{EXT_TYPE}_${YYYYMM_FROM}_${YYYYMM_THRU}_${TMSTMP}.txt
# S3_EXTRACT_FILE=DUALS_MedAdv_XX_${YYYYMM_FROM}_${YYYYMM_THRU}_${TMSTMP}.txt.gz
#
#
# Author     : Paul Baranoski	
# Created    : 01/23/2025
#
# Modified:
#
# Paul Baranoski 2025-01-23 Created script.
# Paul Baranoski 2026-05-29 Modified EFT mask to display in success emails.
# Paul Baranoski 2026-06-04 Convert to python. Add split logic for BH files.
# Paul Baranoski 2026-06-09 Add send email logic in ending Exception block.
# Paul Baranoski 2026-07-15 Re-work timing of logic to free space on linux as soon as its not needed. Space
#                           requirements/needs are "tight".
############################################################################################################

import os
os.environ["TESTING"] = "Y"

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

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

# State Parameter file contains states that want this extract.
ST_PARMFILE = "DUALS_MedAdv_StParms.txt"
DUALS_MEDADV_FILE_PREFIX = "DUALS_MedAdv"
EFT_FILEMASK = "P#EFT.ON.GST.DUAL.AH.PYYQQ.TIMESTAMP"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}DUALS_MedAdv_Ext_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDUALS_MedAdv_Ext.py started at {TMSTMP}")

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
        if not (iNOFParms == 0 or iNOFParms ==  2):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")


        #################################################################################
        # Get any override parameters
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Get any override parameters")
        
        P_EXT_FROM_DT = ""
        P_EXT_THRU_DT = ""

        if iNOFParms == 2:
            lstParms = sys.argv

            #############################################################
            # Get override extract dates if passed 
            #############################################################
            P_EXT_FROM_DT = lstParms[1]
            P_EXT_THRU_DT = lstParms[2]

            rootLogger.info("Parameters to script: ")
            rootLogger.info(f"{P_EXT_FROM_DT=} ")
            rootLogger.info(f"{P_EXT_THRU_DT=} ")
            

        #################################################################################
        # Get current and Prior year values needed to calculate date ranges.
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Get current and prior year values needed to calculate date ranges.")
        
        dttmToday = (datetime.today())
        
        CUR_YR = dttmToday.strftime('%Y')
        PRIOR_YR = (dttmToday + relativedelta(years=-1)).strftime('%Y')

        rootLogger.info(f"{CUR_YR=}")
        rootLogger.info(f"{PRIOR_YR=}")

        CUR_YY = CUR_YR[2:4]
        PRIOR_YY = PRIOR_YR [2:4]

        rootLogger.info(f"{CUR_YY=}")
        rootLogger.info(f"{PRIOR_YY=}")
        
        MM = dttmToday.strftime('%m')
        rootLogger.info(f"{MM=}")


        #################################################################################
        # Create extract parameter date ranges.
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Create Extract From and Thru date parameters for the Python Extract programs.")
    
        #################################
        # Calculate Extract date range
        #################################
        if P_EXT_FROM_DT == "":

            rootLogger.info("Calculating Extract dates")
	
            ######################################################
            # Determine Ext date range
            # Example: Run July 1, 2025 for Jan 1-Mar 31 2025
            ######################################################
            if MM in ("07","08","09"):
                CLM_FROM_DT = f"{CUR_YR}-01-01"
                CLM_THRU_DT = f"{CUR_YR}-03-31"
                CLM_PRD = f"P{CUR_YY}Q1"

            elif MM in ("10","11","12"):
                CLM_FROM_DT = f"{CUR_YR}-04-01"
                CLM_THRU_DT = f"{CUR_YR}-06-30"
                CLM_PRD = f"P{CUR_YY}Q2"
            
            elif MM in ("01","02","03"):
                CLM_FROM_DT = f"{PRIOR_YR}-07-01"
                CLM_THRU_DT = f"{PRIOR_YR}-09-30"		
                CLM_PRD = f"P{PRIOR_YY}Q3"

            elif MM in ("04","05","06"):
                CLM_FROM_DT = f"{PRIOR_YR}-10-01"
                CLM_THRU_DT = f"{PRIOR_YR}-12-31"		
                CLM_PRD = f"P{PRIOR_YY}Q4"
          
        else:
            # Use Override parm dates instead
            rootLogger.info("Using override parameter Extract dates")
                
            CLM_FROM_DT = f"{P_EXT_FROM_DT}"
            CLM_THRU_DT = f"{P_EXT_THRU_DT}"
            CLM_PRD = f"P{P_CLM_PRD}"
            
        # Display parameter dates that will be used by extract	
        rootLogger.info(f"{CLM_FROM_DT=}")
        rootLogger.info(f"{CLM_THRU_DT=}")
        rootLogger.info(f"{CLM_PRD=}")


        #################################################################################
        # Remove residual duals linux files
        #################################################################################
        rootLogger.info(" ")
        rootLogger.info(f"Remove residual {DUALS_MEDADV_FILE_PREFIX} files on linux data directory.")

        deleteFilesFromLinuxUsingPrefix(DATA_DIR, DUALS_MEDADV_FILE_PREFIX)

        
        #################################################################################
        # Download DUAL MA State parameter config file as List
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Get config file list of states")
        
        lstStates = getConfigFile(s3_client, XTR_BUCKET, f"{CONFIG_BUCKET_FLDR}{ST_PARMFILE}")


        #################################################################################
        # Loop thru States in config file  
        #################################################################################
        for ST in lstStates:

            # Remove trailing/leading spaces.
            EXT_ST = ST.strip()
            
            #################################################################################
            # Display State to process 
            #################################################################################
            rootLogger.info(" ")
            rootLogger.info(f"{EXT_ST=}")

            #################################################################################
            # skip comments 
            #################################################################################
            if EXT_ST[0:1] == "#":
                continue


            #############################################################
            # Export environment variables for Python code
            #############################################################
            os.environ["TMSTMP"] = TMSTMP
            os.environ["CLM_FROM_DT"] = CLM_FROM_DT
            os.environ["CLM_THRU_DT"] = CLM_THRU_DT
            os.environ["EXT_ST"] = EXT_ST
            os.environ["CLM_PRD"] = CLM_PRD

            #############################################################
            # Execute Python code to Extract PTA claims data.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Start execution of DUALS_MedAdv_PTA_Ext.py program")

            try:
                sp_info = subprocess.run(['python3', 'DUALS_MedAdv_PTA_Ext.py'], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling DUALS_MedAdv_PTA_Ext.py failed with return code {e.returncode}")
                rootLogger.error(e.stdout)
                rootLogger.error(e.stderr)
                
                ## Send Failure email	
                SUBJECT=f"DUALS_MedAdv_PTA_Ext.py - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"DUALS_MedAdv_PTA_Ext.py has failed. "
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    
    

            rootLogger.info("")
            rootLogger.info("Python script DUALS_MedAdv_PTA_Ext.py completed successfully.")

            #############################################################
            # Execute Python code to Extract PTB claims data.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Start execution of DUALS_MedAdv_PTB_Ext.py program")

            try:
                sp_info = subprocess.run(['python3', 'DUALS_MedAdv_PTB_Ext.py'], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling DUALS_MedAdv_PTB_Ext.py failed with return code {e.returncode}")
                rootLogger.error(e.stdout)
                rootLogger.error(e.stderr)
                
                ## Send Failure email	
                SUBJECT=f"DUALS_MedAdv_PTB_Ext.py - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"DUALS_MedAdv_PTB_Ext.py has failed. "
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    
    

            rootLogger.info("")
            rootLogger.info("Python script DUALS_MedAdv_PTB_Ext.py completed successfully.")

            #############################################################
            # Get list of S3 files and record counts for success email.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Get S3 Extract file list and record counts")
            
            # log file contents need to be converted to string
            S3Files = getExtractFilenamesAndCounts(rootLogger, LOGNAME)              
                
                
            #############################################################
            # Send Success email.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Send success email with S3 Extract filename.")
            rootLogger.info(f"{S3Files=} ")

            SUBJECT=f"DUALS Medicare Advantage extract ({ENVNAME}{TESTEMAIL})" 
            MSG=f"The Extract for the creation of the DUALS Medicare Advantage file(s) from Snowflake has completed.\n\nEFT versions of the below files were created using the following file mask {EFT_FILEMASK}.\n\nThe following file(s) were created:\n\n{S3Files}"

            try:
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DUALMEDADV_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error(e.stdout)
                rootLogger.error(e.stderr)

                sys.exit(12)    
            

            #############################################################
            # Download BH file from s3
            #############################################################
            gzipDUALS_MEDADV_BH_FILE = f"DUALS_MedAdv_BH_{EXT_ST}_{CLM_PRD}_{TMSTMP}.txt.gz"
            
            downloadFileFromS3(s3_client, XTR_BUCKET, f"{DUALS_MedAdv_BUCKET_FLDR}{gzipDUALS_MEDADV_BH_FILE}", f"{DATA_DIR}{gzipDUALS_MEDADV_BH_FILE}")

            #############################################################
            # Unzip BH file from s3
            #############################################################
            txtUnzippedFilename = unzipFile(DATA_DIR, gzipDUALS_MEDADV_BH_FILE)

            #############################################################
            # Move original BH file to archive directory
            #############################################################
            s3MoveLargeFile2NewFolder(s3_client, XTR_BUCKET, f"{DUALS_MedAdv_BUCKET_FLDR}{gzipDUALS_MEDADV_BH_FILE}", f"{DUALS_MedAdv_BUCKET_FLDR}archive/{gzipDUALS_MEDADV_BH_FILE}")

            #############################################################
            # Split BH file into two parts
            #############################################################
            lstOutputFilesNPaths = splitTextFileIntoMultipleFiles(f"{DATA_DIR}{txtUnzippedFilename}", 2, f"{DATA_DIR}{txtUnzippedFilename}")

            #############################################################
            # Free space on linux: Delete unzipped file.
            #############################################################
            rootLogger.info(" ")
            rootLogger.info("Remove unzipped non-split file from linux.")
            deleteFileFromLinux(f"{DATA_DIR}{txtUnzippedFilename}")
            
            #############################################################
            # Zip BH Split files and upload to s3
            #############################################################
            for sOutputFileNPath in lstOutputFilesNPaths:
                sOutputFilename = sOutputFileNPath.replace(DATA_DIR,"")

                #############################################################
                # Zip BH Split file 
                #############################################################
                gzipSplitFilename = gzipFile(DATA_DIR, sOutputFilename)

                #############################################################
                # Upload BH Split file to s3 directory
                #############################################################
                s3UploadFile(s3_client, f"{DATA_DIR}{gzipSplitFilename}", XTR_BUCKET, f"{DUALS_MedAdv_BUCKET_FLDR}{gzipSplitFilename}")


            #############################################################
            # Free space on linux: delete text split files, and zipped split files 
            #############################################################
            rootLogger.info(" ")
            rootLogger.info("Remove residual files from linux data directory.")
            deleteFilesFromLinuxUsingPrefix(DATA_DIR, DUALS_MEDADV_FILE_PREFIX)


            #############################################################
            # EFT Extract files
            #############################################################
            rootLogger.info("")
            rootLogger.info("EFT DUALS Medicare Advantage Extract Files")
            
            try:
                sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', f"{XTR_BUCKET}/{DUALS_MedAdv_BUCKET_FLDR}" ], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
                rootLogger.error(e.stdout)
                rootLogger.error(e.stderr)
                
                ## Send Failure email	
                SUBJECT = f"DUALS_MedAdv_Ext.py EFT process  - Failed ({ENVNAME})"
                MSG= f"DUALS_MedAdv_Ext.py EFT process has failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)  


            #############################################################
            # End-For loop
            #############################################################
            
            

        #############################################################
        # script clean-up
        #############################################################
        rootLogger.info(" ")
        rootLogger.info("Remove residual files from linux data directory.")
        deleteFilesFromLinuxUsingPrefix(DATA_DIR, DUALS_MEDADV_FILE_PREFIX)


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script DUALS_MedAdv_PTB_Ext.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in DUALS_MedAdv_PTB_Ext.py\n {e}")

        rootLogger.error("Exception occured in DUALS_MedAdv_PTB_Ext.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in DUALS_MedAdv_Ext.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in DUALS_MedAdv_Ext.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)  
        

if __name__ == "__main__":
    
    main_processing_loop()