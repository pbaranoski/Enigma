#!/usr/bin/python
############################################################################################################
# Script Name: DashboardInfo_SFUI_Driver.py
#
# Description: This script will extract dashboard info for extracts executed only using the Snowflake UI
#              by examining S3 files named DOJ_SFUI or FOIA_SFUI. Normal execution requires no parameters
#              and will process S3 files from the prior day.
#
#        NOTE: There are two options for providing override parameters.
#              1) supply two override parameter dates. This search for DOJ_SFUI and FOIA_SFUI files created between
#                 the override date range supplied.
#              2) supply three override parameters. Supply two override parameter dates, and 
#                 also supply a non SFUI folder and file prefix that may have been mis-named like DOJ/DOJ_TOUHY
#
# Execute script with no parameters, two override date parameters, or three parameters.
#  python3 DashboardInfo_SFUI_Driver.py 
#  python3 DashboardInfo_SFUI_Driver.py $1 $2  
#  python3 DashboardInfo_SFUI_Driver.py $1 $2 $3 
#
#  $1 --> RUN_FROM_DT (YYYYMMDD format) (Optional)
#  $2 --> RUN_TO_DT   (YYYYMMDD format) (Optional)
# 
#  $1 --> RUN_FROM_DT (YYYYMMDD format) (Optional)
#  $2 --> RUN_TO_DT   (YYYYMMDD format) (Optional)
#  $3 --> BktFldrNFilePrefix (Optional) (Ex. 'DOJ/DOJ_TOUHY') 
# 
#
# Author     : Paul Baranoski	
# Created    : 04/07/2025
#
# Paul Baranoski 2025-04-07 Created script.
# Paul Baranoski 2026-03-30 Convert to python.
############################################################################################################

import os
import os.path
import sys
from pathlib import Path
from datetime import datetime
from datetime import date,time,timedelta
import subprocess

import io
import re
import boto3
import json
from collections import deque

# Our common module with variable constants
from SET_XTR_ENV import *

DATADIR = "/app/IDRC/XTR/CMS/data/"
LOGDIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

import LoggerStandard as EnigmaLog
from CommonFunctions import * 



#############################################################
# Functions
#############################################################
def sendEmailNothing2Process(prmFromDt, prmToDt, prmS3FldNFilePrefix ):   

	############################################################
	# Success email. 
	############################################################
    rootLogger.info("")
    rootLogger.info(f"No S3 files found to process for {prmS3FldNFilePrefix} between {prmFromDt} and {prmToDt}.")

    SUBJECT = f"DashboardInfo_SFUI ({ENVNAME})" 
    MSG = f"No S3 files found to process for {prmS3FldNFilePrefix} between {prmFromDt} and {prmToDt}."

    try:
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
    except subprocess.CalledProcessError as e:
        rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
        rootLogger.error(e.output)

        sys.exit(12)

	
def UpdateDashboardSFTables(prmBucketnFldr, prmFilePrefix, S3LoadFileTmstmp, RUN_FROM_DT, RUN_TO_DT):
	
    #############################################################################################
    # Execute Python code to build S3 Job Details/Info json files AND update SF Dashboard tables
    #############################################################################################
    rootLogger.info("")

    rootLogger.info(f"{prmBucketnFldr=}")
    rootLogger.info(f"{prmFilePrefix=}")
    rootLogger.info(f"{S3LoadFileTmstmp=}")

    BucketFldrNFilePrefix = f"{prmBucketnFldr}{prmFilePrefix}"
    rootLogger.info(f"{BucketFldrNFilePrefix=}")


    #############################################################
    # Execute Python code to Extract claims data.
    #############################################################
    rootLogger.info("")
    rootLogger.info("Start execution of DashboardInfo_SFUI.py")
    
    try:
        
        sp_info = subprocess.run(['python3', 'DashboardInfo_SFUI.py', '--BktFldrNFilePrefix', BucketFldrNFilePrefix, '--FromDate',  RUN_FROM_DT,  '--ToDate', RUN_TO_DT, '--TMSTMP', S3LoadFileTmstmp ], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
    
    except subprocess.CalledProcessError as e:
    
        # if RC == 4 --> nothing to process; end gracefully
        if e.returncode == 4:
            rootLogger.info("")
            sendEmailNothing2Process(RUN_FROM_DT, RUN_TO_DT, BucketFldrNFilePrefix)
    
            return 0
    
        # RC != 4 --> serious error
        rootLogger.error(f"Calling DashboardInfo_SFUI.py failed with return code {e.returncode}")
        rootLogger.error("\n%s", e.stdout)
        rootLogger.error("\n%s", e.stderr)
    
        ## Send Failure email	
        SUBJECT = f"Python program DashboardInfo_SFUI.py - Failed ({ENVNAME})"
        MSG = "Python program DashboardInfo_SFUI.py failed."
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 

        sys.exit(12)    


    rootLogger.info("")
    rootLogger.info("Python script DashboardInfo_SFUI.py completed successfully.")


	############################################################
	# Success email. 
	############################################################
    rootLogger.info("")
    rootLogger.info(f"Send success email for load of Dashboard tables for {prmFilePrefix} files for period {RUN_FROM_DT} to {RUN_TO_DT}.")

    SUBJECT = f"DashboardInfo_SFUI ({ENVNAME})" 
    MSG = f"The loading of the Dashboard tables with SF UI extract information for {prmFilePrefix} files from {RUN_FROM_DT} to {RUN_TO_DT} has completed successfully."

    try:
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)
        
    except subprocess.CalledProcessError as e:
        rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
        rootLogger.error(e.output)

        sys.exit(12)


	############################################################
	# Define DASHBOARD json load files with correct timestamp
	############################################################
    DASHBOARD_JOBINFO_FILE = f"DASHBOARD_JOB_INFO_{S3LoadFileTmstmp}.json"
    DASHBOARD_JOBDTLS_FILE = f"DASHBOARD_JOB_DTLS_EXTRACT_FILES_{S3LoadFileTmstmp}.json"

	############################################################
	# Move Dashboard JOBINFO json file to S3 archive folder.
	############################################################
    rootLogger.info("")
    s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{DASHBOARD_BUCKET_FLDR}{DASHBOARD_JOBINFO_FILE}", f"{DASHBOARD_BUCKET_FLDR}archive/{DASHBOARD_JOBINFO_FILE}")

	############################################################
	# Move Dashboard JOBDTLS json file to S3 archive folder.
	############################################################
    rootLogger.info("")
    s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{DASHBOARD_BUCKET_FLDR}{DASHBOARD_JOBDTLS_FILE}", f"{DASHBOARD_BUCKET_FLDR}archive/{DASHBOARD_JOBDTLS_FILE}")
 

def main_processing_loop():
    
    try:    
    
        # Keep track of warnings
        global TOT_WARNINGS
        TOT_WARNINGS = 0
        
        global reEndedAt
        reEndedAt = None
        
        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        print(f"{TMSTMP=}")

        global LOGNAME
        LOGNAME = f"{LOGDIR}DashboardInfo_SFUI_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDashboardInfo_SFUI_Driver.py started at {TMSTMP}")

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(rootLogger)
        
        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")


        ##################################################################
        # Extract can run stand-alone or as a called script.
        ##################################################################
        iNOFParms = len(sys.argv) - 1
        if not (iNOFParms == 0 or iNOFParms ==  2 or iNOFParms ==  3):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)


        ##################################################################
        # Extract log information for yesteray  
        #  --> unless overriding with date range   
        ##################################################################
        ProcessNonSFUIFiles="N"
            
        if iNOFParms ==  2:
            rootLogger.info("")
            rootLogger.info("Using override dates ")

            lstParms = sys.argv
            RUN_FROM_DT = lstParms[1]
            RUN_TO_DT = lstParms[2]
            parmOverrideFldrNFilePrefix = ""
            
        elif iNOFParms ==  3:
            rootLogger.info("")
            rootLogger.info("Using override dates ")
            
            ProcessNonSFUIFiles="Y"
            
            RUN_FROM_DT = lstParms[1]
            RUN_TO_DT = lstParms[2]
            parmOverrideFldrNFilePrefix = lstParms[3]
            
        else:
            rootLogger.info("")
            rootLogger.info("Using script calculated dates ")
            
            # get yesterday's date
            dttmCalcDate = (datetime.today() + timedelta(days=-1))
            RUN_FROM_DT = dttmCalcDate.strftime('%Y%m%d')  

            dttmCalcDate = (datetime.today() + timedelta(days=-1))
            RUN_TO_DT = dttmCalcDate.strftime('%Y%m%d')  

            parmOverrideFldrNFilePrefix=""


        #############################################################
        # Display parameters passed to script 
        #############################################################
        rootLogger.info("")
        rootLogger.info("Parameters to script: ")
        rootLogger.info(f"{RUN_FROM_DT=}")
        rootLogger.info(f"{RUN_TO_DT=}")
        rootLogger.info(f"{parmOverrideFldrNFilePrefix=}")


        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client/resource objects")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")

        
        ##################################################################
        # Display Dashboard constants in log
        ##################################################################
        rootLogger.info(" ")
        rootLogger.info(f"{DASHBOARD_BUCKET_FLDR=}")

        DASHBOARD_SFUI_HLQ = "DASHBOARD_SFUI"
        rootLogger.info(f"{DASHBOARD_SFUI_HLQ=}")
        

        #################################################################################
        # Create and Update S3 Job Details/Info files for loading into SF
        #################################################################################
        if ProcessNonSFUIFiles == "Y":
            rootLogger.info("")
            rootLogger.info(f"Begin processing of {parmOverrideFldrNFilePrefix} files")

            UpdateDashboardSFTables(f"{XTR_BUCKET}/{bucket_fldr}", parmOverrideFldrNFilePrefix, TMSTMP, RUN_FROM_DT, RUN_TO_DT)
            
        else:
            rootLogger.info("")
            rootLogger.info("Begin processing of DOJ_SFUI files")

            UpdateDashboardSFTables(f"{XTR_BUCKET}/{DOJ_BUCKET_FLDR}", "DOJ_SFUI", f"{TMSTMP}1", RUN_FROM_DT, RUN_TO_DT )

            rootLogger.info("")
            rootLogger.info("Begin processing of FOIA_SFUI files")

            UpdateDashboardSFTables(f"{XTR_BUCKET}/{FOIA_BUCKET_FLDR}", "FOIA_SFUI", f"{TMSTMP}2", RUN_FROM_DT, RUN_TO_DT)


        #############################################################
        # script clean-up
        #############################################################
        rootLogger.info("")
        rootLogger.info("Remove temporary text files from data directory") 


        #############################################################
        # end script
        #############################################################
        rootLogger.info("")
        rootLogger.info("DashboardInfoDriver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in DashboardInfo_SFUI_Driver.py\n {e}")

        rootLogger.error("Exception occured in DashboardInfo_SFUI_Driver.")
        rootLogger.error("\n%s", e)

        ## Send Failure email	
        SUBJECT=f"DashboardInfo_SFUI_Driver.py  - Failed ({ENVNAME})"
        MSG=f"Exception occured in DashboardInfo_SFUI_Driver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)  
        
        
if __name__ == "__main__":

    main_processing_loop()