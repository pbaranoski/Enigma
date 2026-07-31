#!/usr/bin/python
############################################################################################################
# Script Name: DashboardInfo_GUI_Driver.py
#
# Description: This script will process json SF table load files which have been manually loaded into the S3://Dashboard folder.
#      
#              Ex.DASHBOARD_GUI_JOB_INFO_20250923.151850.json and DASHBOARD_GUI_JOB_DTLS_20250923.151850.json 		
# 
#              The normal Dashboard script/process that parses the logs and the Dashboard SFUI script/process
#              build the SF load files as well as process them. Their companion python modules knows which 
#              SF load files to process since the exact filenames to load are passed from the shell script
#              to the python module. This means any DASHBOARD_GUI load files will not be accidentally processed
#              by either of those processes.
#
#              Since the SF load files to be processed are created outside of the scripts that will process them,
#              this script will look for any DASHBOARD_GUI SF load files in s3://Dashboard bucket/folder and process 
#              them. After processing each load file, it will be moved to the s3://Dashboard/archive folder to 
#              prevent re-processing of the files.
#
# Execute script with no parameters
#  ./DashboardInfo_GUI.sh 
#
# Paul Baranoski 2025-12-18 Created python module using bash script.
# Paul Baranoski 2026-01-15 Modified some variable names for clarity. Also, removed the s3 folder from the JOBINFO and JOBDTLS
#                           s3FileKey environment variables being "passed" to the python SQL module.
# Paul Baranoski 2026-03-23 Add import of CommonFunctions. Remove hard-coded common functions from program.
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


# bytes pretty-printing
UNITS_MAPPING = [
    (1<<50, ' PB'),
    (1<<40, ' TB'),
    (1<<30, ' GB'),
    (1<<20, ' MB'),
    (1<<10, ' KB'),
    (1, (' byte', ' bytes')),
]


#############################################################
# Functions
#############################################################
def convertBytes2ReadableSize(iprmBytes, units=UNITS_MAPPING):

    # iprmBytes is expected to be an integer and not a string    
    rootLogger.info("")

    rootLogger.info(f"{iprmBytes=}")	

    if type(iprmBytes) is not int:
        raise TypeError("iprmBytes is expected to be an integer.")

        
    # convert total bytes to human readable file size
    rootLogger.info("Start conversion of total bytes to human readable file size")

    # Calculate human readable file size
    for factor, suffix in units:
        if iprmBytes >= factor:
            break
            
    #amount = int(bytes / factor)
    fAmount = round(( iprmBytes / factor),2)

    if isinstance(suffix, tuple):
        singular, multiple = suffix
        if fAmount == 1:
            suffix = singular
        else:
            suffix = multiple

    if suffix.strip() in ['byte','bytes']:
        sAmount = f"{fAmount:.0f}"
    else:    
        sAmount = f"{fAmount:.2f}"
        
    sHumanFileSize = str(sAmount) + suffix
    rootLogger.info(f"{sHumanFileSize=}")

    return sHumanFileSize


def createJobInfoKeyValuePairs(prmLogfilename, prmJobSuccess): 

    rootLogger.info("")

    rootLogger.info(f"{prmLogfilename=}")
    rootLogger.info(f"{prmJobSuccess=}") 
	
    ############################################################	
    # Create Key/value pairs for Job Info
    ############################################################
    #Ex. blbtn_clm_ext_20231020.134153.log, Fri Oct 20 13:41:53 EDT 2023,Fri Oct 20 13:42:05 EDT 2023
    #Ex. OFM_PDE_Extract_20231018.163447.log, Wed Oct 18 16:34:47 EDT 2023,Wed Oct 18 16:56:11 EDT 2023	

    rootLogger.info("")
    rootLogger.info("Parse for Key Values")

    ##########################################################
	# Parse log filename for Extract name and Run Timestamp
    # Ex. blbtn_clm_ext_20231020.134153.log
    ##########################################################
    sExtName = getExtNameFromLogFilename(prmLogfilename)
    sRunTmpstmp = getRunTimestampFromLogFilename(prmLogfilename)

    # Extract the RunDate from runTimestamp
    sRunDate = sRunTmpstmp.split(".",1)[0]
    rootLogger.info(f"{sRunDate=}")

	##########################################
	# Build JobInfo load record in json format
	##########################################	
    sDashboardJobInfoRec = fr'{{"log": "{prmLogfilename}", "ext": "{sExtName}", "runTmstmp": "{sRunTmpstmp}", "success": "{prmJobSuccess}" }} '

    return sDashboardJobInfoRec
 

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
        LOGNAME = f"{LOGDIR}DashboardInfo_GUI_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDashboardInfo_GUI_Driver.py started at {TMSTMP}")

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
        rootLogger.info(f"Get s3 Client/resource objects")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")

        
        ##################################################################
        # Display Dashboard constants in log
        ##################################################################
        rootLogger.info(" ")
        rootLogger.info(f"{DASHBOARD_BUCKET_FLDR=}")

        DASHBOARD_GUI_HLQ = "DASHBOARD_GUI"
        rootLogger.info(f"{DASHBOARD_GUI_HLQ=}")
        
        DASHBOARD_GUI_KEY_PREFIX = DASHBOARD_BUCKET_FLDR + DASHBOARD_GUI_HLQ

        
        #################################################################
        # Get List of DASHBOARD_GUI Load files in s3://Dashboard folder
        # and download to DATADIR.
        #
        # NOTE: Send email when there are no files to process.
        #################################################################
        rootLogger.info("")
        rootLogger.info("Are there Dashboard GUI SF load files to process? ")

        # Get list of Extract filenames (with folder path) that area ONLY under the requested path. No "archive" folder filenames. No folder without ext filename: "xtr/PSPS/"
        lstDashboardGUIKeys = [ obj.key for obj in s3_resource.Bucket(XTR_BUCKET).objects.filter(Prefix=DASHBOARD_GUI_KEY_PREFIX)  if not obj.key.endswith("/") ]
        rootLogger.info("lstDashboardGUIKeys:\n" + "\n".join(lstDashboardGUIKeys))
       
        NOF_FILES = len(lstDashboardGUIKeys)
        rootLogger.info(f"{NOF_FILES} Dashboard GUI files were found to process")

        if NOF_FILES == 0:
            # No files to process
            rootLogger.info("")
            rootLogger.info(f"DashboardInfo_GUI_Driver.py - No DASHBOARD_GUI files to process in {DASHBOARD_BUCKET_FLDR} like {DASHBOARD_GUI_HLQ}* ")

            ## Send Failure email	
            SUBJECT = f"DashboardInfo_GUI_Driver.py - No Dashboard GUI load files found to process. ({ENVNAME})"
            MSG = f"No Dashboard GUI load files found to process in {DASHBOARD_BUCKET_FLDR} like {DASHBOARD_GUI_HLQ}* "

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(0)	        
        

        #################################################################
        # Sort the list of files by:
        #   1) the 5th node (timestamp) (so the matching INFO and DTLS files
        #      are processed together.
        #   2) then the 4th node (INFO before DTLS)
        #
        #   Priority is a dictionary that assigns INFO or DTLS a numeric sort order. "99" is the default value if no match is found in dict.
        #   
        #
        # Example:
        #     DASHBOARD_GUI_JOB_INFO_20250923.151850.json
        #     DASHBOARD_GUI_JOB_DTLS_20250923.151850.json
        #     DASHBOARD_GUI_JOB_INFO_20250924.151850.json
        #     DASHBOARD_GUI_JOB_DTLS_20250924.151850.json
        #
        #################################################################
        priority = { "INFO": 0, "DTLS": 1,}
        lstDashboardGUIKeys.sort(key=lambda f: ( f.split("_")[4] + "-" + str(priority.get(f.split("_")[3], 99) ) )  )

        rootLogger.info("")
        rootLogger.info(f"List is now sorted: {lstDashboardGUIKeys=}")

        #################################################################
        # Init loop variables
        #################################################################
        rootLogger.info("")
        rootLogger.info("Initialize loop variables")

        swJOB_INFO_EXISTS = False
        swJOB_DTLS_EXISTS = False
        DASHBOARD_JOBINFO_S3KEY = ""
        DASHBOARD_JOBDTLS_S3KEY = ""

        #################################################################
        # Loop thru DASHBOARD_GUI companion files (JOB_INFO and JOB_DTLS)
        #################################################################
        rootLogger.info("Process GUI S3 Files")

        for iNOF_FILES_READ, sDASHBOARD_GUI_s3FileKey in enumerate(lstDashboardGUIKeys, start=1):

            rootLogger.info("")
            rootLogger.info(f"{iNOF_FILES_READ=}")
            rootLogger.info(f"Next DASHBOARD_GUI_s3File to process: {sDASHBOARD_GUI_s3FileKey}")

            #################################################################
            # Load appropriate s3 Load filename variables, set boolean flags
            # NOTE: both companion load files should be present INFO and DTLS
            #################################################################	
            if  sDASHBOARD_GUI_s3FileKey.find('JOB_INFO') >= 0:
                swJOB_INFO_EXISTS = True

                DASHBOARD_JOBINFO_S3KEY = sDASHBOARD_GUI_s3FileKey 
                rootLogger.info(f"{DASHBOARD_JOBINFO_S3KEY=}") 

                JOB_INFO_TMSTMP = DASHBOARD_JOBINFO_S3KEY.split("_")[4]
                rootLogger.info(f"{JOB_INFO_TMSTMP=}")

            elif  sDASHBOARD_GUI_s3FileKey.find('JOB_DTLS') >= 0:    
                swJOB_DTLS_EXISTS = True 

                DASHBOARD_JOBDTLS_S3KEY = sDASHBOARD_GUI_s3FileKey 
                rootLogger.info(f"{DASHBOARD_JOBDTLS_S3KEY=}") 

                JOB_DTLS_TMSTMP = DASHBOARD_JOBDTLS_S3KEY.split("_")[4]
                rootLogger.info(f"{JOB_DTLS_TMSTMP=}")
            else:
                # One of the paired files is missing
                rootLogger.info("")
                rootLogger.info("Program DashboardInfo_GUI.sh failed. ")
                rootLogger.info(f"Dashboard GUI File {sDASHBOARD_GUI_s3FileKey} is not named correctly.")
                
                # Send Failure email	
                SUBJECT = f"Program DashboardInfo_GUI.sh - Failed ({ENVNAME})"
                MSG = f"Dashboard GUI File {sDASHBOARD_GUI_s3FileKey} is not named correctly."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                rootLogger.info(sp_info) 

                sys.exit(12) 


            #################################################################
            # Need both JOB_INFO and JOB_DTLS files to be present to process
            #################################################################
            if (iNOF_FILES_READ % 2) == 0:
                rootLogger.info("iNOF_FILES_READ is even. We have two files to process.")

                ###############################################################################
                # File timestamps need to match --> ensure they are the correct pair to process
                ###############################################################################		
                if  swJOB_INFO_EXISTS and swJOB_DTLS_EXISTS:
                
                    if JOB_INFO_TMSTMP != JOB_DTLS_TMSTMP:
                        # JOB_INFO and JOB_DTLS files are not correct paired files
                        rootLogger.info("")
                        rootLogger.info("Program DashboardInfo_GUI.sh failed. ")
                        rootLogger.info(f"JOB_INFO and JOB_DTLS files are not correct paired files. \n\n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}")
                        
                        # Send Failure email	
                        SUBJECT = f"Program DashboardInfo_GUI.sh - Failed ({ENVNAME})"
                        MSG = f"Program DashboardInfo_GUI.sh failed. JOB_INFO and JOB_DTLS files are not correct paired files. \n\n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}"

                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                        rootLogger.info(sp_info) 

                        sys.exit(12)			
                    
                else:
                    # One of the paired files is missing
                    rootLogger.info("")
                    rootLogger.info("Program DashboardInfo_GUI.sh failed. ")
                    rootLogger.info(f"One of the paired Dashboard GUI files is missing. \n\n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}")
                    
                    # Send Failure email	
                    SUBJECT = f"Program DashboardInfo_GUI.sh - Failed ({ENVNAME})"
                    MSG = f"Program DashboardInfo_GUI.sh failed. One of the paired Dashboard GUI files is missing. \n\n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}"

                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    rootLogger.info(sp_info) 

                    sys.exit(12)

            else:
                # We need to have both paired load filenames before processing them. We only have one at this time
                continue   

	
            #################################################################
            # Execute python module to process the 2 json load files to 
            #  update SF tables. 
            #################################################################
            rootLogger.info("")
            rootLogger.info("Start execution of Dashboard_GUI.py program")

            # Export environment variables for Python code. Isolate the filename from the S3FileKey (s3Path + filename)
            os.environ["DASHBOARD_JOBINFO_FILE"] = DASHBOARD_JOBINFO_S3KEY.replace(DASHBOARD_BUCKET_FLDR,"")
            os.environ["DASHBOARD_JOBDTLS_FILE"] = DASHBOARD_JOBDTLS_S3KEY.replace(DASHBOARD_BUCKET_FLDR,"")

            try:
                sp_info = subprocess.run(['python3', 'DashboardInfo_GUI.py'], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling DashboardInfo_GUI.py failed with return code {e.returncode}")
                rootLogger.error(e.output)
                
                ## Send Failure email	
                SUBJECT="Python program DashboardInfo_GUI.py - Failed (${ENVNAME})"
                MSG="Python program DashboardInfo_GUI.py failed."
                
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    
    

            rootLogger.info("")
            rootLogger.info("Python script DashboardInfo_GUI.py completed successfully.")

	
            ############################################################
            # Move Dashboard JOBINFO json file to S3 archive folder.
            ############################################################
            rootLogger.info("")
            rootLogger.info(f"Move S3 {DASHBOARD_JOBINFO_S3KEY} file to S3 {DASHBOARD_BUCKET_FLDR}archive folder")

            DASHBOARD_BUCKET_ARCHIVE_FLDR = f"{DASHBOARD_BUCKET_FLDR}archive/"
           
            sSourceKey = f"{DASHBOARD_JOBINFO_S3KEY}" 
            sDestinationKey = sSourceKey.replace(DASHBOARD_BUCKET_FLDR, DASHBOARD_BUCKET_ARCHIVE_FLDR)
            
            rootLogger.info(f"{sSourceKey=}")
            rootLogger.info(f"{sDestinationKey=}")
                
            s3MoveFile2NewFolder(s3_client, XTR_BUCKET, sSourceKey, sDestinationKey)

        
            ############################################################
            # Move Dashboard JOBDTLS json file to S3 archive folder.
            ############################################################
            rootLogger.info("")
            rootLogger.info(f"Move S3 {DASHBOARD_JOBDTLS_S3KEY} file to S3 {DASHBOARD_BUCKET_FLDR}archive folder")
           
            sSourceKey = f"{DASHBOARD_JOBDTLS_S3KEY}" 
            sDestinationKey = sSourceKey.replace(DASHBOARD_BUCKET_FLDR, DASHBOARD_BUCKET_ARCHIVE_FLDR)
            
            rootLogger.info(f"{sSourceKey=}")
            rootLogger.info(f"{sDestinationKey=}")
                
            s3MoveFile2NewFolder(s3_client, XTR_BUCKET, sSourceKey, sDestinationKey)


            ############################################################
            # Success email for each set of load files
            ############################################################
            rootLogger.info("")
            rootLogger.info(f"Send success email for load of Dashboard tables using load files: \n\n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}")

            SUBJECT = f"DashboardInfo_GUI ({ENVNAME})" 
            MSG = f"The loading of the Dashboard tables with GUI Load files (listed below) has completed successfully. \n\n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}"

            try:
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error(e.output)

                sys.exit(12) 
            

            #################################################################
            # Initialize loop variables for next set of load files to process
            #################################################################
            swJOB_INFO_EXISTS = False
            swJOB_DTLS_EXISTS = False
            DASHBOARD_JOBINFO_S3KEY = ""
            DASHBOARD_JOBDTLS_S3KEY = ""
	

        #################################################################
        # If there were an odd number of Load files read  
        #  --> an incomplete set of load files was not fully processed for 
        #  --> last set.
        #################################################################
        if (iNOF_FILES_READ% 2 ) == 1:
            rootLogger.info("")
            rootLogger.info(f"Program DashboardInfo_GUI.sh failed.")
            rootLogger.info("Python program DashboardInfo_GUI.sh failed.")
            
            # Send Failure email	
            SUBJECT = f"Program DashboardInfo_GUI.sh - Failed ({ENVNAME})"
            MSG = f"Program DashboardInfo_GUI.sh failed. One of the paired Dashboard GUI files is missing. \n{DASHBOARD_JOBINFO_S3KEY=} \n{DASHBOARD_JOBDTLS_S3KEY=}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)

            sys.exit(12)


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
        print (f"Exception occured in DashboardInfo_GUI_Driver.py\n {e}")

        rootLogger.error("Exception occured in DashboardInfo_GUI_Driver.")
        rootLogger.error("\n%s", e)

        ## Send Failure email	
        SUBJECT=f"DashboardInfo_GUI_Driver.py  - Failed ({ENVNAME})"
        MSG=f"Exception occured in DashboardInfo_GUI_Driver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)  
        
        
if __name__ == "__main__":

    main_processing_loop()