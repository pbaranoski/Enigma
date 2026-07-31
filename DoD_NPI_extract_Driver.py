############################################################################################################
# Name:  DoD_NPI_extract.sh
#
# Desc: DoD_NPI Extract - Load DoD finder file into IDR. Extract data from IDR that matches SSNs in FF.
#
# Author     : Paul Baranoski	
# Created    : 09/13/2023
#
# Modified:
#
# Paul Baranoski   2025-09-11  Create script.
# Paul Baranoski   2026-06-30  Convert from bash to python.
############################################################################################################

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

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

S3BUCKET = rf"{XTR_BUCKET}/{DOD_NPI_BUCKET_FLDR}"


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}DoD_NPI_extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDoD_NPI_extract_Driver.py started at {TMSTMP}")

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
        if not (iNOFParms == 0 ):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")


        #############################################################
        # Execute Python code to Extract claims data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_DoD_NPI_FNDR_FILE_Driver.py program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_DoD_NPI_FNDR_FILE_Driver.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            # End gracefully
            if e.returncode == 4:
                rootLogger.info("LOAD_DoD_NPI_FNDR_FILE.sh ended. No Finder Files found.")
                rootLogger.info(f"\nEnded at {TMSTMP}" )
                ## Send Failure email	
                SUBJECT=f"LOAD_DoD_NPI_FNDR_FILE_Driver.py ended. No Finder Files found. ({ENVNAME}{TESTEMAIL})"
                MSG=f"LOAD_DoD_NPI_FNDR_FILE_Driver.py  ended. No Finder Files found."
 
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                sys.exit(4)

                
            rootLogger.error(f"Calling LOAD_DoD_NPI_FNDR_FILE_Driver.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"LOAD_DoD_NPI_FNDR_FILE_Driver.py - Failed ({ENVNAME})"
            MSG=f"Bash script LOAD_DoD_NPI_FNDR_FILE_Driver.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    
        
        
        rootLogger.info("")
        rootLogger.info("bash script LOAD_DoD_NPI_FNDR_FILE_Driver.py completed successfully.")


        #################################################################################
        # Create Extract parameters
        #################################################################################

        # Export environment variables for Python extracts code
        os.environ["TMSTMP"] = TMSTMP
        
        
        #############################################################
        # Execute Python code to extract data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of DoD_NPI_extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'DoD_NPI_extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling DoD_NPI_extract.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)
            
            ## Send Failure email	
            SUBJECT=f"DoD_NPI_extract.py - Failed ({ENVNAME})"
            MSG=f"Python script DoD_NPI_extract.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script DoD_NPI_extract.py completed successfully.")
  

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
        SUBJECT=f" Weekly DoD_NPI extract - completed ({ENVNAME}{TESTEMAIL})"
        MSG = f"The Extract for the creation of the DoD NPI file has completed.\n\nThe following file(s) were created:\n\n{S3Files}"              
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DOD_NPI_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.stdout)
            rootLogger.error(e.stderr)

            sys.exit(12)     


        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT DoD_NPI extract File ")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', S3BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"DoD_NPI extract EFT process  - Failed ({ENVNAME})"
            MSG= f"DoD_NPI extract EFT process EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12) 
            

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Python program DoD_NPI_extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )


    except Exception as e:
        print (f"Exception occured in DoD_NPI_extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in DoD_NPI_extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in DoD_NPI_extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in DoD_NPI_extract_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()