#!/usr/bin/env python
########################################################################################################
# Name:  OPMHI_Tester_Driver.py
#
# DESC:   This script will encrypt a file using the CommonFunctionsGPG.py import module.
#
# Execute as python3 OPMHI_Tester_Driver.py     (processing without override date) 
#
#   
# Paul Baranoski   2026-05-12 Create Module.
########################################################################################################

import boto3 
import logging
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess

# Our common module with variable constants
from SET_XTR_ENV_v2 import *

import CommonFunctionsGPG as GPGFunctions

# Our include members
import LoggerStandard as EnigmaLog


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

        LOGNAME = f"{LOG_DIR}OPMHI_Tester_Driver_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nOPMHI_Tester_Driver.py started at {TMSTMP}")

        # Establish logger with CommonFunctions module.
        GPGFunctions.setCommonFunctionLogger(rootLogger) 

       
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
        # Encrypt Extract file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Encrypt Extract File. ")

        #####################################################
        # encrypt file using gpg
        #####################################################
        rootLogger.info("Get Secret Key. ")
        EncryptKey = GPGFunctions.get_secret(OPMHI_ENCRYPT_KEY_SECRET_NAME, REGION)

        rootLogger.info("Import gpg Key. ")
        gnupg_home = GPGFunctions.import_gpg_key(EncryptKey)
        
        rootLogger.info("get recipient. ")
        recipient = GPGFunctions.get_key_fingerprint(gnupg_home)
        rootLogger.info(f"{recipient=}")

        input_file = f"{DATA_DIR}nonprdHelloWorld.txt.gz"
        output_file = f"{DATA_DIR}nonprdHelloWorld3.txt.gz.gpg"

        rootLogger.info("encrypt file ")
        GPGFunctions.encrypt_file(gnupg_home, input_file, output_file, recipient)


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script  OPMHI_Tester_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in  OPMHI_Tester_Driver.py\n {e}")

        rootLogger.error("Exception occured in  OPMHI_Tester_Driver.py.")
        rootLogger.error("\n%s", str(e))

        SUBJECT=f" OPMHI_Tester_Driver.py - Failed ({ENVNAME})"
        MSG=f" OPMHI_Tester_Driver.py  has failed."

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()