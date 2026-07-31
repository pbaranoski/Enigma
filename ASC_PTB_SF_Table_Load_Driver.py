#!/usr/bin/env python
########################################################################################################
# Name:  ASC_PTB_SF_Table_Load_Driver.py
#
# Desc: Load ASC PTB EFT extract files into SF table.
#           
#       Execute: python3 ASC_PTB_SF_Table_Load_Driver.py --EFT_Filename P#EFT.ON.ASCPS.Y{PRIOR-YR}.MAR{CUR-YR}.DYYMMDD.THHMMSST  
#                                                                   Ex. P#EFT.ON.ASCPS.Y2023.MAR24.DYYMMDD.THHMMSST 
#
# Paul Baranoski   2025-09-08 Create Module.
# Paul Baranoski   2025-09-12 Modify module to remove subprocess call to ASC_PTB_SF_Table_Load.py module and convert this to an internal function call 
#                             by importing the called module into this one.
# Paul Baranoski   2025-11-04 Modify subprocess.checkoutput to subprocess.run
########################################################################################################

import boto3 
import logging
import os
import sys
import io
import argparse
import contextlib

#import datetime
from datetime import datetime
from datetime import date,timedelta

import subprocess

# Our common module with variable constants
from SET_XTR_ENV import *


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

import LoggerStandard as EnigmaLog
import ASC_PTB_SF_Table_Load as ExtSQL


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

        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        print(f"{TMSTMP=}")

        global LOGNAME
        LOGNAME = f"{LOG_DIR}ASC_PTB_SF_Table_Load_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nASC_PTB_SF_Table_Load_Driver.py started at {TMSTMP}")

        
        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")
        
        ##########################################
        # Get any parameters
        ##########################################
        rootLogger.info(f"Get Parameters")

        parser = argparse.ArgumentParser(description="ASC PTB SF Table load parms")
        parser.add_argument("--EFT_Filename", help="ASC PTB Extract Filename to load into SF")
        args = parser.parse_args()

        global sASC_Ext_Filename 
        sASC_Ext_Filename = str(args.EFT_Filename)

        
        ##########################################
        # Establish variables
        ##########################################
        sInputRec = ""
        lstOutputRecs = []

        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")
        
        ##########################################
        # Does SF Filename exist? 
        ##########################################
        rootLogger.info("Does ASC PTB extract file exist in S3?") 

        s3Folder_n_filename = f"{EFT_FILEST_BUCKET_FLDR}{sASC_Ext_Filename}"
        rootLogger.info(f"{s3Folder_n_filename=}")

        # Is ASC PTB file in s3?         
        resp = s3_client.list_objects_v2(Bucket=XTR_BUCKET, Prefix=s3Folder_n_filename)
        if resp == None:
            ## Send Failure email	
            SUBJECT=f"ASC_PTB_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"ASC PTB Extract file {s3Folder_n_filename} is not in S3. Process failed. "
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)    
            

        ##########################################
        # Get S3 EFT ASC PTB Extract File  
        ##########################################
        rootLogger.info("Get ASC PTB extract file from S3 - Dict Object") 
        
        txt_file = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3Folder_n_filename)

        if txt_file == None:
            ## Send Failure email	
            SUBJECT=f"ASC_PTB_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"ASC PTB Extract file {s3Folder_n_filename} is not in S3. Process failed. "
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

        ##########################################
        # Extract the "Body" of S3 ASC PTB extract file. 
        ##########################################
        rootLogger.info("Extract Body/File Contents from S3 Dict object.") 

        txtExtractFile = txt_file["Body"].read()
        
        rootLogger.info(f"ASC PTB Extract EFT file size: {len(txtExtractFile)=}")


        ####################################################################
        # Load ASC PTB SF table with file in S3://EFT_Files 
        # Ex. P#EFT.ON.ASCPS.Y2023.MAR24.D240430.T0800051
        ####################################################################  
        rootLogger.info(f"Load SF table with ASC PTB Extract EFT file {sASC_Ext_Filename}") 

        # Get Ext data date
        idx = sASC_Ext_Filename.find("ASCPS.Y")
        if idx == -1:
            rootLogger.info(f"(EFT Filename {sASC_Ext_Filename} is not named properly. Missing ASCPB node. Process Failed.")
            
            ## Send Failure email	
            SUBJECT=f"ASC_PTB_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"EFT Filename {sASC_Ext_Filename} is not named properly. Missing ASCPS node. Process failed. "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
        else:
            sExtDtYYYY = sASC_Ext_Filename[(idx + 7) : (idx + 11) ]

        # Get timestamp
        idx = sASC_Ext_Filename.find(".D2")
        if idx == -1:
            rootLogger.info(f"(EFT Filename {sASC_Ext_Filename} is not named properly. Missing Timestamp node. Process Failed.")
            
            ## Send Failure email	
            SUBJECT=f"ASC_PTB_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"EFT Filename {sASC_Ext_Filename} is not named properly. Missing Timestamp node. Process Failed. "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
        else:
            sExtRunDtYYYYMMDD = "20" + sASC_Ext_Filename[(idx + 2) : (idx + 8) ]

        ####################################################################
        # Set environment variables to "pass" to ASC_PTB_SF_Table_Load.py 
        ####################################################################  
        rootLogger.info(f"{sExtRunDtYYYYMMDD=}")
        rootLogger.info(f"{sExtDtYYYY=}")
       
        os.environ["EXT_DT_YYYY"] = sExtDtYYYY
        os.environ["EXT_RUN_DT"] = sExtRunDtYYYYMMDD
        os.environ["EXT_FILENAME"] = sASC_Ext_Filename

        # Redirect stdout from import module call to our log file
        iosCaptureStdOut = io.StringIO()    
        sys.stdout = iosCaptureStdOut
        
        with contextlib.redirect_stdout(iosCaptureStdOut):
            ExtSQL.executeSFSQL()
            
        # send capture stdout messages to log file 
        #for line in iosCaptureStdOut.getValue().splitlines():
        #    rootLogger(line)
        rootLogger(iosCaptureStdOut.getValue() )
        iosCaptureStdOut.close()
        
        ####################################################################
        # Send success email 
        ####################################################################          
        SUBJECT=f"ASC_PTB_SF_Table_Load_Driver.py completed successfully. ({ENVNAME})"
        MSG = f"ASC_PTB_SF_Table_Load_Driver.py successfully loaded ASC PTB EFT Extract file {sASC_Ext_Filename} into SF."
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script ASC_PTB_SF_Table_Load_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in ASC_PTB_SF_Table_Load_Driver.py\n {e}")

        rootLogger.error("Exception occured in ASC_PTB_SF_Table_Load_Driver.py.")
        rootLogger.error(e)

        sys.exit(12)    

if __name__ == "__main__":
    
    main_processing_loop()