#!/usr/bin/env python
########################################################################################################
# Name:  PSPS_SF_Table_Load_Driver.py
#
# Desc: Load PSPS Qtr extract files into SF table.
#           
#       Execute: python3 PSPS_SF_Table_Load_Driver.py --PSPS_Ext_Filename P#EFT.ON.PSPSQ5.DYYMMDD.THHMMSST 
#
#
# Author     : Paul Baranoski	
# Created    : 07/10/2025
#
# Paul Baranoski   2025-07-10 Create Module.
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
from SET_XTR_ENV import *


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Constants
#############################################################


#############################################################
# Functions
#############################################################
def setLogging():

    # Configure root logger
    #logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(funcName)-22s %(message)s",
        encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
        #filename=f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"
        handlers=[
        logging.FileHandler(f"{LOG_DIR}PSPS_SF_Table_Load_{TMSTMP}.log"),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)
 
    global rootLogger
    rootLogger = logging.getLogger() 
  
    os.chmod(LOG_DIR, 0o777)  # for Python3
    
    #logger.setLevel(logging.INFO)


def main_processing_loop():

    try:    

        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        print(f"{TMSTMP=}")

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        setLogging()
        rootLogger.info(f"\nPSPS_SF_Table_Load_Driver.py started at {TMSTMP}")
       
        ##########################################
        # Get any parameters
        ##########################################
        parser = argparse.ArgumentParser(description="PSPS SF Table load parms")
        parser.add_argument("--PSPS_Ext_Filename", help="PSPS Extract Filename to load into SF")
        args = parser.parse_args()

        global sPSPS_Ext_Filename 
        sPSPS_Ext_Filename = str(args.PSPS_Ext_Filename)

        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")
        
        ##########################################
        # Establish variables
        ##########################################
        sPSPSRec = ""
        lstPSPSOutputRecs = []

        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")
        
        ##########################################
        # Does SF Filename exist? 
        ##########################################
        rootLogger.info("Does PSPS extract file exist in S3?") 

        s3PSPSFolder_n_filename = f"{EFT_FILEST_BUCKET_FLDR}{sPSPS_Ext_Filename}"
        rootLogger.info(f"{s3PSPSFolder_n_filename=}")

        # Is PSPS file in s3?         
        resp = s3_client.list_objects_v2(Bucket=XTR_BUCKET, Prefix=s3PSPSFolder_n_filename)
        if resp == None:
            ## Send Failure email	
            SUBJECT=f"PSPS_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"PSPS Extract file {s3PSPSFolder_n_filename} is not in S3. Process failed. "
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info)     
            

        ##########################################
        # Get S3 EFT PSPS Extract File  
        ##########################################
        rootLogger.info("Get PSPS extract file from S3 - Dict Object") 
        
        txt_file = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3PSPSFolder_n_filename)

        if txt_file == None:
            ## Send Failure email	
            SUBJECT=f"PSPS_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"PSPS Extract file {s3PSPSFolder_n_filename} is not in S3. Process failed. "
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 

        ##########################################
        # Extract the "Body" of S3 PSPS extract file. 
        ##########################################
        rootLogger.info("Extract Body/File Contents from S3 Dict object.") 

        txtPSPSExtractFile = txt_file["Body"].read()
        
        rootLogger.info(f"PSPS Extract EFT file size: {len(txtPSPSExtractFile)=}")

        ###############################################################
        # Unzip/Decompress S3 gzip file - only the PSPS/archive .gz file
        # NOTE: Not necessary if using EFT file which is already unzipped.
        ###############################################################
        #rootLogger.info(f"Unzip/Decompress S3 gzip file {sPSPS_Ext_Filename} and convert to string " )
        
        #sPSPSExtractFile = gzip.decompress(gzPSPSExtractFile).encode("utf-8")
        #unzippedLen = len(sPSPSExtractFile)
        
        #rootLogger.info(f"(unzippedLen=}")

        ####################################################################
        # Put unzippped file into S3. 
        # Note: not necessary if we use the file in EFT_Files
        ####################################################################
        

        ####################################################################
        # Load PSPS SF table with file in S3://EFT_Files 
        ####################################################################  
        rootLogger.info(f"Load SF table with PSPS Extract EFT file {sPSPS_Ext_Filename}") 

        idx = sPSPS_Ext_Filename.find("PSPSQ")
        if idx == -1:
            rootLogger.info(f"(EFT Filename {sPSPS_Ext_Filename} is not named properly. Missing PSPSQ node. Process Failed.")
            
            ## Send Failure email	
            SUBJECT=f"PSPS_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"EFT Filename {sPSPS_Ext_Filename} is not named properly. Missing PSPSQ node. Process failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info) 
        else:
            sQtr = sPSPS_Ext_Filename[(idx + 4) : (idx + 6) ]

        idx = sPSPS_Ext_Filename.find(".D2")
        if idx == -1:
            rootLogger.info(f"(EFT Filename {sPSPS_Ext_Filename} is not named properly. Missing Timestamp node. Process Failed.")
            
            ## Send Failure email	
            SUBJECT=f"PSPS_SF_Table_Load_Driver.py  - Failed ({ENVNAME})"
            MSG=f"EFT Filename {sPSPS_Ext_Filename} is not named properly. Missing Timestamp node. Process Failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info)
        else:
            sRunDtYYYYMMDD = "20" + sPSPS_Ext_Filename[(idx + 2) : (idx + 8) ]
            sRunDtYYYY = sRunDtYYYYMMDD[:4]

        ####################################################################
        # Set environment variables to "pass" to PSPS_SF_Table_Load.py 
        ####################################################################  
        rootLogger.info(f"{sQtr=}")
        rootLogger.info(f"{sRunDtYYYYMMDD=}")
        rootLogger.info(f"{sRunDtYYYY=}")
       
        os.environ["PSPS_EXT_DT_YYYY"] = sRunDtYYYY
        os.environ["PSPS_EXT_QTR"] = sQtr
        os.environ["PSPS_EXT_RUN_DT"] = sRunDtYYYYMMDD
        os.environ["PSPS_EXT_FILENAME"] = sPSPS_Ext_Filename

        sp_info = subprocess.check_output(['python3', 'PSPS_SF_Table_Load.py'], text=True)
        rootLogger.info(sp_info)


        ####################################################################
        # Send success email 
        ####################################################################          
        SUBJECT=f"PSPS_SF_Table_Load_Driver.py completed successfully. ({ENVNAME})"
        MSG = f"PSPS_SF_Table_Load_Driver.py successfully loaded PSPS EFT Extract file {sPSPS_Ext_Filename} into SF."
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        rootLogger.info(sp_info)

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PSPS_SF_Table_Load_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in PSPS_SF_Table_Load_Driver.py\n {e}")

        rootLogger.error("Exception occured in PSPS_SF_Table_Load_Driver.py.")
        rootLogger.error(e)

        sys.exit(12)    

if __name__ == "__main__":
    
    main_processing_loop()