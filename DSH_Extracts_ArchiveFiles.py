#!/usr/bin/env python
############################################################################################################
# Script Name: DSH_Extracts_ArchiveFiles.py
#
# Description: This module will be run to archive DSH Extract files a day after being sent to requestor.
#              This is to limit the number of files in the s3 DSH/ folder since the ListFiles functionality 
#              will fail when there are more than 1,000 files in the DSH/ folder. Moving the already 
#              processed Extract files to the "DSH/archive/" folder will prevent this.
# 
# Execute as python3 SH_Extract_Archive_Files.py (processing stand-alone for already-processed EFT files).
# 
# Execute as python3 SH_Extract_Archive_Files $1 $2 (processing for DOJ EFT files) 
# 			$1 = override FROM DATE (YYYY-MM-DD) 
# 			$2 = override TO DATE (YYYY-MM-DD)
#
#
# Paul Baranoski 2026-03-26 Created script.
############################################################################################################

import boto3 
from botocore.exceptions import ClientError 
import logging
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess
from io import StringIO
import json

# Our common module with variable constants
from SET_XTR_ENV import *


# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import * 

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

        LOGNAME = f"{LOG_DIR}DSH_Extract_ArchiveFiles_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified 
        #        without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDSH_Extract_ArchiveFiles.py started at {TMSTMP}")

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
        if not (iNOFParms == 0 or iNOFParms == 2 ):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")


        #############################################################
        # Display parameters passed to script 
        #############################################################
        if iNOFParms == 2:
            lstParms = sys.argv
        
            sOverriderEFTFromDt = lstParms[1]
            sOverriderEFTToDt = lstParms[2]

            rootLogger.info(" ")
            rootLogger.info("Parameters to script: {iNOFParms} ")
            rootLogger.info(f"   {sOverriderEFTFromDt=} ")
            rootLogger.info(f"   {sOverriderEFTToDt=} ")


        #############################################################
        # SET Bucket variables 
        #############################################################
        # Set EFT File Source bucket--> Use override if exists, otherwise use default EFT_files bucket/folder
        if iNOFParms == 2:
            sEFTFromDt = sOverriderEFTFromDt
            sEFTToDt = sOverriderEFTToDt
        else:
            ##################################################################
            # Calculate a date range of the past week thru yesterday although
            # this job will be run daily. This should prevent any residual
            # files remaining when the job is not able to run on a particular 
            # day (e.g., RunDeck is down).            
            ##################################################################
            #sEFTFromDt = "2026-01-01"
            #sEFTToDt = "2026-01-07"
            sEFTFromDt = (date.today() + timedelta(days=-7)).strftime("%Y-%m-%d")
            sEFTToDt = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")


        #############################################################
        # Display variable values in log 
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"{XTR_BUCKET=}")
        rootLogger.info(f"{DSH_BUCKET_FLDR=}")

        rootLogger.info(f"{sEFTFromDt}=") 
        rootLogger.info(f"{sEFTToDt=}")

         
        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client/resource objects")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
 
 
        #################################################################################
        # Get list of EFT files to process.
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Build s3api command to query EFT_Files bucket for files between a date range.")

        """
        aws s3api list-objects-v2 --bucket "aws-hhs-cms-eadg-bia-ddom-extracts" --prefix xtr/EFT_Files/P#EFT.ON \
            --query "Contents[?LastModified>=\`2026-02-28T00:00:00\` && LastModified<=\`2026-03-06T23:59:59\`]" > EFT_Results.txt
            
            f"Contents[?contains(Key, '{timestamp}')].Key"   

        sXTR_BUCKET = 'aws-hhs-cms-eadg-bia-ddom-extracts'
        sEFT_FILEST_BUCKET_FLDR = 'xtr/EFT_Files/'
        """

        cmd = [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket", XTR_BUCKET,
            "--prefix", f"{DSH_BUCKET_FLDR}DSH_EXTRACT_",
            "--query", f"Contents[?LastModified>=`{sEFTFromDt}T00:00:00` && LastModified<=`{sEFTToDt}T23:59:59`]" 
        ]
        
        rootLogger.info(f"{cmd=}")

        rootLogger.info("")
        rootLogger.info("Execute subprocess s3api call to query DSH bucket for files between date range.")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # convert return code string to a list object
        lstResults = json.loads(result.stdout)

        rootLogger.info(f"{type(lstResults)=}")

        #################################################################################
        # Delete files returned from 
        #################################################################################
        if lstResults is None:
            rootLogger.info("")
            rootLogger.info("Send report email")

            SUBJECT = f"DSH Extract Files moved to archive folder ({ENVNAME})"
            MSG = f"There were no DSH Extract Files available to move to archive folder."
           
            try:
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info)
                
                sys.exit(0)
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error(e.output)

                sys.exit(12)  
            
        
        #################################################################################
        # Delete files returned from 
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Move delivered DSH Extract files to archive folder ")
                    
        #################################################################################
        # Loop thru list of EFT Files On-Hold.
        #################################################################################
        lstDSHFilenames = []
        
        for dictEntry in lstResults:

            rootLogger.info("")
            rootLogger.info("*********************")

            #############################################################################################
            # Get EFT filename info from query results 
            #############################################################################################
            rootLogger.info(f"{dictEntry=}")

            DSH_Extract_FilenameKey = os.path.basename(dictEntry["Key"])
            rootLogger.info(f"{DSH_Extract_FilenameKey=}")
            
            DSH_Extract_Filename = DSH_Extract_FilenameKey.replace(DSH_BUCKET_FLDR,"")
            rootLogger.info(f"{DSH_Extract_Filename=}")
            
            lstDSHFilenames.append(DSH_Extract_Filename)

            sDSH_Extract_FileCreateDt = dictEntry["LastModified"] [ : 10]
            rootLogger.info(f"{sDSH_Extract_FileCreateDt=}")
 
            s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{DSH_BUCKET_FLDR}{DSH_Extract_Filename}", f"{DSH_BUCKET_FLDR}archive/{DSH_Extract_Filename}") 


        #################################################################################
        # Success email
        #################################################################################
        # Set pointer to beginning of string
        rootLogger.info("")
        S3Files = "\n".join(lstDSHFilenames)
        rootLogger.info("DSH Extract Files to archive:\n%s", S3Files )
        
        rootLogger.info("")
        rootLogger.info("Send report email")

        SUBJECT = f"DSH Extract Files moved to archive folder ({ENVNAME})"
        MSG = f"DSH Extract Files moved to archive folder for period {sEFTFromDt} thru {sEFTToDt}: \n\n{S3Files}"
       
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)   
       

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("DSH_Extract_Archive_Files.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in DSH_Extract_Archive_Files.py\n {e}")

        rootLogger.error("Exception occured in DSH_Extract_Archive_Files.py.")
        rootLogger.error("\n%s", str(e))

        # Send Failure email	
        SUBJECT=f"DSH_Extract_Archive_Files.py - Failed ({ENVNAME})"
        MSG=str(e)
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()