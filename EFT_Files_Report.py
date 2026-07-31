#!/usr/bin/env python
############################################################################################################
# Script Name: EFT_Files_Report.py
#
# Description: This script can be run stand-alone and will report on the EFT_Files/extract files recently processed.
# 
# Execute as python3 EFTFileReport.py (processing stand-alone for already-processed EFT files).
# 
# Execute as python3 EFTFileReport.py $1 $2 (processing for DOJ EFT files) 
# 			$1 = override FROM DATE (YYYY-MM-DD) 
# 			$2 = override TO DATE (YYYY-MM-DD)
#
#
# Paul Baranoski 2026-03-13 Created script.
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


def s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey):

    # Copy object, then delete to "move" file.
    
    s3_client.copy_object(
        Bucket=sSourceBucket,
        CopySource={"Bucket": sSourceBucket, "Key": sSourceKey},
        Key=sDestinationKey
    )

    s3_client.delete_object(Bucket=sSourceBucket, Key=sSourceKey)


# bytes pretty-printing
UNITS_MAPPING = [
    (1<<50, ' PB'),
    (1<<40, ' TB'),
    (1<<30, ' GB'),
    (1<<20, ' MB'),
    (1<<10, ' KB'),
    (1, (' byte', ' bytes')),
]

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

        LOGNAME = f"{LOG_DIR}EFT_Files_Report_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified 
        #        without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nEFT_Files_Report_Driver.py started at {TMSTMP}")

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
            # Calculate the last week's date range
            #sEFTFromDt = "2026-01-01"
            #sEFTToDt = "2026-01-07"
            sEFTFromDt = (date.today() + timedelta(days=-7)).strftime("%Y-%m-%d")
            sEFTToDt = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")


        #############################################################
        # Display variable values in log 
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"{XTR_BUCKET=}")
        rootLogger.info(f"{EFT_FILEST_BUCKET_FLDR=}")

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
            "--prefix", f"{EFT_FILEST_BUCKET_FLDR}P#EFT.ON",
            "--query", f"Contents[?LastModified>=`{sEFTFromDt}T00:00:00` && LastModified<=`{sEFTToDt}T23:59:59`]" 
        ]
        
        rootLogger.info(f"{cmd=}")

        rootLogger.info("")
        rootLogger.info("Execute subprocess s3api call to query EFT_Files bucket for files between date range.")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # convert return code string to a list object
        lstResults = json.loads(result.stdout)

        rootLogger.info(f"{type(lstResults)=}")
        
        
        #################################################################################
        # Create Report
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Create EFT files report ")
        
        sioHTMLReport = StringIO("")
                    
        #################################################################################
        # Write out HTML header.
        #################################################################################
        sioHTMLReport.write("<html><body><table cellspacing='1px' border='1' > \n")
        sioHTMLReport.write("<tr bgcolor='#00B0F0'><th>EFT Filename</th> <th>Creation Date</th><th>EFT file size</th></tr> \n") 	
    
        #################################################################################
        # Process list of EFT files 
        #################################################################################            
        rootLogger.info("")
        rootLogger.info("Process list of EFT files to build Report Rows ")

        #################################################################################
        # Loop thru list of EFT Files On-Hold.
        #################################################################################
        for dictEntry in lstResults:

            rootLogger.info("")
            rootLogger.info("*********************")

            #############################################################################################
            # Get EFT filename info from query results 
            #############################################################################################
            rootLogger.info(f"{dictEntry=}")

            sEFT_Filename = os.path.basename(dictEntry["Key"])
            rootLogger.info(f"{sEFT_Filename=}")

            sFileSize = f'{int(dictEntry["Size"]):,}' 
            rootLogger.info(f"{sFileSize=}")
            sHumanFileSize = convertBytes2ReadableSize(int(dictEntry["Size"]), units=UNITS_MAPPING)
            rootLogger.info(f"{sHumanFileSize=}")


            sEFT_FileCreateDt = dictEntry["LastModified"] [ : 10]
            rootLogger.info(f"{sEFT_FileCreateDt=}")
            
            #############################################################################################
            # Build Report Rows
            #############################################################################################
            sioHTMLReport.write(f"<tr><td>{sEFT_Filename}</td><td>{sEFT_FileCreateDt}</td><td>{sHumanFileSize}</td></tr> \n") 	


        #################################################################################
        # Write out HTML trailer.
        #################################################################################
        sioHTMLReport.write("</table></body></html>")


        #################################################################################
        # Email report
        #################################################################################
        # Set pointer to beginning of string
        sioHTMLReport.seek(0)
        RPT_INFO = sioHTMLReport.read()
        
        rootLogger.info("")
        rootLogger.info("\n%s", RPT_INFO)
        
        rootLogger.info("")
        rootLogger.info("Send report email")

        SUBJECT = f"EFT Files Processed Report ({ENVNAME})"
        MSG = f"EFT files processed for period {sEFTFromDt} thru {sEFTToDt} . . .<br><br>{RPT_INFO}"
       
        try:
            sp_info = subprocess.run(['python3', 'sendEmailHTML.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
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
        rootLogger.info("EFTFileReport.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in EFTFileReport.py\n {e}")

        rootLogger.error("Exception occured in EFTFileReport.py.")
        rootLogger.error("\n%s", str(e))

        # Send Failure email	
        SUBJECT=f"EFTFileReport.py - Failed ({ENVNAME})"
        MSG=str(e)
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()