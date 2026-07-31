#!/usr/bin/env python
############################################################################################################
# Script Name: ManifestFileReport.py
#
# Description: This script can be run stand-alone and will report on the manifest files/extract files recently processed,
#              and move the "processed" manifest_files to the s3://manifest_files_archive folder.
# 
#              This script can also be run as a called script to report on DOJ manifest files that are in hold status. 
#              The DOJ manifest files are used for reporting purposes only, and are not used to trigger a extract file 
#              delivery process.
#
#              The default ManifestSourceBucketFldr = manifest_files.
#              For DOJ reporting, an override manifest_file folder can be supplied (e.g., aws-hhs-cms-eadg-bia-ddom-extracts/xtr/manifest_files_hold ),
#              and also a manifest file HLQ to process a specific subset of the manifest files in the on-hold folder.
#
# Execute as python3 ManifestFileReport.py (processing stand-alone for already-processed manifest files).
# 
# Execute as python3 ManifestFileReport.py $1 $2 (processing for DOJ manifest files) 
# 			$1 = Manifest Files Source bucket-folder   Ex: xtr/manifest_files_hold 
# 			$2 = ${ManifestFileHLQ}
#
#
# Paul Baranoski 2023-12-01 Created script.
# Paul Baranoski 2023-12-22 Add manifest file migration from s3://manifest_files to manifest_files_archive.
# Paul Baranoski 2024-01-09 Add functionality to allow this script to be called by other scripts and: 
#                        	1) pass in an override S3 manifest folder to process
#                           2) pass in a manifest file HLQ to process a subset of files
# Paul Baranoski 2024-01-16 Add code to remove temporary directory before test to create it if it isn't there.
#                           Remove aws cp --recursive since it is copying from sub-directories which is not what is intended.
# Paul Baranoski 2025-12-12 Convert from bash to python.
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

        LOGNAME = f"{LOG_DIR}ManifestFileReport_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nManifestFileReport_Driver.py started at {TMSTMP}")

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
        
            ManifestSourceBucketFldrOverride = lstParms[1]
            ManifestFileHLQOverride = lstParms[2]

            rootLogger.info(" ")
            rootLogger.info("Parameters to script: {iNOFParms} ")
            rootLogger.info(f"   {ManifestSourceBucketFldrOverride=} ")
            rootLogger.info(f"   {ManifestFileHLQOverride=} ")


        #############################################################
        # SET Bucket variables 
        #############################################################
        # Set Manifest File Source bucket--> Use override if exists, otherwise use default manifest_files bucket/folder
        if iNOFParms == 2:
            ManifestSourceBucketFldr = ManifestSourceBucketFldrOverride
            ManifestFileHLQ = ManifestFileHLQOverride
        else:
            ManifestSourceBucketFldr = MANIFEST_BUCKET_FLDR
            ManifestFileHLQ = ""


        rootLogger.info("")
        rootLogger.info(f"{XTR_BUCKET=}")
        rootLogger.info(f"{MANIFEST_ARCHIVE_BUCKET_FLDR=}")

        rootLogger.info(f"{ManifestSourceBucketFldr}=") 
        rootLogger.info(f"{ManifestFileHLQ=}")

         
        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client/resource objects")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
 
 
        #################################################################################
        # Get list of manifest files to process.
        #################################################################################
        rootLogger.info("")
        rootLogger.info(f"Get list of Manifest files on hold in s3 bucket/folder {ManifestSourceBucketFldr}{ManifestFileHLQ} ") 

        # Get list of Extract filenames (with folder path) that area ONLY under the requested path. No "archive" folder filenames. No folder without ext filename: "xtr/PSPS/"
        lsS3ManifestPrefix = ManifestSourceBucketFldr + ManifestFileHLQ
        
        lstManifestFileKeys = [ obj.key for obj in s3_resource.Bucket(XTR_BUCKET).objects.filter(Prefix=lsS3ManifestPrefix)  if not obj.key.endswith("/") ]
        rootLogger.info("lstManifestFileKeys:\n" + "\n".join(lstManifestFileKeys))
       
        NOF_FILES = len(lstManifestFileKeys)
        rootLogger.info(f"{NOF_FILES} manifest files were found to process")

        if NOF_FILES == 0:
            # No files to process
            rootLogger.info("")
            rootLogger.info(f"No manifest files to report on in {ManifestSourceBucketFldr}{ManifestFileHLQ} ")

            ## Send Failure email	
            SUBJECT = f"ManifestFileReport.py - No manifest files to process. ({ENVNAME})"
            MSG = f"No manifest files to report on in {ManifestSourceBucketFldr}{ManifestFileHLQ} "
            
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 
           
            sys.exit(0)	
          

        #################################################################################
        # Create Report
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Create manifest files report ")
        
        sioHTMLReport = StringIO("")
                    
        #################################################################################
        # Write out HTML header.
        #################################################################################
        sioHTMLReport.write("<html><body><table cellspacing='1px' border='1' > \n")
        sioHTMLReport.write("<tr bgcolor='#00B0F0'><th>Data Request ID</th><th>Manifest Filename</th> <th>Extract filename</th><th>Extract file size</th></tr> \n") 	
    
        #################################################################################
        # Process list of manifest files 
        #################################################################################            
        rootLogger.info("")
        rootLogger.info("Process list of manifest files to build Report Rows ")

        #################################################################################
        # Loop thru list of Manifest Files On-Hold.
        #################################################################################
        for ManifestFile2Process in lstManifestFileKeys:

            rootLogger.info("")
            rootLogger.info("*********************")
            rootLogger.info(f"{ManifestFile2Process=}")

            #############################################################################################
            # Get manifest file from S3 
            #############################################################################################
            rootLogger.info(f"Make s3_client.get_object call")
            resp = s3_client.get_object(Bucket=XTR_BUCKET, Key=ManifestFile2Process)

            if resp == None:
                rootLogger.info("")
                rootLogger.info(f"ManifestFileReport.py failed on get_object on file {ManifestFile2Process}.")
                
                ## Send Failure email	
                SUBJECT = f"ManifestFileReport.py Failed ({ENVNAME})"
                MSG = f"Getting manifest file {{ManifestFile2Process}} from S3 has failed. "
                
                #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                rootLogger.info(sp_info) 
                
                sys.exit(12)

            # Manifest s3 Obj
            rootLogger.info(resp)
            
            # Extract the "Body" which is the actual file contents. Convert from array of bytes to String
            sManifestFile = resp["Body"].read().decode('utf-8')
            
            # preserve new lines for better readability in log file
            rootLogger.info("\n%s", sManifestFile)
            
            # Convert string to Dict to make it easier to access the info
            dctManifestFile = json.loads(sManifestFile)
            
            #############################################################################################
            # Build Report Rows
            #############################################################################################
            manifestFilename = os.path.basename(ManifestFile2Process)

            sDataRequestID =  dctManifestFile["shareDetails"]["dataRequestID"]
            rootLogger.info(f"{sDataRequestID=}")

            lstFiles = dctManifestFile["fileInformation"]
            for dctExtFile in lstFiles:
                s3ExtractFilename = dctExtFile["fileName"]
                s3ExtractBucketFolder = dctExtFile["fileLocation"]
                
                s3ExtFilenameKey = s3ExtractBucketFolder + s3ExtractFilename    

                # Get File size if available
                try:
                    resp =  s3_client.head_object(Bucket=XTR_BUCKET, Key=s3ExtFilenameKey)
                    rootLogger.info(f"{resp=}")
                    sContentLength = resp["ContentLength"]
                    sExtFileSize = convertBytes2ReadableSize(int(sContentLength), units=UNITS_MAPPING)
                    
                    # resp.get like COALESCE("StorageClass","STANDARD") 
                    sStorageClass = resp.get("StorageClass","STANDARD")

                except ClientError as e:
                    rootLogger.info(f"s3 Extract file {s3ExtFilenameKey} notfnd in s3. Setting file size = blank")
                    sExtFileSize = ""

                # Display file information
                rootLogger.info(f"{s3ExtractFilename=}")
                rootLogger.info(f"{sExtFileSize=}")
                    
                sioHTMLReport.write(f"<tr><td>{sDataRequestID}</td><td>{manifestFilename}</td><td>{s3ExtractFilename}</td><td></td>{sExtFileSize}</tr> \n") 	


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

        SUBJECT = f"Manifest Files Processed Report ({ENVNAME})"
        MSG = f"Manifest Files Processed Report has completed.<br><br>The manifest files processed . . .<br><br>{RPT_INFO}"
       
        try:
            sp_info = subprocess.run(['python3', 'sendEmailHTML.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)   
       

        #############################################################
        # Move processed manifest files to archive folder.
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Move processed manifest files from s3://manifest_files folder to s3://manifest_files_archive folder")  

            
        for ManifestFileKey in lstManifestFileKeys:
        # ManifestFile2Process = "xtr/DEV/manifest_files/DSH_Manifest_20240710.141624.json"

            rootLogger.info("")
            rootLogger.info(f"{ManifestFileKey=}")
            
            manifestFilename = os.path.basename(ManifestFileKey)
            sDestinationKey = MANIFEST_ARCHIVE_BUCKET_FLDR + manifestFilename
            
            rootLogger.info(f"{manifestFilename=}")
            rootLogger.info(f"{sDestinationKey=}")

            try:                
                # move manifest file from manifest_files bucket/folder to manifest_files_archive bucket/folder
                s3MoveFile2NewFolder(s3_client, XTR_BUCKET, ManifestFileKey, sDestinationKey)
                    
            except Exception as e:
            
                SUBJECT = f"Manifest Files Processed Report ({ENVNAME})"
                MSG = "Moving manifest file {manifestFilename} from {ManifestFileKey} to {MANIFEST_ARCHIVE_BUCKET_FLDR} failed.\n em.output) "

                sp_info = subprocess.run(['python3', 'sendEmailHTML.py', CMS_EMAIL_SENDER, ENIGA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info)
                
                # Re-raise exception
                raise

        
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("ManifestFileReport.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in ManifestFileReport.py\n {e}")

        rootLogger.error("Exception occured in ManifestFileReport.py.")
        rootLogger.error("\n%s", str(e))

        # Send Failure email	
        SUBJECT=f"ManifestFileReport.py - Failed ({ENVNAME})"
        MSG=str(e)
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 
        
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()