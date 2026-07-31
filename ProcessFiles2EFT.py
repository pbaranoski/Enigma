#!/usr/bin/env python
#
######################################################################################
# Name: ProcessFiles2EFT.py
# Desc: Unzip extract files, rename them as P#EFT.ON.*, and place them in S3 EFT_Files
#       folder.
#
# Execute as ./ProcessFiles2EFT.py $1 $2 (Optional) 
#            $1 = Extract S3 folder 
#                 (Note: Does not include the full path when run from RunDeck).
#            $2 = S3 Destination folder (optional) default: EFT_Files   
#
# Ex. 1: python3 ProcessFiles2EFT.py Blbtn/                     (Default is EFT_Files/) 
# Ex. 2: python3 ProcessFiles2EFT.py Blbtn/ EFT_Files_Archive/ 
#
# Created: Paul Baranoski  04/11/2023
#
# Modified: 
#
# Paul Baranoski 2023-04-11 Created script.
# Paul Baranoski 2023-04-18 Add '1' to end of time in EFT filename
#                           Modify script to accept S3 Extract folder as optional parameter
#                           to script. Script will use that parameter if it is included, 
#                           and parameter file if it is not included.
#                           Add code to clean config Xref file of any CR (\r) characters.
# Paul Baranoski 2023-05-09 Added code to be able to find the actual substitution token
#                           offset when the token contained hard-coded leading characters. 
#                           Ex. Finding token "PR{YY}" was able to extract {YY} as actual 
#                               substitution token.
# Paul Baranoski 2023-05-10 Added ability to count XREF file matches. Re-worked if statement
#                           to handle more conditions.
# Paul Baranoski 2023-05-12 Added code to check return_status after call to CombineS3Files.sh.
# Paul Baranoski 2023-05-15 Change code to remove '\r' to use sed -i command.
# Paul Baranoski 2023-05-16 Modify grep -bo '{' code to only look at first occurrence.
# Paul Baranoski 2023-06-02 Add code to verify that MF_FILENAME is a valid length.
# Paul Baranoski 2023-06-06 Add code to handle SF suffix files Ex. filename.txt-0, filename.txt-1
# Paul Baranoski 2023-06-07 Make some modifications to the suffix files logic after testing in prod.
# Paul Baranoski 2023-06-08 Correct syntax "if [ ${sfx_num} -gt 9]" to if [ ${sfx_num} -gt 9 ]" which was causing 
#                           error ->  [: missing `]
#                           Remove duplicate edit for filename length. (How did that happen?)
# Paul Baranoski 2023-06-16 Modify TMSTMP variable to use current value if it exists. This will help to group
#                           log files from same run together since they will all have the same timestamp.
# Paul Baranoski 2023-06-21 Revamped script to remove call to python code to unzip compressed extract file.
#                           Instead, the script downloads the file to linux, unzips there, and moves/renames file
#                           from linux to S3.
# Paul Baranoski 2023-06-23 When getting S3 ls of files, added grep -v to exclude "parts" files.
# Paul Baranoski 2023-06-27 Add sed command to convert 2-byte encoded characters to space. These 2-byte encoded characters
#                           were causing EFT issues, and are from "bad" binary data contained in Teradata and SF databases.
#                           Add edit to force script to end if NOF characters and NOF bytes are not equal.
# Paul Baranoski 2023-07-21 Modified sed command to convert 2-byte encoded characters to space to handle all non-UTF-8/ASCII
#                           characters 2-byte encoded characters instead of one particular instance. (After additional 2-byte
#                           characters were found).
# Paul Baranoski 2023-09-21 Modified code to move EFT file to S3 EFT_Files folder. Only performed move if EFT file HLQ was P#EFT  
#                           or T#EFT.
# Paul Baranoski 2024-01-09 Modified script to no longer use config file to get S3ExtractFolder param. RunDeck will instead pass the parameter. 
#                           Added ability to accept an over-ride S3 EFT Destination folder. 
#                           ADD SSA-RDATE Key with special processing.
# Paul Baranoski 2024-01-26 Modified script to convert double back-slashes to single backslash. If there is a back-slash in the data
#                           snowflake convert to double back-slash because the back-slash is an escape character. The reason
#                           for the change is that the extrac back-slash incorrectly increases the LRECL, causing failure of the file
#                           to EFT. 
# Paul Baranoski 2024-02-01 Add ENVNAME to SUBJECT for all emails.  
# Paul Baranoski 2024-02-27 Comment out code to convert double back-slashes to single backslash to resolve SAF ENC OPT issue '\\T' in data
#                           which causes mis-aligned data.         
# Paul Baranoski 2024-03-25 Add echo "FINAL MF_FILENAME=${MF_FILENAME}" to make it easier to find EFT filenames for SFTP processes to display in emails. 
# Paul Baranoski 2024-05-06 Add logic to determine if file is a SAS/binary file, and bypass specific text file data verfication logic. Add logic to
#                           remove SAS file extension from filename for EFT filename conversion. 
# Paul Baranoski 2024-06-13 Add code to remove SAS file extension from SF_FILENAME variable. 
# Paul Baranoski 2025-07-18 Add -f flag to gzip command to force replacement of unzipped file if still on server. 
# Paul Baranoski 2025-11-07 Convert from bash to python.
# Paul Baranoski 2026-01-13 Add logic to retrieve "TESTING" environment variable. Modify If statment to only load EFT Files to s3://EFT_FILES
#                           when the HLQ in ("P#EFT,T#EFT,MNUP) AND swTESTING=N.
######################################################################################
import os
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta

import subprocess

import io
import re
import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import gzip
import shutil
import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data"

# Our common module with variable constants
from SET_XTR_ENV import *

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import * 

DATADIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Functions
#############################################################
def cleanseExtFile(sPathNFilename):

    rootLogger.info("")
    rootLogger.info(f"Cleanse extract file {sPathNFilename} on linux ")
    
    # Replace double-byte UTF-8 character to single byte space characater
    # Need to maintaine LRECL count for EFT to send file successfully to MF
    subprocess.run(["sed", "-i", "s/[\x80-\xff][\x80-\xff]/ /g", sPathNFilename ],env={"LC_ALL": "C"}  )

    
def main_processing_loop():

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        global rootLogger

        #TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
        TMSTMP = os.environ.get('TMSTMP', datetime.now().strftime('%Y%m%d.%H%M%S'))
        print(f"{TMSTMP=}")

        swTESTING = os.getenv("TESTING","N") 
        
        LOGNAME = f"{LOG_DIR}ProcessFiles2EFT_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nProcessFiles2EFT.py started at {TMSTMP}")
        
        rootLogger.info(f"{swTESTING=}")

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(rootLogger)
 
        ###########################################################
        # Set current working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")
       
        ##########################################
        # Were the correct NOF parameters sent?
        ##########################################
        iNOFParms = len(sys.argv) - 1
        if not (iNOFParms == 1 or iNOFParms ==  2):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} parameters to script.")

        #############################################################
        # Display parameters passed to script 
        #############################################################
        rootLogger.info("")
        rootLogger.info("Getting parameters... ")
        
        lstParms = sys.argv

        if iNOFParms == 1:
            S3ParmExtractFolder = lstParms[1]
            # There is no EFT override folder
            S3ParmEFTDestFolder = None
            
        elif iNOFParms == 2:
            S3ParmExtractFolder = lstParms[1]
            S3ParmEFTDestFolder = lstParms[2]
            rootLogger.info(f"EFT Destination folder is being overriden. New destination is {S3ParmEFTDestFolder}. ")

       
        S3_EFT_DESTINATION_FLDR =  "EFT_Files/" if  S3ParmEFTDestFolder is None else S3ParmEFTDestFolder  
        rootLogger.info(f"EFT Destination folder is {S3_EFT_DESTINATION_FLDR}")
        
        #############################################################
        # Get Bucket variables needed.
        # S3 Bucket = "aws-hhs-cms-eadg-bia-ddom-extracts" 
        # S3S3HLFolder = "xtr/" or "xtr/DEV/" 
        #############################################################
        S3Bucket = XTR_BUCKET 
        S3HLFolder = bucket_fldr
      
        rootLogger.info(f"{S3Bucket=}")
        rootLogger.info(f"{S3HLFolder=}")

        ###########################################################################
        # NOTE: When called by other extract modules --> S3ParmExtractFolder = Full S3 Path   
        #            
        # Ex. Full path: S3ParmExtractFolder = "aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DEV/PTDDualMnth/"
        #
        # NOTE: When run stand-alone from RunDeck --> S3ParmExtractFolder = Ext Folder only 
        #
        # Ex. Ext S3 Folder only: 3ParmExtractFolder = "PTDDualMnth/"
        #
        ###########################################################################
        rootLogger.info("")
        rootLogger.info("Determine if param Extract folder contains full S3 path")
        
        # Does parm contain full S3 bucket/folder path?
        if S3ParmExtractFolder.find(XTR_BUCKET) >= 0:

            rootLogger.info("Folder path parm contains full S3 path")
        
            # Split Full S3 bucket/folder into parts
            lstBktFldParts = S3ParmExtractFolder.split("/")

            # NOTE: The last item in list should be empty string. We want the next to last node to get the Ext Folder.
            # Ex. PTDDualMnth + "/"
            S3ExtractFolder = str(lstBktFldParts[-2]) + "/"

        else:
            rootLogger.info("Folder path parm contains only s3 Extract folder.")
            
            # Executed stand-alone by RunDeck. Should already have the ending slash.
            S3ExtractFolder = S3ParmExtractFolder	

        rootLogger.info(f"{S3ExtractFolder=}")

        
        #############################################################
        # Set S3 extract bucket/folder to process
        #############################################################
        S3ExtKeyFldr2Process = f"{S3HLFolder}{S3ExtractFolder}" 
        rootLogger.info(f"{S3ExtKeyFldr2Process=}")
        
        S3ExtBktNFldr2Process = f"{XTR_BUCKET}/{S3HLFolder}{S3ParmExtractFolder}" 
        rootLogger.info(f"{S3ExtBktNFldr2Process=}")


        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
        
        #############################################################
        # Get a list of all files in S3 bucket/folder - NOTE: we don't have a file Prefix
        # NOTE: 1) obj.key.count("/") == S3ExtKeyFldr2Process.count("/")  --> Does NOT retrieve any files in sub-folders like "archive"
        #       2) Do not include "parts" files like 'xtr/Blbtn/blbtn_clm_ext_20250804.080202.txt.gz_0_0_0.csv.gz'
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get S3 Extract filenames to EFT in s3 bucket/folder {S3ExtBktNFldr2Process} ")

        ##############################################################################
        # Get list of all Extract files in s3 folder path - We do NOT know prefix.
        # We don't wnat any files under the "archive" folder filenames. 
        # Ex. "xtr/PSPS/"
        ##############################################################################
        lstKeys = [ obj.key for obj in s3_resource.Bucket(XTR_BUCKET).objects.filter(Prefix=S3ExtKeyFldr2Process) if obj.key.count("/") == S3ExtKeyFldr2Process.count("/") and obj.key != S3ExtKeyFldr2Process]
        rootLogger.info("lstKeys:\n" + "\n".join(lstKeys))

        ##############################################################################
        # Verify that none of the filenames are "parts" files.
        # Ex. 'xtr/Blbtn/blbtn_clm_ext_20250804.080202.txt.gz_0_0_0.csv.gz'
        ##############################################################################
        lstKeysFinal = [ filename for filename in lstKeys if not re.search(r'_[0-9]{1,2}_[0-9]{1,2}_[0-9]{1,2}\.',filename)  ]          
        rootLogger.info("lstKeysFinal:\n" + "\n".join(lstKeysFinal))

        ##############################################################################
        # There are no files to EFT in S3 Extract folder 
        ##############################################################################
        if len(lstKeysFinal) == 0:
            rootLogger.info(f"No files to process in {S3ExtBktNFldr2Process} ")
            sys.exit(0)

        # Assign to variable to be processed.
        lstExtractFileKeys2EFT = lstKeysFinal


        #############################################################
        # Download configuration file EFT_SF_2_MF_XREF.txt
        #############################################################
        rootLogger.info("")
        rootLogger.info("Retrieve EFT_SF_2_MF_XREF configuration file from S3")

        # Configuration filename in S3
        EFT_SF_2_MF_XREF_FILE = f"EFT_SF_2_MF_XREF_{ENVNAME}.txt" 
        
        s3ConfigFolder_n_filename = f"{CONFIG_BUCKET_FLDR}{EFT_SF_2_MF_XREF_FILE}"
        
        lstEFTConfigRecs = getConfigFile(s3_client, XTR_BUCKET, s3ConfigFolder_n_filename)
        
        
        #############################################################
        # Loop thru each file in lstExtractFiles2EFT
        #
        # NOTE!!! We assume that we will not be processing "parts" files
        #############################################################
        for gz_ExtractFileKey in lstExtractFileKeys2EFT: 

            rootLogger.info("")
            rootLogger.info("*****************************************************************")
            rootLogger.info(f"{gz_ExtractFileKey=}")

            #############################################################
            # Build config file key to convert to EFT filename
            #############################################################
            # Remove file path to get gz_filename. Get last "node" which contains filename by itself.
            gz_filename = gz_ExtractFileKey.split("/")[-1] 
            rootLogger.info(f"{gz_filename=}")
           
            #############################################################
            # Download S3 compressed file to linux
            #############################################################
            downloadFileFromS3(s3_client, XTR_BUCKET, gz_ExtractFileKey, f"{DATADIR}{gz_filename}")

            #############################################################
            # unzip gz file on linux
            #############################################################
            txt_filename = unzipFile(DATADIR, gz_filename)

            #############################################################
            # Convert bad binary data x'c28d' to spaces.
            # Other two-byte characters: x'c39b', x'c386', x'c384'
            # UTF-8/ASCII characters are x'00' thru x'7f'
            #############################################################
            cleanseExtFile(f"{DATADIR}{txt_filename}")

            #######################################################################################
            # Verify that byte-count and char-count are same. Unncessary with new cleanse logic.
            #######################################################################################
            byte_count, char_count = wc_cm_largefile(f"{DATADIR}{txt_filename}")

            if byte_count !=  char_count:
                rootLogger.info("")
                rootLogger.info(f"ProcessFiles2EFT.py failed. Could not convert all multi-byte characters for s3 file {gz_ExtractFileKey}. {byte_count=} {char_count=} ")

                # remove file from linux
                os.remove(f"{DATADIR}{txt_filename}")
            
                ## Send Failure email	
                SUBJECT=f"ProcessFiles2EFT.py - Failed ({ENVNAME})"
                MSG=f"ProcessFiles2EFT.py failed. Could not convert all multi-byte characters for s3 file {gz_ExtractFileKey}. {byte_count=} {char_count=} "
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
                
                sys.exit(12)
            
            
            ###################################################################
            # Find Key/Value file mapping record
            ###################################################################
            rootLogger.info("")
            rootLogger.info("Build MF EFT file name for SF file ")

            # Build search keys for EFT config file
            SEARCH_1NODE  = txt_filename.split("_") [0]  	
            SEARCH_2NODES = '_'.join(txt_filename.split("_") [0:2])  

            # Extract appropriate key/value record from config file
            lstMatches = [ EFTConfigRec for EFTConfigRec in lstEFTConfigRecs if re.search(f'^{SEARCH_1NODE}',EFTConfigRec) ]
            
            NOF_SF2MF_KEY_VALUE_MATCHES = len(lstMatches)
            rootLogger.info(f"{NOF_SF2MF_KEY_VALUE_MATCHES=}")
            
            if NOF_SF2MF_KEY_VALUE_MATCHES == 0:
                rootLogger.info("")
                rootLogger.info("ProcessFiles2EFT.py failed")
                
                # Send Failure email	
                SUBJECT=f"ProcessFiles2EFT.py - Failed ({ENVNAME})"
                MSG=f"ProcessFiles2EFT.py has failed. Could not find matching SF2MF Key/value record for key={SEARCH_1NODE}"
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)	
                
            elif NOF_SF2MF_KEY_VALUE_MATCHES == 1:	
                SF2MF_KEY_VALUE_PAIR = lstMatches[0]
                rootLogger.info(f"{SF2MF_KEY_VALUE_PAIR=}")

            elif NOF_SF2MF_KEY_VALUE_MATCHES >= 2:
                # Extract appropriate key/value record from config file
                lstMatches = [ EFTConfigRec for EFTConfigRec in lstEFTConfigRecs if re.search(f'^{SEARCH_2NODES}',EFTConfigRec) ]

                NOF_SF2MF_KEY_VALUE_MATCHES = len(lstMatches)
                rootLogger.info(f"{NOF_SF2MF_KEY_VALUE_MATCHES=}")
                
                if NOF_SF2MF_KEY_VALUE_MATCHES == 1:	
                    SF2MF_KEY_VALUE_PAIR = lstMatches[0]
                    rootLogger.info(f"{SF2MF_KEY_VALUE_PAIR=}")

                else:
                    rootLogger.info("")
                    rootLogger.info(f"Found {NOF_SF2MF_KEY_VALUE_MATCHES} matching (too many or not any) SF2MF Key/value records for key={SEARCH_2NODE}")
                    
                    # Send Failure email	
                    SUBJECT=f"ProcessFiles2EFT.py - Failed ({ENVNAME})"
                    MSG="ProcessFiles2EFT.py has failed. Found ${NOF_SF2MF_KEY_VALUE_MATCHES} matching (too many) SF2MF Key/value records for key=${SEARCH_2NODE} "
                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info) 

                    sys.exit(12)	

            
            ############################################################
            # Extract SF and MF file masks from config record; 
            #   1) remove file extension 
            #   2) change '_' to ' ' to make it easier to see array elements 
            #
            # NOTE: "=" separates key and value parts
            ############################################################
            SF_FILE_MASK = str(SF2MF_KEY_VALUE_PAIR.split("=")[0]).replace(".txt","").replace(".csv","").replace("_"," ")
            SF_FILENAME = txt_filename.replace(".txt","").replace(".txt","").replace(".csv","").replace("_"," ")

            MF_FILE_MASK = (SF2MF_KEY_VALUE_PAIR.split("=")[1]).replace("_"," ")
            MF_FILENAME = MF_FILE_MASK

            rootLogger.info(f"{MF_FILE_MASK=}")
            rootLogger.info(f"SF_FILE_MASK Array = {SF_FILE_MASK}" )
            rootLogger.info(f"SF_FILENAME Array = {SF_FILENAME}") 

            # Create array of tokens from filenames
            SF_FILEMASK_ARRAY = SF_FILE_MASK.split(" ")
            SF_FILENAME_ARRAY = SF_FILENAME.split(" ")

            rootLogger.info("")
            rootLogger.info("Parse SF filename mask nodes")

            
            for i, sNode in enumerate(SF_FILEMASK_ARRAY):
                rootLogger.info("")
                rootLogger.info(f"{i} = {sNode}")

                # This is a replacement token  
                if sNode.find("{") >= 0:
                    key = SF_FILEMASK_ARRAY [i] 
                    value = SF_FILENAME_ARRAY [i] if len(SF_FILENAME_ARRAY) > i else "" 
                    
                    if sNode == "{TIMESTAMP}":
                        YYMMDD = value [2:8]
                        HHMMSS = value [9:15]
                        # EFT transer process needs Time node to have 7 digits -- add "1" after time
                        value=f"D{YYMMDD}.T{HHMMSS}1"
                        rootLogger.info(f"valueTM={value}")
                        
                    elif sNode == "{SSA-RDATE}":
                        YYMMDD = value [2:8]
                        HHMMSS = value [9:15]
                        # SSA needs RDATE with no time component
                        value=f"R{YYMMDD}.T{HHMMSS}"
                        rootLogger.info(f"valueRDT={value}")

                    else:
                        # calculate offset if there are leading characters before substitution token
                        offset = key.index("{")
                        rootLogger.info(f"{offset=}")
                        
                        key = key [offset : ]
                        value = value [offset : ]
                
                    rootLogger.info(f"{key} replaced by {value} ")
                    
                    MF_FILENAME = MF_FILENAME.replace(key, value)
                    rootLogger.info(f"{MF_FILENAME=}")

            # end-for
            rootLogger.info(f"FINAL MF_FILENAME={MF_FILENAME}")   
  
                    
            ###################################################################
            # Verify that EFT filename is a valid length 
            ###################################################################
            rootLogger.info("")
            rootLogger.info("Verify EFT filename length.")
            
            if len(MF_FILENAME) > 44:
                rootLogger.info("")
                rootLogger.info(f"{MF_FILENAME} filename is {len(MF_FILENAME)} characters which is too long. ")
                
                # Send Failure email	
                SUBJECT=f"ProcessFiles2EFT.py - Failed ({ENVNAME})"
                MSG=f"{MF_FILENAME} filename is {len(MF_FILENAME)} characters which is too long.  "
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                sys.exit(12)	


            #############################################################
            # Upload uncompressed linux file to S3/EFT_Files folder (or overrider folder)
            # NOTE: Only send/trigger files that can actually be EFT'd.
            #############################################################
            offset =  MF_FILENAME.index('.')
            HLQ = MF_FILENAME[: offset]
   
            if HLQ in ("P#EFT", "T#EFT", "MNUP") and swTESTING == "N":
                rootLogger.info("")
                rootLogger.info(f"Upload linux decompressed/cleansed file {txt_filename} to s3://{XTR_BUCKET}{S3HLFolder}{S3_EFT_DESTINATION_FLDR}{MF_FILENAME}")
               
                s3EFTFileKey = f"{S3HLFolder}{S3_EFT_DESTINATION_FLDR}{MF_FILENAME}"
                s3UploadFile(s3_client, unzippedExtFilePath, XTR_BUCKET, s3EFTFileKey)
                
            else:
                rootLogger.info("")
                rootLogger.info(f"{HLQ=}; File NOT loaded to S3 EFT_FILES folder.")


            ###################################################################
            # Move processed Extract file to Extract archive folder.
            ###################################################################
            rootLogger.info("")
            rootLogger.info(f"Move processed {gz_filename} to S3 Extract archive folder ")

            sSourceKey = f"{S3ExtKeyFldr2Process}{gz_filename}"
            sDestinationKey = f"{S3ExtKeyFldr2Process}archive/{gz_filename}"

            s3MoveLargeFile2NewFolder(s3_client, XTR_BUCKET, f"{S3ExtKeyFldr2Process}{gz_filename}", f"{S3ExtKeyFldr2Process}archive/{gz_filename}")


            #############################################################
            # clean-up linux data directory
            #############################################################
            os.remove(f"{DATADIR}{txt_filename}")

            # end-for

        
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("ProcessFiles2EFT.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in ProcessFiles2EFT.py\n {e}")

        rootLogger.error("Exception occured in ProcessFiles2EFT.py.")
        rootLogger.error("\n%s", str(e))
        
        # Send Failure email	
        SUBJECT=f"ProcessFiles2EFT.py - Failed ({ENVNAME})"
        MSG=str(e)
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 

        sys.exit(12)


if __name__ == "__main__":
    
    main_processing_loop()