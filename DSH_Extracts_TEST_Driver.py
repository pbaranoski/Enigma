#!/usr/bin/env python
########################################################################################################
# Name:   DSH_Extracts_Bogus_Driver.py
#
# Desc: Create DSH Extract file for each request record in a request file.
#
# Created: Paul Baranoski
#
# Paul Baranoski 2024-06-28 Remove dashes in PRVDR_ID. (They shouldn't be there).
# Paul Baranoski 2024-07-10 Remove overide destination for manifest file. We want manifest files to be released automatically. 
# Paul Baranoski 2024-07-13 Add code to remove non-display binary characters from file. Add BCC to email message with invalid email address.
# Paul Baranoski 2024-07-15 Modify code calculating FY values from requested dates. 
#                           Add code to verify filename is in correct format (no double underscores).
#                           Make email addresses edit better. No spaces or brackets in email address. 
# Paul Baranoski 2024-07-16 Added code to bypass creation of manifest file, when no extract files were created due to no data for request.
#                           Also, added filename format to invalid filename error message.  
#                           Added newline character at end of request file. Some request files created in Windows were missing the end-of-record marker
#                           which prevented the last record to be processed. 
# Paul Baranoski 2024-07-18 Modified and improved code to bypass creation of manifest file when there were no records found for any of the requested extracts.
# Paul Baranoski 2024-08-15 Added Error handling for SendEmail calls.
#                           Also, added phrase "Ended at " when there are no finder files to process so that the Dashboard extract will see that script/job
#                           completed successfully.  
# Paul Baranoski 2024-09-20 Modified error message: changed "Too many fields" to "Incorrect NOF fields". 
# Paul Baranoski 2024-10-15 1) When getting Files2Process from S3, if request filename contains spaces, the "awk $4" will only get part of the filename. Added $5 and $6 fields 
#                           to awk command logic to get full filename when it contains spaces.
#                           2) Change IFS to "newline only" before for-loop to properly process request filenames which could contain spaces.
#                           3) Add double quotes around ${FF_EXT} references since request filename can contain spaces.
#                           4) in archiveRequestFile function, add double quotes around S3 filenames in s3 mv command since request filenames may contain spaces.
#                           5) Modify invalid filename error message to include what is allowed for {UNIQ-ID).  
# Paul Baranoski 2025-04-08 Add lower case command when extracting extension so that .CSV is the same as .csv in edit of request file extension. Add {csv|CSV) to egrep
#                           egrep regular expression.
# Paul Baranoski 2025-04-25 Modify egrep edit for request filename to use "\." instead of "." since a single period can represent any character where by the "\." is looking 
#                           for a literal period (like .csv). 
#                           Modify egrep email edit to allow a dash in email before and after the '@'. 
# Paul Baranoski 2025-05-08 Add call to DSH_AddReqEmails.py to capture DSH Requestor-UNIQ-ID and Requestor-Email into SF table.
# Paul Baranoski 2025-08-13 Modify success email verbiage to say request is in-process and not complete, and files will be available once they receive an email with a link to their Box account.
# Paul Baranoski 2026-03-11 Convert bash to python. Add TESTING functionality.
######################################################################################


########################################################################################################
# Set TESTING status 
########################################################################################################
import os
os.environ["TESTING"] = "N"

# This switch is needed to prevent Request Email addresses from being include in error and success emails and manifest files.
swInTESTMode = os.getenv("TESTING","N") 

# Our common module with variable constants
from SET_XTR_ENV import *

########################################################################################################
# IMPORTS
########################################################################################################
import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import logging
import sys
import argparse
import re
import io

import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data"

from datetime import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

########################################################################################################
# CONSTANTS
########################################################################################################
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"
DATADIR = "/app/IDRC/XTR/CMS/data/"

FINDER_FILE_BUCKET = rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"

# DSH_REQUEST_{Sender}_DYYYYMMDD.csv	
PREFIX = "DSH_REQUEST_"

# Variables for extracting logfile entries
LOG_FROM_LINE = 1
LOG_TO_LINE = 1
TMP_DSH_FF_LOGFILE = "tmpDSHFFLOG.txt"


#############################################################
# Functions
#############################################################
def archiveRequestFile(s3_client, sSourceBucket, FF):

    sSourceKey = f"{FINDER_FILE_BUCKET_FLDR}{FF}"
    sDestinationKey  = f"{FINDER_FILE_BUCKET_FLDR}archive/{FF}"

    #############################################################
    # Move Finder File in S3 to archive folder
    #############################################################
    rootLogger.info(f"Moving S3 DSH Finder file {FF} to S3 archive folder.")

    s3MoveFile2NewFolder(s3_client, sSourceBucket, sSourceKey, sDestinationKey)


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}DSH_Extracts_Bogus_{TMSTMP}.log" 

    
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        #global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDSH_Extracts_Bogus_Driver.py started at {TMSTMP}")

        # Pass the logger object to the CommonFunctions module.
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
        # Display S3 Buckets
        ##########################################
        rootLogger.info("")

        rootLogger.info(f"{XTR_BUCKET=}")
        rootLogger.info(f"{DSH_BUCKET_FLDR=}")
        rootLogger.info(f"{FINDER_FILE_BUCKET_FLDR=}")
        rootLogger.info(f"{FINDER_FILE_BUCKET=}")
        
        #################################################################################
        # Are there any DSH Extract Request/Finder files in S3?
        #################################################################################
        rootLogger.info("")
        rootLogger.info(f"Count NOF DSH Request/Finder files found in {FINDER_FILE_BUCKET_FLDR}")

        lstRequestFileKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_BUCKET_FLDR, PREFIX)
        
        rootLogger.info(f"NOF Request Files found: {len(lstRequestFileKeys)}")
        
        #################################################
        # If 0 finder files --> end gracefully		
        #################################################
        if len(lstRequestFileKeys) == 0:
            rootLogger.info("")
            rootLogger.info(f"There are no S3 DSH Finder files to process in s3://{FINDER_FILE_BUCKET}{PREFIX}.")
            
            # Send Info email	
            SUBJECT=f"DSH Extract ended - nothing to process ({ENVNAME}{TESTEMAIL})"
            MSG=f"There are no S3 DSH Finder files to process in s3://{FINDER_FILE_BUCKET}{PREFIX}."
       
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  
            
            rootLogger.info("")
            rootLogger.info("DSH_Extracts.sh completed successfully.")

            rootLogger.info(f"\nEnded at {TMSTMP}" )

            sys.exit(0)


        #################################################################################
        # Loop thru DSH Extract Request/Finder files.
        #################################################################################
        # Convert list of s3 File Keys to list of S3 Filenames
        lstFilenames = getFilenamesFromS3Keys(lstRequestFileKeys, FINDER_FILE_BUCKET_FLDR)
        
        # Loop thru finder filenames
        for sFF in lstFilenames:

            rootLogger.info("")
            rootLogger.info("******************************")
            rootLogger.info(f"Processing {sFF}")

            #################################################
            # Create separate timestamp for all files created from Request/Finder file
            #################################################
            sFF_TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')

            #################################################
            # Re-set BAD_FILE_SW
            #################################################
            BAD_FILE_SW = False

            #################################################
            # Verify that file extension is a .csv file	
            # Ex. DSH_REQUEST_Paul-B_20250923.CSV
            #################################################
            sFF_Ext = sFF.lower().split(".")[1]
            rootLogger.info(f"{sFF_Ext=}")
            
            if sFF_Ext != "csv":
                rootLogger.info("")
                rootLogger.info(f"Request file {sFF} has incorrect file extension. File cannot be processed. ")

                rootLogger.info(f"{DSH_EMAIL_BCC=}")
                rootLogger.info(f"{DSH_EMAIL_REPLY_MSG=}")
                
                # Send Failure email	
                SUBJECT=f"DSH Extract - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"Request file {sFF} has incorrect file extension. File cannot be processed. Please correct and re-submit file as csv file."
       
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DSH_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)  

                # migrate finder file to archive folder
                archiveRequestFile(s3_client, XTR_BUCKET, sFF)
                
                # process next Finder File
                continue


            #################################################
            # Verify Request filename matches expected format
            # NOTE: 1) No double underscores.
            #       2) Unique ID contains no spaces or special character except dash
            #################################################
            if not re.match("^DSH_REQUEST_[a-zA-Z0-9-]+_[0-9]+\.(csv|CSV)$", sFF):
                rootLogger.info("")
                rootLogger.info(f"Request file {sFF} is named incorrectly. ")

                rootLogger.info(f"{DSH_EMAIL_BCC=}")
                rootLogger.info(f"{DSH_EMAIL_REPLY_MSG=}")
                
                # Send Failure email	
                SUBJECT=f"DSH Extract - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"Request file {sFF} is named incorrectly. Please ensure that filename follows this pattern: DSH_REQUEST_[UNIQ-ID]_YYYYMMDD.csv. [UNIQ-ID] can only contain letters, numbers, and dash. Please correct and re-submit file with proper filename."
       
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DSH_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)  

                # migrate finder file to archive folder
                archiveRequestFile(s3_client, XTR_BUCKET, sFF)
                
                # process next Finder File
                continue

            
            #################################################
            # Extract FF ID NODE to use for extract files
            # Ex. 'DSH_REQUEST_Paul-B_20250923.csv' --> "Paul-B"
            #################################################
            rootLogger.info("Get user supplied unique Node from Request filename.")

            sFF_UniqID_Node = sFF.split("_")[2]

            rootLogger.info(f"{sFF_UniqID_Node=}")
            
            ##########################################################################################
            # Save logfile start line num for current FF
            # NOTE: This is used to extract filenames and record counts for individual request success emails.	
            ##########################################################################################
            rootLogger.info("")
            
            iLOG_FROM_LINE = wc_l(LOGNAME)

            rootLogger.info(f"The starting rec # of log messages for the current request file --> {iLOG_FROM_LINE=}")
            
            #################################################
            # Copy DSH Extract/Finder File to linux.		
            #################################################
            rootLogger.info("")
            rootLogger.info(f"Copy Finder File {sFF} from s3 to linux ")
            
            downloadFileFromS3(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{sFF}", f"{DATADIR}{sFF}")


            ##############################################
            # Cleansing of request file
            ##############################################
            # Perform sed to remove any non-display characters, non-ASCII characters. Thanks Monica!
            rootLogger.info(f"Cleanse linux file using sed to remove any non-display characters, non-ASCII/non-UTF-8 characters like x'EF'.")

            sp_info = subprocess.run(rf"LC_ALL=C sed -i 's/[\x80-\xff]//g' {DATADIR}{sFF}", shell=True, capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            # Add ending newline character in case its missing for last record. (User forgot to press enter after entering record data). 
            with open(f"{DATADIR}{sFF}", "a", newline="") as f:
                f.write("\n")

            ##############################################
            # Process each record in Finder File
            ##############################################
            with open(f"{DATADIR}{sFF}", "r", encoding="UTF-8") as f:
            
                for sExtRecord in f:

                    rootLogger.info("")
                    rootLogger.info("----------------------------")
                    
                    # Remove leading and trailing spaces and end-of-record markers
                    sExtRecord = sExtRecord.strip()
                    rootLogger.info(f"{sExtRecord=}")
                
                    # skip blank lines - zero length
                    if sExtRecord == "":
                        continue

                    # skip "blank lines" containing only spaces and commas 
                    if sExtRecord.replace(" ","").replace(",","") == "":
                        rootLogger.info("Skip blank record")
                        continue
                   
                    # skip comment lines
                    if sExtRecord.startswith("#"):
                        rootLogger.info("Skip comment record")
                        continue

                    # skip Header record
                    if sExtRecord [0:5] == "PRVDR":
                        rootLogger.info("Skip header record")
                        continue

                    ##############################################
                    # NOF fields not correct for record?
                    # --> Reject file
                    ##############################################
                    lstFields = sExtRecord.split(",")
                    iNOF_FIELDS = len(lstFields)
                    
                    if iNOF_FIELDS != 4:
                        rootLogger.info("")
                        rootLogger.info(f"Request file {sFF} has incorrectly formatted records. Incorrect number of fields {iNOF_FIELDS} found instead of 4. ")
                        
                        # Send Failure email	
                        SUBJECT=f"Request file ${FF} has incorrectly formatted records. ({ENVNAME}{TESTEMAIL})"
                        MSG=f"Request file {sFF} has incorrectly formatted records. Incorrect NOF fields {iNOF_FIELDS} instead of 4. Request file has been rejected. Please correct and re-submit file."
       
                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DSH_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                        write_sp_info_2_log(sp_info)  

                        # Set BAD_FILE_SW to True
                        BAD_FILE_SW = True
                        
                        # Exit for loop that is processing records in file
                        break


                    ##############################################
                    # Extract DSH record fields; remove leading/trailing spaces
                    ##############################################
                    rootLogger.info("Extract record fields")
                    
                    sPRVDR_ID_ON_REC = lstFields[0].strip()
                    rootLogger.info(f"{sPRVDR_ID_ON_REC=}")
                    
                    # Remove any dashes in PRVDR_ID
                    sPRVDR_ID = sPRVDR_ID_ON_REC.replace('-','')
                    rootLogger.info(f"{sPRVDR_ID=}")
                    
                    # Remove leading and trailing spaces
                    sFROM_FY_DT = lstFields[1].strip()
                    rootLogger.info(f"{sFROM_FY_DT=}")

                    sTO_FY_DT = lstFields[2].strip()
                    rootLogger.info(f"{sTO_FY_DT=}")

                    sREQSTR_EMAIL = lstFields[3].strip()
                    rootLogger.info(f"{sREQSTR_EMAIL=}")

                
                    ##############################################
                    # Validate EMAIL Address in record
                    ##############################################
                    rootLogger.info("Validate email address")
                      
                    # Email Address is missing    
                    if sREQSTR_EMAIL == "":            
                        rootLogger.info("")
                        rootLogger.info(f"Request file {sFF} has blank/empty email address. ")

                        # Send Failure email	
                        SUBJECT=f"Request file {sFF} has blank/empty email address. ({ENVNAME}{TESTEMAIL})"
                        MSG=f"Request file {sFF} has blank/empty email address. File cannot be processed. Please correct and re-submit file."
               
                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DSH_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                        write_sp_info_2_log(sp_info)  

                        # Set BAD_FILE_SW to True
                        BAD_FILE_SW = True
                        
                        # Exit for loop that is processing records in file
                        break	
                        
                    else:
                        # Make sure there are one or more valid characters before and after the at-sign
                        if not re.match("^[a-zA-Z0-9_\.-]+@[a-zA-Z0-9\.-]+$", sREQSTR_EMAIL):
                            rootLogger.info("")
                            rootLogger.info(f"Request file {sFF} has invalid email address: {sREQSTR_EMAIL} ")
                            
                            # Send Failure email	
                            SUBJECT=f"Request file {sFF} has invalid email address. ({ENVNAME}{TESTEMAIL}). "
                            MSG=f"Request file {sFF} has invalid email address: {sREQSTR_EMAIL}. File cannot be processed. Please correct and re-submit file."

                            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, DSH_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                            write_sp_info_2_log(sp_info)  

                            # Set BAD_FILE_SW to True
                            BAD_FILE_SW = True

                            # Exit for loop that is processing records in file
                            break	
                        

                    # Set the email recipients who will receive the request emails
                    if swInTESTMode == "Y":
                        # Do not include requestor's email address 
                        rootLogger.info(f"{swInTESTMode=}")			

                        RQST_EMAIL_RECIPIENT = f"{DSH_EMAIL_SUCCESS_RECIPIENT}"
                    else:
                        rootLogger.info(f"{swInTESTMode=}")			

                        RQST_EMAIL_RECIPIENT = f"{DSH_EMAIL_SUCCESS_RECIPIENT},{sREQSTR_EMAIL}"
                        
                    rootLogger.info(f"{RQST_EMAIL_RECIPIENT=}")			


                    ##############################################
                    # Is FROM_FY_DT a valid date (YYYY-MM-DD)
                    ##############################################
                    rootLogger.info("Validate From FY Date")
                      
                    if sFROM_FY_DT == "":        
                        rootLogger.info("")
                        rootLogger.info("Incorrectly formatted record found. 'From FY Date' is blank/empty")
                        
                        # Send Failure email	
                        SUBJECT=f"Request file {sFF} has incorrectly formatted records. ({ENVNAME}{TESTEMAIL})"
                        MSG=f"Incorrectly formatted record found. 'From FY Date' is blank/empty. Request file cannot be processed. Please correct and re-submit file."

                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RQST_EMAIL_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                        write_sp_info_2_log(sp_info)  

                        # Set BAD_FILE_SW to True
                        BAD_FILE_SW = True

                        # Exit loop to process records in file
                        break
                        
                    else:

                        # is date a valid date in the expected format?
                        if not isValidDate(sFROM_FY_DT, "%m/%d/%Y"):

                            rootLogger.info("")
                            rootLogger.info(f"Incorrectly formatted record found. Invalid date for 'From FY Date': {sFROM_FY_DT}")
                            
                            # Send Failure email	
                            SUBJECT=f"Request file {sFF} has incorrectly formatted records. ({ENVNAME}{TESTEMAIL})"
                            MSG=f"Incorrectly formatted record found. Invalid date for 'From FY Date': {sFROM_FY_DT}. Request file cannot be processed. Please correct and re-submit file."

                            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RQST_EMAIL_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                            write_sp_info_2_log(sp_info)  

                            # Set BAD_FILE_SW to True
                            BAD_FILE_SW = True

                            # Exit loop to process records in file
                            break


                    ##############################################
                    # Is TO_FY_DT a valid date
                    ##############################################
                    rootLogger.info("Validate To FY Date")

                    if sTO_FY_DT == "":
                        rootLogger.info("")
                        rootLogger.info("Incorrectly formatted record found. 'To FY Date' is blank/empty")
                        
                        # Send Failure email	
                        SUBJECT=f"Request file {sFF} has incorrectly formatted records. ({ENVNAME}{TESTEMAIL})"
                        MSG=f"Incorrectly formatted record found. 'To FY Date' is blank/empty. Request file cannot be processed. Please correct and re-submit file."

                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RQST_EMAIL_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                        write_sp_info_2_log(sp_info)  

                        # Set BAD_FILE_SW to True
                        BAD_FILE_SW = True

                        # Exit loop to process records in file
                        break
                        
                    else:
                        
                        # is date a valid date in the expected format?
                        if not isValidDate(sTO_FY_DT, "%m/%d/%Y"):
                            rootLogger.info("")
                            rootLogger.info(f"Incorrectly formatted record found. Invalid date for 'To FY Date': {sTO_FY_DT}")
                            
                            # Send Failure email	
                            SUBJECT=f"Request file {sFF} has incorrectly formatted records. ({ENVNAME}{TESTEMAIL})"
                            MSG=f"Incorrectly formatted record found. Invalid date for 'To FY Date': {sTO_FY_DT}. Request file cannot be processed. Please correct and re-submit file."

                            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RQST_EMAIL_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                            write_sp_info_2_log(sp_info)  

                            # Set BAD_FILE_SW to True
                            BAD_FILE_SW = True

                            # Exit loop to process records in file
                            break
                        

                    ##############################################
                    # Convert FROM_DT to DSH FY.
                    ##############################################
                    rootLogger.info("Convert FROM_DT TO DSH FY YYYY")
                    
                    dttmFromFYDt = datetime.strptime(sFROM_FY_DT,"%m/%d/%Y")
                    iMM = int(dttmFromFYDt.strftime("%m"))
                    if iMM >= 10:
                        sFROM_FY = (dttmFromFYDt + relativedelta(years=1)).strftime('%Y')
                    else:
                        sFROM_FY = dttmFromFYDt.strftime('%Y')

                    rootLogger.info(f"{sFROM_FY=}")	

                    ##############################################
                    # Convert TO_DT to DSH FY.
                    ##############################################
                    rootLogger.info("Convert TO_DT TO DSH FY YYYY")

                    dttmToFYDt = datetime.strptime(sTO_FY_DT,"%m/%d/%Y")
                    iMM = int(dttmToFYDt.strftime("%m"))
                    if iMM >= 10:
                        sTO_FY = (dttmToFYDt + relativedelta(years=1)).strftime('%Y')
                    else:
                        sTO_FY = dttmToFYDt.strftime('%Y')

                    rootLogger.info(f"{sTO_FY=}")	

                    
                    ##############################################
                    # Export fields for python extract code.
                    ##############################################	
                    os.environ["PRVDR_ID"] = sPRVDR_ID                
                    os.environ["FROM_FY"] = sFROM_FY
                    os.environ["TO_FY"] = sTO_FY
                    os.environ["FF_TMSTMP"] = sFF_TMSTMP
                    os.environ["FF_ID_NODE"] = sFF_UniqID_Node


                    ##############################################
                    # Extract DSH records for Extract record.
                    ##############################################
                    rootLogger.info("")
                    rootLogger.info(f"Extract DSH data for Provider {sPRVDR_ID} for Extract Dates {sFROM_FY} to {sTO_FY} ")

                    #  check=True --> will throw an expception when RC != 0; Want to check RC myself so I capture both stdout and stderr
                    sp_info = subprocess.run(['python3', 'DSH_Extracts.py'], capture_output=True, text=True)
                    write_sp_info_2_log(sp_info)  

                    ##############################################
                    # End-For loop: Processing request file
                    ##############################################



            #######################################################
            # If bad file --> archive file; remove file from linux 		
            #######################################################
            if BAD_FILE_SW == True:
                archiveRequestFile(s3_client, XTR_BUCKET, sFF)
                deleteFileFromLinux(f"{DATADIR}{sFF}")
                
                # process next Request file
                continue


            #################################################
            # Finish processing Good Request File		
            #################################################

            #################################################
            # Save logfile end line num for current Request file	
            #################################################
            rootLogger.info("")
            rootLogger.info("select end range of lines in log file to search for extract filenames and counts. ")
                    
            iLOG_TO_LINE = wc_l(LOGNAME)
            rootLogger.info(f"The end of log messages for the currently processed request file: {iLOG_TO_LINE}")
            
            #############################################################
            # Extract log file entries for current request file.
            #############################################################
            sLogfileRecs4Request = getRangeOfRecords(LOGNAME, iLOG_FROM_LINE, iLOG_TO_LINE)

            rootLogger.info(f"Write log records pertaining to request to temp log file")

            with tempfile.NamedTemporaryFile(delete=False) as tmpReqLogFile:
                tmpReqLogFileNPath = tmpReqLogFile.name
                rootLogger.info(f"{tmpReqLogFileNPath=}")

                tmpReqLogFile.write(sLogfileRecs4Request.encode("utf-8"))


            #############################################################
            # Get list of S3 files and record counts for success email.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Get S3 Extract file list and record counts")
            
            # Retrieve extract files and record counts from temp log file
            S3Files = getExtractFilenamesAndCounts(rootLogger, tmpReqLogFileNPath)  
        
            rootLogger.info(f"{S3Files=}")

            # Delete temp file - no longer needed
            os.remove(tmpReqLogFileNPath)


            #############################################################
            # Create Manifest file and success email.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Create Manifest file for DSH Request Extract. ")

            # Get Count of NOF Extract Files to include in manifest file
            lstFiles4ManifestFile = getExtFiles4RequestList(s3_resource, XTR_BUCKET, f"{DSH_BUCKET_FLDR}", sFF_TMSTMP)
            
            if len(lstFiles4ManifestFile) == 0:
            
                rootLogger.info("No manifest file to create for DSH Request Extract. ")

                #############################################################
                # Send success email of DSH Extract files
                #############################################################
                rootLogger.info("")
                rootLogger.info("Send success email.")

                # Send Success email	
                SUBJECT=f"DSH Extract - completed ({ENVNAME}{TESTEMAIL})"
                MSG=f"DSH Extract completed for request file {sFF}. \n\nThe following extract files were processed:\n\n{S3Files}\n\nNo manifest file was created.\n\nPlease note that DSH data is calculated by the federal government fiscal year which goes from October 1 from the prior year, through September 30 of the current year. Example: Fiscal year 2021 is from 10/1/2020 through 9/30/2021."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RQST_EMAIL_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)  
                
            else:	
                #####################################################
                # S3BUCKET --> points to location of extract file. 
                #          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
                # TMSTMP   --> uniquely identifies extract file(s) 
                # EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
                # MANIFEST_HOLD_BUCKET --> overide destination for manifest file
                #
                # Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DSH/ 20231211.125522 pbaranoski-con@index.com 
                #####################################################
                rootLogger.info("Creating manifest file for DSH Request Extract. ")

                if sREQSTR_EMAIL == "":
                    BOX_RECIPIENT = DSH_BOX_RECIPIENT
                else:
                    rootLogger.info(f"{swInTESTMode=}")	
                    
                    if swInTESTMode == "Y":
                        BOX_RECIPIENT = DSH_BOX_RECIPIENT
                    else:
                        BOX_RECIPIENT = f"{sREQSTR_EMAIL},{DSH_BOX_RECIPIENT}"
                
                rootLogger.info(f"{BOX_RECIPIENT=}")

                
                #####################################################
                # bucket/s3folder --> points to location of extract file.
                #                 --> S3 folder is key token to config file to determine if manifest file is in HOLD status
                #          TMSTMP --> uniquely identifies extract file(s)
                #       BoxEmails --> manifest file recipients
                #####################################################
                rootLogger.info(f"Create Manifest file")

                try:
                    CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=DSH_BUCKET_FLDR, runToken=sFF_TMSTMP, BoxEmails=BOX_RECIPIENT)

                except Exception as e:

                    SUBJECT=f"Create Manifest file in DSH_Extracts_Bogus_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
                    MSG=f"Create Manifest file in DSH_Extracts_Bogus_Driver.py has failed."

                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info)

                    # re-raise exception
                    raise

                #############################################################
                # Send success email of DSH Extract files
                #############################################################
                rootLogger.info("")
                rootLogger.info("Send success email.")

                # Send Success email	
                SUBJECT = f"DSH Extract - In-Process ({ENVNAME}{TESTEMAIL})"
                MSG = f"DSH Extract in process for request file {sFF}. \n\nThe following extract files were created:\n\n{S3Files}\n\nOnce the process is complete and the file(s) are available, you will receive an email from data.request@datainsights.cms.gov with a link to the file location in your Box account.\n\nThe manifest file is DSH_EXTRACT_Manifest_{sFF_TMSTMP}.json\n\nPlease note that DSH data is calculated by the federal government fiscal year which goes from October 1 from the prior year, through September 30 of the current year. Example: Fiscal year 2021 is from 10/1/2020 through 9/30/2021."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, RQST_EMAIL_RECIPIENT, SUBJECT, MSG, DSH_EMAIL_BCC, DSH_EMAIL_REPLY_MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)  


                #############################################################
                # Insert new Requestor Emails into DSH_EMAIL table.
                #############################################################
                """
                rootLogger.info("")
                rootLogger.info(f"Insert new DSH Requestor Email Address for Requestor UNIQ-ID")

                try:
                    sp_info = subprocess.run(['python3', 'DSH_AddReqEmails.py', "--ReqID", sFF_UniqID_Node, "--Email", sREQSTR_EMAIL ], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info)  

                except Exception as e:

                    SUBJECT = f"Insert new DSH Requestor Email Address into DSH_EMail table in DSH_Extract.sh - Failed ({ENVNAME})"
                    MSG = f"Insert new DSH Requestor Email Address into DSH_EMail table in DSH_Extract.sh has failed."

                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info)

                    # re-raise exception
                    raise
                """
                
            #############################################################
            # END-IF
            #############################################################

            
            #############################################################
            # Move Finder File in S3 to archive folder
            #############################################################
            archiveRequestFile(s3_client, XTR_BUCKET, sFF)
            deleteFileFromLinux(f"{DATADIR}{sFF}")
	

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################     -
        rootLogger.info("")
        rootLogger.info("DSH_Extracts_Bogus_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )
        sys.exit(0)


  except Exception as e:
        print (f"Exception occured in DSH_Extracts_Bogus_Driver.py\n {e}")

        rootLogger.error("Exception occured in DSH_Extracts_Bogus_Driver.py.")
        rootLogger.error("\n%s", str(e))
        
        ## Send Failure email	
        SUBJECT=f"DSH_Extracts_Bogus_Driver.py  - Failed ({ENVNAME}{TESTEMAIL})"
        MSG=f"Exception occured in DSH_Extracts_Bogus_Driver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)    


if __name__ == "__main__":

        main_processing_loop()
