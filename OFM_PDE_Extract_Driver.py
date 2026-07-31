#!/usr/bin/bash
############################################################################################################
# Name:  OFM_PDE_Extract.sh
#
# Desc: OFM PDE Extract using finder files for various contractors
#
# Execute as ./OFM_PDE_Extract.sh 
#
# NOTE: Script must be /usr/bin/bash instead of /usr/bin/sh because the included FilenameCounts.bash file requires some bash specific syntax for 
#       a paste command which uses "bash Process Substitution". The paste command expects files and not variables as input. However, 
#       "bash Process Substitution" makes variables appear to the command as files.  The purpose of doing it this way instead of using temp files 
#       is because we have multiple scripts using the same data directory that would use the same temp file, possibly interferring with each other. 
#
#       With the EFT functionality changing the AWS name to Mainframe Name, ensure that the CONTRACTOR NAME in the finder
#       files follow the proper MF dataset name constraints. The script will abort if the name is too long. 
#
# Author     : Paul Baranoski	
# Created    : 03/24/2023
#
# Modified:
#
# Paul Baranoski 2023-03-24 Created script.
# Paul Baranoski 2023-04-04 Had wrong log file descriptions for S3 config and Finder_files folders. Corrected.
# Paul Baranoski 2023-04-26 Added code to skip blank lines in Finder files.
# Josh Turner    2023-05-11 Added EFT functionality
# Paul Baranoski 2024-07-31 Add ENV to Subject line for emails.
# Paul Baranoski 2024-08-02 Comment out EFT processing so I can process Conrad Finder File and manually create Manifest file.
#                           Remove configuration file logic.
# Paul Baranoski 2024-08-06 Add createManifestFileFunc.sh include-script to handle the create of manifest files to
#                           limit the NOF extract files to a constant value set in parent script.
# Paul Baranoski 2024-08-28 Correct syntax for TMPSTMP from '=' to ':='. 
# Paul Baranoski 2025-01-29 Remove createManifestFileFunc.sh and use of its function to control NOF files to include in a manifest file.
#                           That logic is now contained in the CreateManifestFile.sh. 
# Paul Baranosi  2025-02-06 Update script to accept parameter year override so we can run extract as if run during a prior year.
# Paul Baranoski 2026-04-14 Convert script to python.
############################################################################################################

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
import logging
import sys
import argparse

#import datetime
from datetime import datetime
from datetime import date,timedelta
import time

import os
import subprocess

import CreateManifestFileDriver as CreManDr

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

DATADIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

S3BUCKET = rf"{XTR_BUCKET}/{OFM_PDE_BUCKET_FLDR}"
FINDER_FILE_BUCKET = rf"{XTR_BUCKET}/{FINDER_FILE_BUCKET_FLDR}"

# DSH_REQUEST_{Sender}_DYYYYMMDD.csv	
FF_PREFIX = "OFM_PDE_Finder_File"

# Variables for extracting logfile entries
LOG_FROM_LINE = 1
LOG_TO_LINE = 1
TMP_OFM_PDE_FF_LOGFILE = "tmpOFM_PDE_FFLOG.txt"
TMP_OFM_PDE_FF_LIST = "tempOFMPDE.txt"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}OFM_PDE_Extract_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nOFM_PDE_Extract_Driver.py started at {TMSTMP}")

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
        if not (iNOFParms == 0 or iNOFParms ==  1):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")
            
        #############################################################
        # Determine date parameters for extract query in python code
        #############################################################
        rootLogger.info("")
        rootLogger.info("Determine date parameters for extract query")

        if iNOFParms == 1:
            lstParms = sys.argv

            ParmOverrideDateYYYY = lstParms[1]
            
            rootLogger.info(f"{ParmOverrideDateYYYY=} ")            
            
            CUR_YYYY = ParmOverrideDateYYYY
        else:
            rootLogger.info(f"No override YYYY")            

            dtTodayDt = date.today()
            CUR_YYYY = dtTodayDt.strftime("%Y")

        ###########################################################
        # Calculate additional date parameters for SQL
        ###########################################################
        CLM_EFCTV_DT = f"{CUR_YYYY}-06-30"        
        CLM_PRIOR_YYYY = str(int(CUR_YYYY) - 1 )

        ###########################################################
        # Display extract dates to use.
        ###########################################################
        rootLogger.info(f"{CUR_YYYY=}")
        rootLogger.info(f"{CLM_EFCTV_DT=}")
        rootLogger.info(f"{CLM_PRIOR_YYYY=}")


        #################################################################################
        # Get list of OFM PDE Finder Files in S3.
        #################################################################################
        rootLogger.info("")
        rootLogger.info("List OFM PDE Finder Files in S3 ")

        lstFFKeys = getS3FileKeysList(s3_resource, XTR_BUCKET, FINDER_FILE_BUCKET_FLDR, FF_PREFIX)
        
        lstFinderFiles = getFilenamesFromS3Keys(lstFFKeys, FINDER_FILE_BUCKET_FLDR)
        

        #################################################################################
        # Loop thru OFM PDE Finder Files
        #################################################################################
        for finderFile in lstFinderFiles: 

            #############################################################
            # Start extract for next Finder file record
            #############################################################
            rootLogger.info("")
            rootLogger.info("-----------------------------------")
            rootLogger.info(f"Processing {finderFile}")		
	
            #################################################
            # Save logfile start line num for current FF
            # NOTE: This is used to extract filenames and 
            #       record counts for current respective success emails.	
            #################################################
            rootLogger.info("")
            
            LOG_FROM_LINE = wc_l(LOGNAME)
            rootLogger.info(f"{LOG_FROM_LINE=}")
		
            #############################################################
            # Copy Finder file from S3 to linux 
            #############################################################
            rootLogger.info(f"Download Finder File {finderFile} from S3 to linux data directory")
            
            try:
                
                downloadFileFromS3(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{finderFile}", f"{DATADIR}{finderFile}")

            except Exception as ex:

                rootLogger.info("")
                rootLogger.info(f"Copying S3 OFM PDE Finder file {finderFile} to Linux failed.")
                
                # Send Failure email	
                SUBJECT = f"OFM_PDE_Extract.sh  - Failed ({ENVNAME}{TESTEMAIL})"
                MSG = f"Copying S3 file from {FINDER_FILE_BUCKET_FLDR} failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                # Re-raise exception
                raise

	
            #################################################################################
            # Extract CONTRACTOR/Mailbox from Finder File filename
            # Ex. OFM_PDE_Finder_File_Bland_20230324.145911 --> "Bland"
            #
            # If EFT'ing file: be sure to verify the CONTRACTOR/Mailbox is not too long for the Mainframe
            #################################################################################
            rootLogger.info("")
            rootLogger.info("Extract CONTRACTOR Name from Finder File filename")
            
            # Get the 5th node from filename which contains the Contractor
            CONTRACTOR = finderFile.split("_")[4]
            rootLogger.info(f"{CONTRACTOR=}")

            # CONTRACTOR string/node cannot be greater than 8 characters
            iCONTRACTOR_LEN = len(CONTRACTOR)
            if iCONTRACTOR_LEN > 8:
        
                rootLogger.info("")
                rootLogger.info(f"Finder file CONTRACTOR node length ({iCONTRACTOR_LEN}) too long.")
                
                # Send Failure email	
                SUBJECT = f"OFM_PDE_Extract.sh  - Failed ({ENVNAME}{TESTEMAIL})"
                MSG = f"The contractor name length for finder file {finderFile} is too long with length: {iCONTRACTOR_LEN}. Please check all finder file names."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                rootLogger.info(sp_info)        

                sys.exit(12)


            #################################################################################
            # Set Box email recipients based on Contractor	
            #################################################################################
            rootLogger.info("")
            rootLogger.info("Set appropriate Contractor/Mailbox recipient  ")
            
            # Set Box recipient for Contractor/Mailbox
            if CONTRACTOR == "BLAND":
                BOX_RECIPIENT = OFM_PDE_BLAND_BOX_RECIPIENT 
            elif CONTRACTOR == "CGI":
                BOX_RECIPIENT = OFM_PDE_CGI_BOX_RECIPIENT 
            elif CONTRACTOR == "MHM":
                BOX_RECIPIENT = OFM_PDE_MHM_BOX_RECIPIENT 
            elif CONTRACTOR == "DJLLC":
                BOX_RECIPIENT = OFM_PDE_DJLLC_BOX_RECIPIENT 
            elif CONTRACTOR == "CONRAD":
                BOX_RECIPIENT = OFM_PDE_CONRAD_BOX_RECIPIENT 
            else:
                rootLogger.info("")
                rootLogger.info(f"CONTRACTOR {CONTRACTOR} box email recipients are not set-up. Skip processing of contractor finder file. Make appropriate coding changes.")
 
                # Send Failure email	
                SUBJECT = f"OFM_PDE_Extract.py  - Warning ({ENVNAME}{TESTEMAIL})"
                MSG = f"CONTRACTOR {CONTRACTOR} box email recipients are not set-up. Skip processing of contractor finder file. Make appropriate coding changes."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                rootLogger.info(sp_info)        
 
                # Process next Finder File
                continue

            # display BOX Recipient
            rootLogger.info(f"{BOX_RECIPIENT=}")


            #################################################################################
            # Create Timestamp for Extract files for this BOX Recipient
            #################################################################################
            EXTRACT_FILE_TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
            rootLogger.info(f"{EXTRACT_FILE_TMSTMP=}")
		
            #################################################################################
            # Process records in Finder File
            #################################################################################
            rootLogger.info("")
            rootLogger.info(f"Process records in {finderFile}")
	
            #################################################
            # Process Finder File Request records
            #################################################
            with open(f"{DATADIR}{finderFile}", "r", encoding="utf-8") as f:

                for FF_RECORD in f:
                    rootLogger.info("")
                    rootLogger.info("Read next Finder File record")

                    # Remove CR from input record. Do I really need this? Will iterating thru text file remove end-of-record markers?
                    FF_RECORD = FF_RECORD.replace("\r","")
                    FF_RECORD = FF_RECORD.strip()          
         
                    # skip blank lines
                    if FF_RECORD == "": 
                        continue
                    
                    # Extract parameter values from record
                    lstFields = FF_RECORD.split(",")                    
                    CONTRACT_NUM = lstFields[0]
                    PBP_NUM = lstFields[1]
                    
                    rootLogger.info(f"{CONTRACT_NUM=}")
                    rootLogger.info(f"{PBP_NUM=}")

                    # Export environment variables for Python code
                    os.environ["EXTRACT_FILE_TMSTMP"] = EXTRACT_FILE_TMSTMP
                    os.environ["CLM_PRIOR_YYYY"] = CLM_PRIOR_YYYY
                    os.environ["CLM_EFCTV_DT"] = CLM_EFCTV_DT
                    os.environ["CONTRACTOR"] = CONTRACTOR
                    os.environ["CONTRACT_NUM"] = CONTRACT_NUM
                    os.environ["PBP_NUM"] = PBP_NUM

                    #############################################################
                    # Execute Python code to extract data.
                    #############################################################
                    rootLogger.info("Start execution of OFM_PDE_Extract.py program")

                    try:
                        sp_info = subprocess.run(['python3', 'OFM_PDE_Extract.py'], capture_output=True, text=True, check=True)
                        write_sp_info_2_log(sp_info) 
                        
                    except subprocess.CalledProcessError as e:
                        rootLogger.error(f"Calling OFM_PDE_Extract.py failed with return code {e.returncode}")
                        rootLogger.error(e.output)
                        
                        ## Send Failure email	
                        SUBJECT=f"OFM_PDE_Extract.Driver.py extract - Failed ({ENVNAME}{TESTEMAIL})"
                        MSG=f"Calling OFM_PDE_Extract.py failed with return code {e.returncode} "
                        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                        write_sp_info_2_log(sp_info) 

                        sys.exit(12)  
                    
                    # call was successful
                    rootLogger.info("Python script OFM_PDE_Extract.py completed successfully. ")
                    
            # end-for


            #############################################################
            # Create Manifest file
            #############################################################
            rootLogger.info("")
            rootLogger.info("Creating manifest file for OFM PDE Request Extract. ")

            #####################################################
            # bucket/s3folder --> points to location of extract file.
            #          TMSTMP --> uniquely identifies extract file(s)
            #       BoxEmails --> manifest file recipients
            #####################################################
            try:
                CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=OFM_PDE_BUCKET_FLDR, runToken=EXTRACT_FILE_TMSTMP, BoxEmails=BOX_RECIPIENT )

            except Exception as e:

                SUBJECT=f"Create Manifest file in OFM_PDE_Extract_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
                MSG=f"Create Manifest file in OFM_PDE_Extract_Driver.py  has failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)

                # re-raise exception
                raise

    
            #################################################
            # Save logfile end line num for current FF		
            #################################################
            rootLogger.info("")
            rootLogger.info("select range of lines in log file to search for extract filenames and counts.  ")
                  
            LOG_TO_LINE = wc_l(LOGNAME)

            rootLogger.info(f"{LOG_TO_LINE=}")


            #############################################################
            # Extract log file entries for current Finder file.
            #############################################################
            sLogfileRecs4FF = getRangeOfRecords(LOGNAME, LOG_FROM_LINE, LOG_TO_LINE)

            rootLogger.info(f"Write log records pertaining to request to temp log file")

            with tempfile.NamedTemporaryFile(delete=False) as tmpReqLogFile:
                tmpReqLogFileNPath = tmpReqLogFile.name
                rootLogger.info(f"{tmpReqLogFileNPath=}")

                tmpReqLogFile.write(sLogfileRecs4FF.encode("utf-8"))


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
            # Send Success email.
            #############################################################
            rootLogger.info("")
            rootLogger.info("Send success email.")

            # Add Box recipients to Success email recipients	
            SUCCESS_EMAIL_RECIPIENT = f"{OFM_PDE_EMAIL_SUCCESS_RECIPIENT},{BOX_RECIPIENT}"
                
            SUBJECT= f"OFM_PDE Extract - completed ({ENVNAME}{TESTEMAIL})" 
            MSG = f"OFM_PDE Extract completed for request file {finderFile}.\n\n\nThe following extract file(s) were created:\n\n{S3Files}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, SUCCESS_EMAIL_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

		
            #############################################################
            # Move S3 Finder file to archive directory    
            #############################################################	
            rootLogger.info("")
            rootLogger.info(f"Move processed S3 Finder File {finderFile} to S3 archive folder")
            
            s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{FINDER_FILE_BUCKET_FLDR}{finderFile}", f"{FINDER_FILE_BUCKET_FLDR}archive/{finderFile}")

		
            #############################################################
            # Remove Finder file from linux data directory   
            #############################################################	
            rootLogger.info(f"Remove processed finder file {finderFile} from linux data directory")		
            os.remove(f"{DATADIR}{finderFile}")

            # allow snowflake to clear up connections - sleep 30 seconds
            time.sleep(30)

		
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script OFM_PDE_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in OFM_PDE_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in OFM_PDE_Extract_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT=f"OFM_PDE_Extract_Driver.py  - Failed ({ENVNAME}{TESTEMAIL})"
        MSG=f"Exception occured in OFM_PDE_Extract_Driver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()