#!/usr/bin/env python
######################################################################################
# Name:  PartB_Carrier_Driver.py
#
# Desc: Extract Part B Carrier Claims data. 
#
# Created: Sumathi Gayam  
# Modified: 06/13/2022
# 
# Paul Baranoski 2022-09-27 Added code to call CombineS3Files.sh to concatenate/combine 
#                           S3 "parts" files 
# Paul Baranoski 2023-07-26 Modify logic to get filenames and record counts for email.  
# Paul Baranoski 2024-02-01 Add ENVNAME to SUBJECT line of emails.
# Paul Baranoski 2024-02-01 Remove call to box. Add EFT functionality.
#                           Add logic to remove temp file at end of script.
# Paul Baranoski 2025-02-04  Modify Email constants to use CMS_EMAIL_SENDER and ENIGMA_EMAIL_FAILURE_RECIPIENT.
# Paul Baranoski 2026-02-04 Modify success email. Change PECOS_EMAIL_SENDER to CMS_EMAIL_SENDER.
# Vijayendra Mandavilli 2026-02-04 Convert Shell code to python script.
# Paul Baranoski 2026-07-23 Add "from CommonFunctions import *" and remove 2 hard-coded functions that duplicate this functionality.
#                           Change logic to CombineS3 files from call to CombineS3Files.sh to import of module CombineS3FilesDriver.py, 
#                           and caling function in that module to combine s3 files. 
######################################################################################

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

import CreateManifestFileDriver as CreManDr

# Our common module with variable constants
from SET_XTR_ENV import *

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import CombineS3FilesDriver as CombineS3FilesDr
import LoggerStandard as EnigmaLog
from CommonFunctions import *

PTB_CARR_BUCKET = rf"{XTR_BUCKET}/{PTB_CARR_BUCKET_FLDR}"
S3BUCKET={PTB_CARR_BUCKET}
sTempFilename = "PTBCarrTemp.txt"

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

        LOGNAME = f"{LOG_DIR}{TESTLOG}PartB_Carrier_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPartB_Carrier.sh started at {TMSTMP}")

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
        if not (iNOFParms == 0 or iNOFParms ==  1):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
        else:
            rootLogger.info(f"There were {iNOFParms} override parameters to script.")


        #############################################################
        # Get S3 references
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
        
        ##########################################
        # Set S3 Bucket
        ##########################################
      
        rootLogger.info(f"Part B Carrier Bucket={PTB_CARR_BUCKET_FLDR}")


        ###########################################
        # Section 1: PartB_Carrier_Driver.py logic
        ############################################    

        rootLogger.info("--- Starting PartB_Carrier_Driver.py logic ---")  

        ##################################################################
        # Execute Python code to load tmp table for Part B Carrier data.
        ##################################################################
        rootLogger.info("")
        rootLogger.info("Start execution of LOAD_ST_TMP_LEO_PTB_TAB.py program")

        try:
            sp_info = subprocess.run(['python3', 'LOAD_ST_TMP_LEO_PTB_TAB.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling LOAD_ST_TMP_LEO_PTB_TAB.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)
            
            ## Send Failure email	
            SUBJECT=f"PartB Carrier Load to temp table - Failed ({ENVNAME})"
            MSG=f"PartB Carrier load to temp table has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    
    

        rootLogger.info("")
        rootLogger.info("Python script LOAD_ST_TMP_LEO_PTB_TAB.py completed successfully.")


        ############################################################################
        # Establish Date Parameters 
        ############################################################################
        rootLogger.info("")
        rootLogger.info(f"Create Parameters")

        now = datetime.now()
        LAST_YEAR =  str(now.year -1)
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        iMonth = int(now.strftime("%m"))

        # Set Extract type
        if iMonth < 3:
            EXT_TYPE = "M12"
        elif iMonth >= 3 and iMonth <= 6:
            EXT_TYPE = "EARLY"
        else:
            EXT_TYPE = "FINAL"

        ext_qtrs = [
            {"EXT_QTR":"1",
             "QSTRT_DT": ["-01-01"],
             "QEND_DT": ["-03-31"]
            },
            {"EXT_QTR":"2",
             "QSTRT_DT": ["-04-01"],
             "QEND_DT": ["-06-30"]
            },
            {"EXT_QTR":"3",
             "QSTRT_DT": ["-07-01"],
             "QEND_DT": ["-09-30"]
            },
            {"EXT_QTR":"4",
             "QSTRT_DT": ["-10-01"],
             "QEND_DT": ["-12-31"]
            }
        ]

        #############################################################
        # Display Parameters in log
        #############################################################
        rootLogger.info("")
        rootLogger.info("Part B Carrier Processing will use the following parameters:")
        rootLogger.info(f"{LAST_YEAR=}")
        rootLogger.info(f"{EXT_TYPE=}")     

        #############################################################
        # Process Extract thru Quarters
        #############################################################
        rootLogger.info("")
        rootLogger.info("Loop thru Quarters for extract files")


        for i in range(len(ext_qtrs)):
            
            iQTR = ext_qtrs[i]['EXT_QTR']
            rootLogger.info(f"{iQTR=}")

            for j in range(len(ext_qtrs[i]["QSTRT_DT"])):

                # Ex. 2026-01-01, 2026-03-31
                QSTART_DATE = f"{LAST_YEAR}{ext_qtrs[i]['QSTRT_DT'][j]}"
                QEND_DATE = f"{LAST_YEAR}{ext_qtrs[i]['QEND_DT'][j]}"
            
                XTR_FILE_NAME = f"PartB_Carrier_{EXT_TYPE}_{LAST_YEAR}_QTR{iQTR}_{TMSTMP}.txt.gz"
                
                rootLogger.info(f"{QSTART_DATE=}")
                rootLogger.info(f"{QEND_DATE=}")
                rootLogger.info(f"{iQTR=}")
                rootLogger.info(f"{XTR_FILE_NAME=}")
     
                ###########################################################################################
                # Set environment variables
                ###########################################################################################
                os.environ["QSTART_DATE"] = QSTART_DATE
                os.environ["QEND_DATE"] = QEND_DATE
                os.environ["EXT_TYPE"] = EXT_TYPE
                os.environ["XTR_FILE_NAME"] = XTR_FILE_NAME
                # os.environ["TMSTMP"] = TMSTMP

                #############################################################
                # Execute Python code to extract data.
                #############################################################
                rootLogger.info("")
                rootLogger.info("Start execution of PartB_Carrier_v2.py program")

                try:
                    sp_info = subprocess.run(['python3', 'PartB_Carrier_v2.py'], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info) 
                    
                except subprocess.CalledProcessError as e:
                    rootLogger.error(f"Calling PartB_Carrier_v2.py failed with return code {e.returncode}")
                    rootLogger.error(e.output)
                    
                    ## Send Failure email	
                    SUBJECT=f"PartB_Carrier_v2.py extract - Failed ({ENVNAME}{TESTEMAIL})"
                    MSG=f"Calling PartB_Carrier_v2.py failed with return code {e} "
                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info) 

                    sys.exit(12)  
                
                # call was successful
                rootLogger.info("Python script PartB_Carrier_v2.py completed successfully. ")


                ####################################################################
                # Concatenate PTB Carrier S3 files into a single file 
                # NOTE: Multiple files with suffix "n_n_n.txt.gz" are created. 
                #       Will concatenate them into single file.
                #
                # Example --> blbtn_clm_ex_20220922.084321.txt.gz_0_0_0.txt.gz 
                #         --> blbtn_clm_ex_20220922.084321.txt.gz
                ####################################################################
                rootLogger.info("")
                rootLogger.info("Concatenate S3 files using CombineS3FilesDriver.py") 

                PTB_CARR_BUCKET = f"{XTR_BUCKET}/{PTB_CARR_BUCKET_FLDR}"
                rootLogger.info(f"{PTB_CARR_BUCKET=} ")
                
                sConcatFilename = XTR_FILE_NAME
                rootLogger.info(f"{sConcatFilename=}")

                try:
                    #sp_info = subprocess.run(['python3', 'CombineS3FilesDriver.py', PTB_CARR_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True)
                    #write_sp_info_2_log(sp_info)
                    
                    CombineS3FilesDr.combineS3Files(s3BucketAndFldr=PTB_CARR_BUCKET, s3CombinedFilename=sConcatFilename)

                    # switch common functions back to using rootLogger
                    setCommonFunctionLogger(rootLogger)    
            
                #except subprocess.CalledProcessError as e:
                #    rootLogger.error(f"Calling CombineS3FilesDriver.py failed with return code {e.returncode}")
                #    rootLogger.error(e.stdout)
                #    rootLogger.error(e.stderr)

                except Exception as e:
                    rootLogger.error(f"Calling CombineS3FilesDriver.py failed with error: {e}")
                    
                    ## Send Failure email	
                    SUBJECT=f"Combining S3 files in PartB_Carrier_Driver.py - Failed ({ENVNAME})"
                    MSG=f"Combining S3 files in PartB_Carrier_Driver.py has failed."

                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info) 

                    sys.exit(12)   

            # end-for
        # end-for
  
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
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")

        # Send Success email	
        SUBJECT=f"PartB Carrier extract - completed ({ENVNAME}{TESTEMAIL})"
        MSG=f" The Extract for the creation of the PartB Carrier file from Snowflake has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error("\n%s", e.output)

            # Send Failure email
            SUBJECT=f"Sending Success email in PartB_Carrier_Driver.py - Failed ({ENVNAME})"
            MSG=f"Sending Success email in PartB_Carrier_Driver.py - Failed"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info("EFT PartB Carrier Extract Files ")
        rootLogger.info(f"{PTB_CARR_BUCKET=}")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', PTB_CARR_BUCKET ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT = f"PartB Carrier Extract EFT process - Failed ({ENVNAME}{TESTEMAIL})"
            MSG= f"PartB Carrier Claim Extract EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)  

        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script PartB_Carrier_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in PartB_Carrier_Driver.py\n {e}")

        rootLogger.error("Exception occured in PartB_Carrier_Driver.py.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT = f"Exception occured in PartB_Carrier_Driver.py - Failed ({ENVNAME}{TESTEMAIL})"
        MSG= f"Exception occured in PartB_Carrier_Driver.py {e}.  Process failed"

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)

        sys.exit(12)  


if __name__ == "__main__":
    
    main_processing_loop()

