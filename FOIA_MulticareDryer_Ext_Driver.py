#!/usr/bin/env python
############################################################################################################
# Name:  FOIA_MulticareDryer_Ext_Driver.py
#
# Desc: FOIA Multicare Dryer Extract
#
# Execute as python3 FOIA_MulticareDryer_Ext_Driver.py 
#
# 08/01/2025 Paul Baranoski   Created script.	
# Paul Baranoski   2025-09-26 Modify subprocess.run to subprocess.run which allows to capture stderr as well as stdout. 
#                             Add write_sp_info_2_log function and companion logging import module LoggerStandard. 
############################################################################################################
import boto3 
import logging
import sys

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess

# Our common module with variable constants
from SET_XTR_ENV import *

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

import CreateManifestFileDriver as CreManDr

# Our include members
import LoggerStandard as EnigmaLog

FOIA_BUCKET = rf"{XTR_BUCKET}/{FOIA_BUCKET_FLDR}"

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
    
def getConfigFileContents(sConfigFilename):
    
    s3ConfigFolder_n_filename = f"{CONFIG_BUCKET_FLDR}{sConfigFilename}"
    rootLogger.info(f"{s3ConfigFolder_n_filename=}")

    # Is config file in s3?         
    resp = s3_client.list_objects_v2(Bucket=XTR_BUCKET, Prefix=s3ConfigFolder_n_filename)
    
    if resp == None:
        ## Send Failure email	
        SUBJECT=f"FOIA_MulticareDryer_Ext_Driver.py - Failed ({ENVNAME})"
        MSG=f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
        
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
        write_sp_info_2_log(sp_info)        
        sys.exit(12)
    
    # Get config file from S3    
    ConfigFile = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3ConfigFolder_n_filename)

    if ConfigFile == None:
        ## Send Failure email	
        SUBJECT=f"FOIA_MulticareDryer_Ext_Driver.py - Failed ({ENVNAME})"
        MSG=f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
        
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
        write_sp_info_2_log(sp_info)  
        sys.exit(12)        


    # S3 Body is byte array. Convert byte array to utf-8 string. Splitlines recognizes "\r\n" as end-of-record markers     
    lstConfigRecs = ConfigFile["Body"].read().decode('utf-8').splitlines()
    rootLogger.info(f"{lstConfigRecs=}") 

    return lstConfigRecs
   
    
def main_processing_loop():

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME

        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}FOIA_MulticareDryer_Ext_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        
        rootLogger.info("################################### ")
        rootLogger.info(f"\nFOIA_MulticareDryer_Ext_Driver.py started at {TMSTMP} ")

        ###########################################################
        # Set working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck.  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        rootLogger.info(f"{pwd=}")


        #############################################################
        # Global constants  
        #############################################################
        FOIA_MulticareDryer_PARM_FILE = "FOIA_MulticareDryer_PARM_FILE.txt"
       
        rootLogger.info(f"{CONFIG_BUCKET_FLDR}")        
        rootLogger.info(f"{FOIA_MulticareDryer_PARM_FILE=}")

        rootLogger.info(f"{XTR_BUCKET=}")  
        rootLogger.info(f"{FOIA_BUCKET_FLDR=}")  


        #############################################################
        # Get S3 reference
        #############################################################
        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")
 

        #############################################################
        # Retrieve config file JIRA_Extract_Mappings.txt contents
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Retrieve parm file {FOIA_MulticareDryer_PARM_FILE} contents" )
        lstParmRecs = getConfigFileContents(FOIA_MulticareDryer_PARM_FILE)
        
        rootLogger.info("")
        rootLogger.info(f"{lstParmRecs=}")


        #############################################################
        # Process parameter records
        #############################################################        
        for sParmRec in lstParmRecs:
    
            #############################################################
            # Skip non-parm recs
            #############################################################
            rootLogger.info("")
            rootLogger.info("-----------------------------------")
            
            # Display Parameter file record
            rootLogger.info(f"{sParmRec=}")

            # skip blank lines
            if sParmRec.strip() == "":
                continue
		
            # skip comment lines
            if sParmRec[0:1] == '#':
                continue


            #################################################################################
            # Load parameter values from Parm record
            #################################################################################
            rootLogger.info("")
            
            lstParmFlds = sParmRec.split(",")
            
            CLM_TYPE_LIT = lstParmFlds[0]
            EXT_FROM_DT = lstParmFlds[1]
            EXT_TO_DT = lstParmFlds[2]
            FILE_LIT = lstParmFlds[3]
		
            rootLogger.info(f"{CLM_TYPE_LIT=}")
            rootLogger.info(f"{EXT_FROM_DT=}")
            rootLogger.info(f"{EXT_TO_DT=}")
            rootLogger.info(f"{FILE_LIT=}")
                
		
            #############################################################
            # Get claim-type codes
            #############################################################
            if CLM_TYPE_LIT == "HHA":
                CLM_TYPE_CODES = "10"
                PTA_PTB_SW = "A"
                SINGLE_FILE_PHRASE = "SINGLE=TRUE"

            elif CLM_TYPE_LIT == "HSP":
                CLM_TYPE_CODES = "50"
                PTA_PTB_SW = "A"
                SINGLE_FILE_PHRASE = "SINGLE=TRUE"

            elif CLM_TYPE_LIT == "SNF":
                CLM_TYPE_CODES = "20,30"
                PTA_PTB_SW = "A"
                SINGLE_FILE_PHRASE = "SINGLE=TRUE"

            elif CLM_TYPE_LIT == "INP":
                CLM_TYPE_CODES = "60"
                PTA_PTB_SW = "A"
                SINGLE_FILE_PHRASE="SINGLE=TRUE"

            elif CLM_TYPE_LIT == "OPT":
                CLM_TYPE_CODES = "40"
                PTA_PTB_SW = "A"
                SINGLE_FILE_PHRASE = ""

            elif CLM_TYPE_LIT == "CAR":
                CLM_TYPE_CODES = "71,72"
                PTA_PTB_SW = "B"
                SINGLE_FILE_PHRASE = ""	

            elif CLM_TYPE_LIT == "DME":
                CLM_TYPE_CODES = "81,82"
                PTA_PTB_SW = "B"
                SINGLE_FILE_PHRASE = "SINGLE=TRUE"
                
            else:
                
                rootLogger.info(f"Invalid claim type literal {CLM_TYPE_LIT} on parameter record.")
                    
                ## Send Failure email	
                SUBJECT=f"FOIA_MulticareDryer_Ext - Failed ({ENVNAME})"
                MSG=f"FOIA MulticareDryer extract has failed. \nInvalid claim type literal {CLM_TYPE_LIT} on parameter record."
                
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info) 

                sys.exit(12)   

		
            rootLogger.info(f"{CLM_TYPE_CODES=}")
            rootLogger.info(f"{PTA_PTB_SW=}")  
            rootLogger.info(f"{SINGLE_FILE_PHRASE=}")      


            #############################################################
            # Export environment variables for Python SQL code
            #############################################################
            os.environ["TMSTMP"] = TMSTMP
            os.environ["CLM_TYPE_LIT"] = CLM_TYPE_LIT
            os.environ["FILE_LIT"] = FILE_LIT
            os.environ["SINGLE_FILE_PHRASE"] =  SINGLE_FILE_PHRASE
            
            os.environ["CLM_TYPE_CODES"] = CLM_TYPE_CODES
            os.environ["EXT_FROM_DT"] = EXT_FROM_DT
            os.environ["EXT_TO_DT"] = EXT_TO_DT

            
            #############################################################
            # Execute Python code to Extract claims data.
            #############################################################
            rootLogger.info("")

            try:

                if PTA_PTB_SW == "A":
                    rootLogger.info("Start execution of DOJ_MulticareDryer_Ext_PTA.py program")
                    FOIA_MulticareDryer_Ext_py_PGM = "FOIA_MulticareDryer_Ext_PTA.py" 
                else:
                    rootLogger.info("Start execution of DOJ_MulticareDryer_Ext_PTB.py program")
                    FOIA_MulticareDryer_Ext_py_PGM = "FOIA_MulticareDryer_Ext_PTB.py" 

                sp_info = subprocess.run(['python3', FOIA_MulticareDryer_Ext_py_PGM], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling {FOIA_MulticareDryer_Ext_py_PGM} failed with return code {e.returncode}")
                rootLogger.error(e.output)
                
                ## Send Failure email	
                SUBJECT=f"FOIA_MulticareDryer_Ext - Failed ({ENVNAME})"
                MSG=f"FOIA_MulticareDryer_Ext_Driver.py has failed. "
                
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    
        

            rootLogger.info("")
            rootLogger.info(f"Python script {FOIA_MulticareDryer_Ext_py_PGM} completed successfully.")


            ####################################################################
            # Concatenate S3 files
            # NOTE: Multiple files with suffix "n_n_n.txt.gz" are created. 
            #       Will concatenate them into single file.
            #
            # Example --> blbtn_clm_ex_20220922.084321.txt.gz_0_0_0.txt.gz 
            #         --> blbtn_clm_ex_20220922.084321.txt.gz
            ####################################################################
            rootLogger.info("")
            rootLogger.info("Concatenate S3 files using CombineS3Files.sh") 

            rootLogger.info(f"{FOIA_BUCKET=} ")
            
            sConcatFilename=f"FOIA_MulticareDryer_EXTRACT_{CLM_TYPE_LIT}_{FILE_LIT}_{TMSTMP}.txt.gz"
            rootLogger.info(f"{sConcatFilename=}")

            try:
                sp_info = subprocess.run(['bash', 'CombineS3Files.sh', FOIA_BUCKET, sConcatFilename ], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling CombineS3Files.sh failed with return code {e.returncode}")
                rootLogger.error(e.output)
                
                ## Send Failure email	
                SUBJECT=f"Combining S3 files in FOIA_MulticareDryer_Ext_Driver.py - Failed ({ENVNAME})"
                MSG=f"Combining S3 files in FOIA_MulticareDryer_Ext_Driver.py has failed."

                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
                write_sp_info_2_log(sp_info) 

                sys.exit(12)    


  
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
       
        SUBJECT=f"FOIA MulticareDryer extract ({ENVNAME}) " 
        MSG=f"The Extract for the creation of the FOIA MulticareDryer data pull has completed.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            raise    


        #############################################################
        # Create Manifest file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Create Manifest file for FOIA_MulticareDryer_Ext_Driver.py extract. ")

        #####################################################
        # S3BUCKET --> points to location of extract file. 
        #          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
        # TMSTMP   --> uniquely identifies extract file(s) 
        # ENIGMA_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
        #
        # Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
        #####################################################
        BOX_RECIPIENTS="pbaranoski-con@index-analytics.com" 
                
        try:
            CreManDr.createManifestFile(bucket=XTR_BUCKET, s3folder=FOIA_BUCKET_FLDR, runToken=TMSTMP, BoxEmails="pbaranoski-con@index-analytics.com" )

            #s3BktNFldr = rf"{XTR_BUCKET}/{FOIA_BUCKET_FLDR}"
            
            #sp_info = subprocess.run(['bash', 'CreateManifestFile.sh', s3BktNFldr, TMSTMP, "pbaranoski-con@index-analytics.com"], text=True)
            #write_sp_info_2_log(sp_info)  
  

        except Exception as e:
        
            SUBJECT=f"FOIA MulticareDryer_Ext_Driver.py - create manifest file failed. ({ENVNAME}) " 
            MSG=f"FOIA MulticareDryer_Ext_Driver.py - create manifest file failed.\n\n {e}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)
                
            raise
 
 
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script FOIA_MulticareDryer_Ext_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in FOIA_MulticareDryer_Ext_Driver.py\n {e}")

        rootLogger.error("Exception occured in FOIA_MulticareDryer_Ext_Driver.py.")
        rootLogger.error(e)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()