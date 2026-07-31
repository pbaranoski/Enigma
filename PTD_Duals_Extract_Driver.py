#!/usr/bin/python
############################################################################################################
# Script Name: PTD_Duals_Extract_Driver.py
#
# Description: This script will extract dashboard info from extract scripts.
#
#  python3 PTD_Duals_Extract_Driver.py $1  
#   
#       $1 = M/D/H --> Monthly/Daily    
#
# Paul Baranoski 2025-10-20 Created script.
# Paul Baranoski 2025-11-06 Renamed function getStConfigFile to getConfigFile, and added bucket parameter to make
#                           the function more generic for reuse.
#                           Modified function downloadExtFileAndUnzip to add s3Bucket parameter to make function more generic.
# Paul Baranoski 2026-01-06 Added tempfile tempdir override to use our "data" folder instead of "tmp" directory which does not have enough space allocated 
#                           on linux server.
#                           Add logic to remove full extract temp file when processing for all states has completed.  
# Paul Baranoski 2026-02-03 Modify to Add "TESTING" functionality.
# Paul Baranoski 2026-06-18 Add CommonFunctions module, and remove duplicate hard-coded functions.
############################################################################################################
import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *


import os.path
import sys
from pathlib import Path
from datetime import datetime
from datetime import date,time,timedelta
from dateutil.relativedelta import relativedelta
import subprocess

import io
import re
import boto3

import gzip
import shutil
import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data"


# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

import LoggerStandard as EnigmaLog
from CommonFunctions import *


DATADIR = "/app/IDRC/XTR/CMS/data/"
LOGDIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

iTotalBytesTransferred = 0

s3UploadConfig = TransferConfig(multipart_chunksize=4 * 1024 * 1024,
                                multipart_threshold=4 * 1024 * 1024)


#############################################################
# Functions
#############################################################
        
"""    
def s3UploadFile(s3_client, sLocalPathNFilename, sBucket, sKeyPathNFilename):

    rootLogger.info(f"Upload file to s3 bucket {sBucket} and key {sKeyPathNFilename} ")
    
    s3_client.upload_file(sLocalPathNFilename, sBucket, sKeyPathNFilename, Config=s3UploadConfig, Callback=UploadFileProgress, ExtraArgs={'ContentType': 'text/plain'} )

"""    


def buildSQLStInPhrase(lstConfigRecs):

    ###########################################################################################################
    # Example of config file
    ###########################################################################################################
    #  |001|006|009|029|049|057|058|066|074|093|095|110|112|113|114|115|125|128|130|165|166|167|168|169|182|195
    ##########################################################################################################
    #AZ|XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |   |XX |XX |XX |XX 
    #CA|XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX
    #ZY|001|006|009|029|049|057|058|066|074|093|095|110|112|113|114|115|125|128|130|165|166|167|168|169|182|195
    #ZZ|005|003|020|020|008|001|008|008|019|002|015|002|001|001|001|010|003|002|035|001|001|001|001|013|013|011
    
    rootLogger.info("")
    rootLogger.info("Create State In-Phrase parameter for the Python Extract program.")
    rootLogger.info("")
    
    lstStates = []
    
    for configRec in lstConfigRecs:
        rootLogger.debug(configRec) 

        # Skip comments
        if configRec[0:1] == "#":
            continue

        elif configRec[0:1] == "Z":
           continue

        else:
            lstStates.append(configRec[0:2])

    rootLogger.info(f"\n{lstStates=}")
    
    #########################################################
    # 1) Add quotes aound states, and add commas between states
    # 2) Add beginning and ending single quotes.
    #########################################################
    sSQLStInPhrase = "'" + "','".join(lstStates) + "'"
    rootLogger.info(f"{sSQLStInPhrase=}")    

    return sSQLStInPhrase 
    
        
def buildStFldDisplayRules(lstConfigRecs):

    ###########################################################################################################
    # Example of config file
    ###########################################################################################################
    #  |001|006|009|029|049|057|058|066|074|093|095|110|112|113|114|115|125|128|130|165|166|167|168|169|182|195
    ##########################################################################################################
    #AZ|XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |   |XX |XX |XX |XX 
    #CA|XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX
    #CT|XX |XX |XX |   |   |   |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |   |XX |   |   |XX |XX |XX
    #IL|XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX
    #IN|XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX |XX   
    #ZY|001|006|009|029|049|057|058|066|074|093|095|110|112|113|114|115|125|128|130|165|166|167|168|169|182|195
    #ZZ|005|003|020|020|008|001|008|008|019|002|015|002|001|001|001|010|003|002|035|001|001|001|001|013|013|011

    ########################################################################
    # define variables
    ########################################################################    
    lstFldDataTypes = []
    lstFldStartPos = []
    lstFldLen = []
   
    lstAllStFldsDisplayInd  = []

    ########################################################################
    # Parse config file into: 
    #   1) List of flds and their attributes: 
    #      a) Data Type (X/9) (Alpha/numeric) --> convert to actual replacement char
    #      b) Fld Start Pos (1-based index) --> convert to 0-based index 
    #      c) field length 
    #      d) Create List of End Positions 
    #   2) List/array of States containing list of display/non-display fld indicators
    ########################################################################
    for configRec in lstConfigRecs:
        rootLogger.debug(configRec) 

        # Skip comments
        if configRec[0:1] == "#":
            continue
            
        elif configRec[0:2] == "ZX":
            # Skip the 'ZX'; convert 'X' and '9' to actual char; insert 'ZX' back.
            lstFldDataTypes = [ (b' ' if (fldType.strip() == 'X') else b'0')  for fldType in configRec.split("|")[1:] ]
            lstFldDataTypes.insert(0,'ZX')
            rootLogger.info(f"\n{lstFldDataTypes=}")

        elif configRec[0:2] == "ZY":
            # Skip the 'ZY'; convert StartPos to 0-based index; insert 'ZY' back.
            lstFldStartPos = [ (int(sfldPos) - 1 ) for sfldPos in configRec.split("|")[1:] ]
            lstFldStartPos.insert(0,'ZY')
            rootLogger.info(f"\n{lstFldStartPos=}")

        elif configRec[0:2] == "ZZ":
            # Skip the 'ZZ'; convert Fldlen to int; insert 'ZZ' back.
            lstFldLen = [ int(sFldLen) for sFldLen in (configRec.split("|")[1:]) ]
            lstFldLen.insert(0,'ZZ')
            rootLogger.info(f"\n{lstFldLen=}")
        else:
            # State config rec and list of states to display or maske/hide
            lstStFldsDisplayInd = configRec.split("|")
            rootLogger.info(f"\n{lstStFldsDisplayInd=}")
            lstAllStFldsDisplayInd.append(lstStFldsDisplayInd)

    ########################################################################
    # Create End-Pos List
    # Ex. ['ZYZZ', 5, 8, 28, 48, 56, 57, 65, 73, 92, 94, 109, 111, 112, 113, 114, 124, 127, 129, 164, 165, 166, 167, 168, 181, 194, 205]
    ########################################################################
    rootLogger.info("")
    rootLogger.info("Create End Position list")
    lstFldEndPos = [StartPos + lstFldLen[i] for i, StartPos in enumerate(lstFldStartPos)]

    ########################################################################
    # Last item in lstFldEndPos is length of output file
    ########################################################################
    iOutputRecLength = lstFldEndPos[len(lstFldEndPos) - 1]
    rootLogger.info(f"{iOutputRecLength=}")

    ########################################################################
    # Combine fld attributes into single list
    #  NOTE: DO NOT REMOVE first element which is ('ZX','ZY','ZZ','ZYZ')
    ########################################################################
    rootLogger.info("Combine fld attributes into single list")
    
    lstFldAttrs = list(zip(lstFldStartPos, lstFldEndPos, lstFldLen, lstFldDataTypes))

    rootLogger.info(f"{lstFldAttrs=}")

    ########################################################################
    # Create State Dictionary containing list non-display (masked) flds only
    ########################################################################
    rootLogger.info("")
    rootLogger.info("Create new State list containing list non-display (masked) flds only")

    dictAllStatesNFlds2Init = {}

    # Create list of states and their FldDisplayIndicators
    for lstStFlds in lstAllStFldsDisplayInd:

        rootLogger.debug(lstStFlds)
            
        lstStFlds2BInit = []

        for i, StFld in enumerate(lstStFlds):
            # the first element is the State abreviation
            if i == 0:           
                sStAbrv = StFld
                #lstStFlds2BInit.append(sStAbrv)
                rootLogger.info(f"{sStAbrv=}")
                
            elif StFld.strip() == "":
                # Field needs to be initialized
                lstStFlds2BInit.append(lstFldAttrs[i])

        rootLogger.info(lstStFlds2BInit)

        # Create Dictionary entry for easy look-up
        dictAllStatesNFlds2Init[sStAbrv] = lstStFlds2BInit 
        rootLogger.debug(dictAllStatesNFlds2Init)
        #rootLogger.debug(dictAllStatesNFlds2Init['CT'])

    ########################################################################
    # Return dictionary and Output Record Length
    ########################################################################
    return dictAllStatesNFlds2Init, iOutputRecLength


def downloadExtFileAndUnzip(s3_client, S3BUCKET, s3ExtractFileKey):    

    ################################################################
    # Download s3Extract gz file. Download does not have 5GB limit.
    # Create temporary file to store it.
    #
    # Note: The "Delete=false" --> Temp file is not automatically deleted.
    #       Must manually delete when done with file.  
    ################################################################
    with tempfile.NamedTemporaryFile(delete=False) as tmp_gz:
        gzExtFilePath = tmp_gz.name
        rootLogger.info(f"Starting download of {s3ExtractFileKey} to {gzExtFilePath}")
        
        s3_client.download_file(S3BUCKET, s3ExtractFileKey, gzExtFilePath)
        rootLogger.info(f"The download of file {s3ExtractFileKey} to {gzExtFilePath} has completed.")
        
    ################################################################
    # Unzip downloaded s3 Extract file.
    ################################################################
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_unzipped:
        unzippedExtFilePath = tmp_unzipped.name
        rootLogger.info(f"Unzipping {s3ExtractFileKey} to {unzippedExtFilePath}")

        ############################################################
        # When you use gzip.open() in read mode ('r' or 'rb'), it 
        #    automatically decompresses (unzips) the .gz file
        #    as you read from it.        
        #############################################################
        with gzip.open(gzExtFilePath, 'rb') as fgzExtFile:
            
            # copyfileobj --> copies data from one file-like object to another in chunks
            # Note: 3rd parm is chunk size. Default (no parm) is 16 KB which is OK for small files.
            #       For large files --> 1 MB to 4MB is most efficient. 
            #       1 MB chunk = (1024 * 1024); 4 MB = (4096 * 1024) 
            shutil.copyfileobj(fgzExtFile, tmp_unzipped, length=4096*1024 )

            rootLogger.info(f"Unzipping to temp file has completed.")
        
    ################################################################
    # Delete Temporary file that stored extract .gz file
    ################################################################
    rootLogger.info(f"Removing temp file {gzExtFilePath}")
    os.remove(gzExtFilePath)

    ################################################################
    # Return temp unzipped file
    ################################################################            
    return unzippedExtFilePath
 

def processCompletedState(s3_client, sPrevStCD, tmpStateFile, S3_BUCKET_FLDR, sExtStGzFilename, iRecCount): 

    ############################################################
    # Add "RECORD COUNT" trailer record to each state file.
    # Format record count info as 10 digit number with leading 
    # zeroes like MF file
    ############################################################
    bTrailer = f"RECORD COUNT {str(iRecCount).zfill(10)}\n".encode('utf-8')
    tmpStateFile.write(bTrailer)

    # close temp file
    tmpStateFile.close()

    # upload zipped file to s3
    tmpStateFilePath = tmpStateFile.name
    
    # zip State file
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmpStateFileZipgz:
        tmpStateFileZipgzPath = tmpStateFileZipgz.name
        
        with open(tmpStateFilePath, 'rb') as f_in:
            with gzip.GzipFile(fileobj = tmpStateFileZipgz, mode='wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


    # create state filename
    s3StExtractFileKey = S3_BUCKET_FLDR + sExtStGzFilename
    
    s3UploadFile(s3_client, tmpStateFileZipgzPath, XTR_BUCKET, s3StExtractFileKey)
    
    # get State temp .txt and .gz sizes in bytes 
    iStUnzippedByteSize = os.path.getsize(tmpStateFilePath)
    iStGzByteSize = os.path.getsize(tmpStateFileZipgzPath)
    
    # Get unzipped and zipped byte sizes for Dashboard Info: space is delimiter between fields
    sFileByteSizes = f"{iStUnzippedByteSize} {iStGzByteSize}"
    
    # remove temp file from operating system   
    rootLogger.info(f"Removing temp file {tmpStateFilePath}")    
    rootLogger.info(f"Removing temp file {tmpStateFileZipgzPath}")    

    os.remove(tmpStateFilePath) 
    os.remove(tmpStateFileZipgzPath)  
    
    return sFileByteSizes


def getVar_CLNDR_CY_MO_NUM_ENDDT(PROCESSING_TYPE):

    rootLogger.info("")
    rootLogger.info("Create CLNDR_CY_MO_NUM_ENDDT date parameter for the Python Extract program.")

    if PROCESSING_TYPE == "M":
        # Current Date + 2 day - 8 years -> format date 'YYYYMM' --> +2 ensures we find a date in the next month for formula to work correctly
        # CLNDR_CY_MO_NUM_ENDDT=`date -d "+2 day - 8 year" +%Y%m`

        # Step 1: move to first day of next month
        dttmFirstDayNextMon = (date.today().replace(day=1) + relativedelta(months=1))
        # Step 2: subtract 8 years
        dttmEightYearAgo = dttmFirstDayNextMon - relativedelta(years=8)
        # Step 3: format as YYYYMM
        CLNDR_CY_MO_NUM_ENDDT = dttmEightYearAgo.strftime("%Y%m")

    else:
        #Current Date - 1 month
        #CLNDR_CY_MO_NUM_ENDDT=`date -d "-1 month" +%Y%m`
        dttmFirstDayNextMon = (date.today().replace(day=1) + relativedelta(months=-1))
        CLNDR_CY_MO_NUM_ENDDT = dttmFirstDayNextMon.strftime("%Y%m")

        
    rootLogger.info(f"{CLNDR_CY_MO_NUM_ENDDT=}")
    
    return CLNDR_CY_MO_NUM_ENDDT


def main_processing_loop():
    
    try:    
       
        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        print(f"{TMSTMP=}")

        global LOGNAME
        LOGNAME = f"{LOGDIR}{TESTLOG}PTD_Duals_Extract_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPTD_Duals_Extract_Driver.py started at {TMSTMP}")

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
        # Get any parameters
        ##########################################
        rootLogger.info("Getting Parameters ")
        
        ##################################################################
        # Expecting 1 parameter: (D)aily or (M)onthly
        ##################################################################
        iNOFParms = len(sys.argv) - 1
        if not (iNOFParms == 1 ):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)
           
        ##########################################
        # Get Processing type: Daily or Monthly
        ##########################################
        lstParms = sys.argv
        PROCESSING_TYPE = lstParms[1]
        
        rootLogger.info(f"Parameters to script: {PROCESSING_TYPE}")

        #################################################################################
        # Create CLNDR_CY_MO_NUM_ENDDT parameter date (YYYYMM)
        #################################################################################
        CLNDR_CY_MO_NUM_ENDDT = getVar_CLNDR_CY_MO_NUM_ENDDT(PROCESSING_TYPE)

        
        #################################################################################
        # Create Submission Date parameters (YYYY-MM-DD)
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Create Claim Submission Start and End date parameters for the Python Extract program.")

        if PROCESSING_TYPE == "M":
            # Process for entire prior month; cur_MM - 1 day -> last day of prior month
            #CLM_SUBMSN_DT_START_DT=`date -d "$(date +%Y-%m-01) - 1 day" +%Y-%m-01`
            #CLM_SUBMSN_DT_END_DT=`date -d "$(date +%Y-%m-01) - 1 day" +%Y-%m-%d`
            
            CLM_SUBMSN_DT_START_DT = (date.today().replace(day=1) + timedelta(days=-1)).strftime("%Y-%m-01")
            CLM_SUBMSN_DT_END_DT = (date.today().replace(day=1) + timedelta(days=-1)).strftime("%Y-%m-%d")
            
            # create variables used in filename
            YYYY = CLM_SUBMSN_DT_END_DT[0:4]
            MM = CLM_SUBMSN_DT_END_DT[5:7]
            
            rootLogger.info(f"{YYYY=}")
            rootLogger.info(f"{MM=}")
        else:
            ONE_DAY_AGO = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
            THREE_DAYS_AGO = (date.today() + timedelta(days=-3)).strftime("%Y-%m-%d")
            DOW = date.today().strftime("%A")

            # Daily CDS.CLM_SUBMSN_DT filter --> Prior day for Tue-Fri; (Fri, Sat, Sun) for Mon run.
            CLM_SUBMSN_DT_END_DT = ONE_DAY_AGO
            if DOW == "Monday":
                CLM_SUBMSN_DT_START_DT = THREE_DAYS_AGO
            else:
                CLM_SUBMSN_DT_START_DT = ONE_DAY_AGO
 
 
            # create variables used in filename
            YYMMDD = CLM_SUBMSN_DT_START_DT.replace("-","")[2:8]
            
            rootLogger.info(f"{YYMMDD=}")


        # Examples:
        #CLM_SUBMSN_DT_START_DT = '2023-09-01'
        #CLM_SUBMSN_DT_END_DT = '2023-09-30'
        #CLNDR_CY_MO_NUM_ENDDT = '202212'
	
        rootLogger.info(f"{CLM_SUBMSN_DT_START_DT=}")
        rootLogger.info(f"{CLM_SUBMSN_DT_END_DT=}")


        #############################################################
        # Set variables to appropriate values for monthly/daily processing.
        #############################################################
        MONTHLY_ST_PARMFILE = "PTDDualsMonthlyStParms.txt"
        DAILY_ST_PARMFILE = "PTDDualsDailyStParms.txt"

        if PROCESSING_TYPE == "M":
            rootLogger.info("")
            rootLogger.info("Monthly processing started.")
            rootLogger.info("")
            
            EXTRACT_TYPE = "Monthly"
            SNOWFLAKE_STG = "PTDDUALMNTH_STG"

            S3_BUCKET_FLDR = PTDDUALMNTH_BUCKET_FLDR 
            ST_PARMFILE = MONTHLY_ST_PARMFILE
            
            ST_EXT_FNAME_MODEL = f"PTDDUALS_MONTHLY_XX_Y{YYYY}M{MM}_{TMSTMP}.txt"
            S3_EXTRACT_FILE = f"PTDDUALS_MONTHLY_Y{YYYY}M{MM}_{TMSTMP}.csv.gz"
            S3_EXT_ST_FILE=f"PTDDUALS_MONTHLY_XX_Y{YYYY}M{MM}_{TMSTMP}.txt.gz"
            
            PTDDUAL_EMAIL_SENDER = CMS_EMAIL_SENDER
            PTDDUAL_EMAIL_SUCCESS_RECIPIENT = PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT
            PTDDUAL_EMAIL_FAILURE_RECIPIENT = ENIGMA_EMAIL_FAILURE_RECIPIENT

            # Used in success email - double braces will display the brace in the string (escape)
            EFT_FILEMASK = f"P#EFT.ON.G{{ST}}.IDRPD.Y{YYYY}M{MM}.{{TIMESTAMP}}"

        else:
            rootLogger.info("")
            rootLogger.info("Daily processing started.")
            rootLogger.info("")

            EXTRACT_TYPE = "Daily"
            SNOWFLAKE_STG = "PTDDUALDLY_STG"
                
            S3_BUCKET_FLDR = PTDDUALDAILY_BUCKET_FLDR 
            ST_PARMFILE = DAILY_ST_PARMFILE

            ST_EXT_FNAME_MODEL = f"PTDDUALS_DAILY_XX_R{YYMMDD}_{TMSTMP}.txt"
            S3_EXTRACT_FILE = f"PTDDUALS_DAILY_R{YYMMDD}_{TMSTMP}.csv.gz"
            S3_EXT_ST_FILE=f"PTDDUALS_DAILY_XX_R{YYMMDD}_{TMSTMP}.txt.gz"

            
            PTDDUAL_EMAIL_SENDER = CMS_EMAIL_SENDER
            PTDDUAL_EMAIL_SUCCESS_RECIPIENT = PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT	
            PTDDUAL_EMAIL_FAILURE_RECIPIENT = ENIGMA_EMAIL_FAILURE_RECIPIENT

            # Used in success email
            EFT_FILEMASK = f"P#EFT.ON.G{{ST}}.IDRPD.R{YYMMDD}.{{TIMESTAMP}}"


        #############################################################
        # Display variable values
        #############################################################
        rootLogger.info(f"{CONFIG_BUCKET_FLDR}")
        rootLogger.info(f"{S3_BUCKET_FLDR=}") 
        rootLogger.info(f"{ST_PARMFILE=}")
        rootLogger.info(f"{ST_EXT_FNAME_MODEL=}")
        rootLogger.info(f"{S3_EXTRACT_FILE}")

        rootLogger.info(f"{PTDDUAL_EMAIL_SENDER=}")
        rootLogger.info(f"{PTDDUAL_EMAIL_SUCCESS_RECIPIENT=}")
        rootLogger.info(f"{PTDDUAL_EMAIL_FAILURE_RECIPIENT=}")


        #############################################################
        # Get S3 reference
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")
    
        ##################################################################
        # Retrieve config file from S3 (copy)
        ##################################################################
        s3ConfigFolder_n_filename = CONFIG_BUCKET_FLDR + ST_PARMFILE

        lstConfigRecs = getConfigFile(s3_client, XTR_BUCKET, s3ConfigFolder_n_filename)    

        ##################################################################
        # Create State IN-Phrase for Extract SQL 
        ##################################################################
        sSQLStInPhrase = buildSQLStInPhrase(lstConfigRecs)

        #############################################################
        # Execute Python code to extract data.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Set Environment variables to be used in PTD_Duals_Extract.py")

        # Export environment variables for Python code
        os.environ["TMSTMP"]  = TMSTMP
        os.environ["SNOWFLAKE_STG"] = SNOWFLAKE_STG
        os.environ["S3_EXTRACT_FILE"] = S3_EXTRACT_FILE
        os.environ["CLNDR_CY_MO_NUM_ENDDT"] = CLNDR_CY_MO_NUM_ENDDT
        os.environ["CLM_SUBMSN_DT_START_DT"] = CLM_SUBMSN_DT_START_DT
        os.environ["CLM_SUBMSN_DT_END_DT"] = CLM_SUBMSN_DT_END_DT
        os.environ["STATE_IN_PHRASE"] = sSQLStInPhrase

        #################################################################################
        # Execute Python code to Run SQL and load Extract data to SF
        #################################################################################
        rootLogger.info("Start execution of PTD_Duals_Extract.py program")

        try:
            sp_info = subprocess.run(['python3', 'PTD_Duals_Extract.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling PTD_Duals_Extract.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Python program PTD_Duals_Extract.py - Failed ({ENVNAME})"
            MSG=f"Python program PTD_Duals_Extract.py failed. \n {e.output}"

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        rootLogger.info("")
        rootLogger.info("Python script DashboardInfo_MS.py completed successfully.")


        #############################################################
        # Were zero rows extracted?
        #############################################################
        rootLogger.info("")
        rootLogger.info("Get S3 Extract file list and record counts")

        # get Extract filename and Record count
        S3Files = getExtractFilenamesAndCounts(rootLogger, LOGNAME)
        
        #Ex. PTDDUALS_MONTHLY_Y2025M08_20250915.093946.csv.gz  51,234
        sExtFilenameZipped = S3Files.split()[0]
        #Ex. "1,234" --> "1234" --> int: 1234
        iNOF_ROWS = int(S3Files.split()[1].replace(",","")) 
        rootLogger.info(f"{iNOF_ROWS=}")


        #############################################################
        # Daily processing doesn't always have data to extract
        # --> end gracefully if there is no data to extract.
        # Monthly processing should extract data
        # --> no extracted data should be "hard error"
        #############################################################
        if iNOF_ROWS == 0:
            if PROCESSING_TYPE == "D":

                rootLogger.info("")
                rootLogger.info("Python script PTD_Duals_extract.py - No data available") 

                ## Send Failure email	
                SUBJECT=f"PTD Duals {EXTRACT_TYPE} Extract  - No data available. ({ENVNAME}{TESTEMAIL})"
                MSG=f"PTD Duals {EXTRACT_TYPE} Extract  - No data available."

                # Send No data available email	                
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PTDDUAL_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 

                rootLogger.info("")
                rootLogger.info("PTD_Duals_Extract.sh completed successfully.")
                rootLogger.info(f"\nEnded at {TMSTMP}" )

                sys.exit(0)

            else:
                rootLogger.info("")
                rootLogger.info("Python script PTD_Duals_extract.py failed - No data extracted") 

                ## Send Failure email	
                SUBJECT=f"PTD Duals Extract - Failed ({ENVNAME})"
                MSG=f"PTD Duals extract failed. No data extracted."
                
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info) 
            
                sys.exit(12)	


        ##################################################################
        # Build State Field Display rules - accept 2 return values
        ##################################################################
        dictAllStatesNFlds2Init, iOutputRecLength = buildStFldDisplayRules(lstConfigRecs)

        ##################################################################
        # Download Extract .gz file from s3 and unzip it into temp file.
        # NOTE: Contains data for all states.
        ##################################################################
        s3ExtractFileKey = S3_BUCKET_FLDR + S3_EXTRACT_FILE
        
        unzippedExtFilePath = downloadExtFileAndUnzip(s3_client, XTR_BUCKET, s3ExtractFileKey)            


        ##################################################################
        # Split unzipped Extract file into multiple state files,
        #  and mask specific fields depending on the State field display rules.
        ##################################################################
        swFirstRec = True
        iRecCount = 0
        lstDashboardRecs = []
        
        with open(unzippedExtFilePath, 'rb') as fExtFile:
            
            for bExtRec in fExtFile:

                ###############################################
                # Convert record contents to bytearray  
                ###############################################
                baExtRec = bytearray(bExtRec)

                ###############################################                
                # Get State Abrev for record
                ###############################################
                bStCD = bExtRec[220:222]

                ############################################### 
                # First-time thru set variables
                ###############################################                 
                if swFirstRec:
                    swFirstRec = False
                    swNewState = True
                    bPrevStCD = bStCD

                ############################################### 
                # write completed state file
                ###############################################   
                if bPrevStCD != bStCD:
                    sPrevStCD = bPrevStCD.decode('utf-8')
                    
                    rootLogger.info(f"Processed all extract file records for {sPrevStCD}. ")
                    
                    sExtStGzFilename = S3_EXT_ST_FILE.replace("XX",sPrevStCD)
                                        
                    sFileByteSizes = processCompletedState(s3_client, sPrevStCD, tmpStateFile, S3_BUCKET_FLDR, sExtStGzFilename, iRecCount)
                    
                    rootLogger.info(f"Processing complete for {sPrevStCD}. ")

                    # Build Dashboard extract info: 
                    sDashboardRec = f"DASHBOARD_INFO: {sExtStGzFilename} {iRecCount} {sFileByteSizes} "
                    lstDashboardRecs.append(sDashboardRec)
                    
                    swNewState = True


                ############################################### 
                # Perform initialization for new state file
                ############################################### 
                if swNewState:
                    swNewState = False
                    sStCD = bStCD.decode('utf-8')

                    rootLogger.info(f"Start processing State: {sStCD}")

                    # Init Prev St to current St
                    bPrevStCD = bStCD
                    iRecCount = 0

                    # Get new State's list of flds to mask
                    rootLogger.info(f"Get lstFlds2Init")
                    lstFlds2Init = dictAllStatesNFlds2Init[sStCD] 
                    rootLogger.info(f"{lstFlds2Init=}")
                    
                    # Create temporary file to write new state records to        
                    rootLogger.info(f"Create temp file for new state")
                    tmpStateFile = tempfile.NamedTemporaryFile(delete=False, mode='wb') 
                    tmpStateFilePath = tmpStateFile.name
                    
                
                ############################################### 
                # Count NOF recs for each state
                ###############################################
                iRecCount += 1
                
                ############################################### 
                # Iterate thru fields to mask flds on record
                ############################################### 
                # Ex.  [(28, 48, 20, b' '), (48, 56, 8, b'0'), (56, 57, 1, b'0'), (164, 165, 1, b' '), (166, 167, 1, b' '), (167, 168, 1, b' ')]       
                ############################################### 
                for fld2Init in lstFlds2Init:
                    
                    # Load fld attributes    
                    fldStartPos = fld2Init[0]
                    fldEndPos = fld2Init[1]
                    fldLen  = fld2Init[2]
                    fldMaskValue = fld2Init[3] 
                    
                    # initialize fld
                    baExtRec[fldStartPos : fldEndPos] = fldMaskValue * fldLen

                ###############################################
                # Write current record  
                ###############################################
                tmpStateFile.write(baExtRec[:iOutputRecLength]+b'\n')  


      
        #############################################################
        # Process last completed state (after EOF)
        #############################################################
        rootLogger.debug(f"{iRecCount=}")
        
        if iRecCount > 0:
            sPrevStCD = bPrevStCD.decode('utf-8')

            rootLogger.info(f"Processed all extract file records for {sPrevStCD}. ")
            
            sExtStGzFilename = S3_EXT_ST_FILE.replace("XX",sPrevStCD)
                    
            sFileByteSizes = processCompletedState(s3_client, sPrevStCD, tmpStateFile, S3_BUCKET_FLDR, sExtStGzFilename, iRecCount)

            rootLogger.info(f"Processing complete for {sPrevStCD}. ")

            # Build Dashboard extract info: 
            sDashboardRec = f"DASHBOARD_INFO: {sExtStGzFilename} {iRecCount} {sFileByteSizes} "
            lstDashboardRecs.append(sDashboardRec)


        #############################################################
        # Remove temp file of full extract.
        #############################################################
        rootLogger.info(f"Removing temp file {unzippedExtFilePath}")
        os.remove(unzippedExtFilePath)


        #############################################################
        # Write Dashboard Info records to log file
        #############################################################
        rootLogger.info("")
        sDashboardLogMsg = "\n".join(lstDashboardRecs)
        rootLogger.info("\n%s", sDashboardLogMsg)

        
        #############################################################
        # Build list of files and record counts for email
        # Ex. 'DASHBOARD_INFO: PTDDUALS_MONTHLY_AZ_Y2024M08_{TMSTMP}.txt.gz 8765 41234222 567833'
        # --> 'PTDDUALS_MONTHLY_AZ_Y2024M08_{TMSTMP}.txt.gz 8765'
        #############################################################
        lstFilenameNRecCount = [ sDashboardInfo.split(" ")[1:3] for sDashboardInfo in lstDashboardRecs ]
        rootLogger.info(f"{lstFilenameNRecCount=}")

        # Format for email display
        lstEmailFilenameRecCountInfo = [ f"{filename:<30} {int(RecCount):>14,}"  for filename, RecCount in lstFilenameNRecCount]
        rootLogger.info(f"{lstEmailFilenameRecCountInfo=}")

        S3Files = "\n".join(lstEmailFilenameRecCountInfo)

        #############################################################
        # Archive Full Extract file - not sent to states
        #############################################################
        rootLogger.info("")
        rootLogger.info("Moving S3 All States Extract file from {s3ExtSourceKey} to {s3ExtDestinationKey}")

        s3ExtSourceKey = S3_BUCKET_FLDR + S3_EXTRACT_FILE
        s3ExtDestinationKey = S3_BUCKET_FLDR + "archive/" + S3_EXTRACT_FILE
        
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, s3ExtSourceKey, s3ExtDestinationKey)


        #############################################################
        # Send Success email.
        #############################################################
        rootLogger.info("")
        rootLogger.info("Send success email with S3 Extract filename.")
        rootLogger.info(f"{S3Files=}")
       
        SUBJECT=f"PartD Duals {EXTRACT_TYPE} extract ({ENVNAME}{TESTEMAIL})" 
        MSG=f"The Extract for the creation of the PartD Duals {EXTRACT_TYPE} file(s) from Snowflake has completed.\n\nEFT versions of the below files were created using the following file mask {EFT_FILEMASK}.\n\nThe following file(s) were created:\n\n{S3Files}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, PTDDUAL_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    


        #############################################################
        # EFT Extract files
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"EFT PTD Duals {EXTRACT_TYPE} Extract Files  ")
        
        try:
            sp_info = subprocess.run(['bash', 'ProcessFiles2EFT.sh', rf"{XTR_BUCKET}/{S3_BUCKET_FLDR}" ], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling ProcessFiles2EFT.sh failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"PTD Duals {EXTRACT_TYPE} EFT process  - Failed ({ENVNAME})"
            MSG=f"PTD Duals {EXTRACT_TYPE} EFT process has failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    

        
        #############################################################
        # end script
        #############################################################
        rootLogger.info("")
        rootLogger.info("PTD_Duals_Extract_Driver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in PTD_Duals_Extract_Driver.py\n {e}")

        rootLogger.error("Exception occured in DashboardInfo_MS_Driver.")
        rootLogger.error("\n%s", str(e))

        ## Send Failure email	
        SUBJECT=f"PTD_Duals_Extract_Driver.py  - Failed ({ENVNAME})"
        MSG=f"Exception occured in PTD_Duals_Extract_Driver.py {e}. Process failed. "
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)        

        sys.exit(12)  
        
        
if __name__ == "__main__":

    main_processing_loop()