#!/usr/bin/python
############################################################################################################
# Script Name: DashboardInfo_MS_Driver.py
#
# Description: This script will extract dashboard info from extract scripts.
#
#  python3 DashboardInfo_MS_Driver.py $1 python3 DashboardInfo_MS_Driver.py $1 $2  
#      $1 --> Run Mode: Y, T --> (Y)esterday; (T)oday; 
#
#  For On-demand with override date range
#      $1 --> dttmRunFromDt (YYYYMMDD format) (Optional)
#      $2 --> dttmRunToDt   (YYYYMMDD format) (Optional)
#
# Paul Baranoski 2025-10-06 Created script.
# Paul Baranoski 2025-10-16 Added "global TOT_WARNINGS" to ExtractFilenamesAndCountsLegacy function.
# Paul Baranoski 2025-10-27 Add code to remove 'extract' all lower-case from log filename to create extract Key for Dashboard.
# Paul Baranoski 2025-10-29 Modified logic to accept a new "Mode" parameter for Yesterday or Today. The purpose is to 
#                           be able to provide more up-to-date updates to Dashboard tables.
# Paul Baranoski 2025-12-11 Modify convertBytes2ReadableSize to not include decimal places when size is bytes.
# Paul Baranoski 2026-01-02 Bypass DashboardVolReports log files for capturing Dashboard information.
# Paul Baranoski 2026-01-06 Modify Recipient email constant for success email.
# Paul Baranoski 2026-01-13 Add code to not process "TESTING_" log files.
# Paul Baranoski 2026-03-06 Add code to not process "CleanUp" log files.
# Paul Baranoski 2026-03-16 Add code to not process "EFT_Files_" log files.
# Paul Baranoski 2026-03-23 Add import of CommonFunctions. Remove hard-coded common functions from program.
# Paul Baranoski 2026-06-30 Add code to not process "DSH_Extract_ArchiveFiles" log files.
############################################################################################################

import os
import os.path
import sys
from pathlib import Path
from datetime import datetime
from datetime import date,time,timedelta
import subprocess

import io
import re
import boto3
import json
from collections import deque

# Our common module with variable constants
from SET_XTR_ENV import *

DATADIR = "/app/IDRC/XTR/CMS/data/"
LOGDIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

import LoggerStandard as EnigmaLog
from CommonFunctions import * 


# bytes pretty-printing
UNITS_MAPPING = [
    (1<<50, ' PB'),
    (1<<40, ' TB'),
    (1<<30, ' GB'),
    (1<<20, ' MB'),
    (1<<10, ' KB'),
    (1, (' byte', ' bytes')),
]


#############################################################
# Functions
#############################################################
def egrep(fFileNPath, pattern, ignore_case=False):
   
    flags = re.IGNORECASE if ignore_case else 0
    regex = re.compile(pattern, flags)

    lstMatchedLines = []
    
    with open(fFileNPath,"r") as fLogFile:
        for line in fLogFile:
            if regex.search(line):
                lstMatchedLines.append(line.strip())
                
    return lstMatchedLines            
  
    
def head(fFileNPath, iFirstNOFRecs): 
    
    with open(fFileNPath,"r") as fFile:
        lstHead = []
        for _ in range(iFirstNOFRecs):
            try:
                lstHead.append(next(fFile))
                
            except StopIteration:
                break

        return lstHead
                

def tail(fFileNPath, iLastNOFRecs): 
    
    with open(fFileNPath,"r") as fFile:
        lstTail = deque(fFile, maxlen=iLastNOFRecs)

        return lstTail  


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
    
    
def getExtractFilenamesAndCounts(P_LOGNAME):
    
    rootLogger.info("")
    rootLogger.info(f"logname parameter: {P_LOGNAME}")

    ################################################
    # Create regex
    ################################################    
    reCOPY_INTO = re.compile('^Executing: COPY INTO [@]{1}[a-zA-Z0-9_\.]+[/]+')
    reROW_COUNTS = re.compile('rows_unloaded,input_bytes,output_bytes')
    
	######################################################
    # Search log file for extract names and record counts
	######################################################  
    COPY_INTO_FILENAMES = []
    ROW_COUNTS = []
    
    with open(P_LOGNAME,"r") as fLogFile:
        for sLine in fLogFile:

            sLine = sLine.strip()
            
            if sLine == "":
                continue
                
            rootLogger.debug(f"{sLine=}") 
            
            ################################################            
            # Ex. "Executing: COPY INTO @BIA_DEV.CMS_STAGE_XTR_DEV.BIA_DEV_XTR_STS_MED_INS_MN_STG/STS_MED_INS_RPT_BB2A_MN_2024_DEC_20250926.151032.csv.gz" 
            #      --> STS_MED_INS_RPT_BB2A_MN_2024_DEC_20250926.151032.csv.gz
            ################################################
            if reCOPY_INTO.search(sLine):
                rootLogger.info(f"\n{sLine}")
                extFilename = (sLine.split("/")[1]).strip()
                COPY_INTO_FILENAMES.append(extFilename)

            ################################################
            # Ex. rows_unloaded,input_bytes,output_bytes
            #     2605154,601790572,107807527
            #
            # extract record-count,unzipped-bytes,zipped-bytes. Convert commas to spaces                
            ################################################
            if reROW_COUNTS.search(sLine):
                try:
                    #rootLogger.info(f"{sLine}")
                    # get the following record --> awk {getline;print $0}
                    sNextLine = next(fLogFile).strip()
                    # remove commas
                    sNextLine = sNextLine.replace(","," ")
                    rootLogger.info(f"{sNextLine=}")
                    ROW_COUNTS.append(sNextLine)
                except StopIteration:
                    # EOF
                    sNextLine = None

    ################################################
	# No Extract Filenames were found	
    ################################################
    if len(COPY_INTO_FILENAMES) == 0:
        rootLogger.info("")
        rootLogger.info("COPY_INTO_FILENAMES is empty/blank. ")
        rootLogger.info("Exiting function getExtractFilenamesAndCounts")
        
        # return empty list
        return []


    ################################################
	# No Extract Filenames were found	
    ################################################	
    rootLogger.info("")
    rootLogger.info(f"{COPY_INTO_FILENAMES=} ")
    rootLogger.info(f"{ROW_COUNTS=}")

	# Ex. filenamesAndCounts = "TRICARE_EXTRACT_20241016.103059.txt.gz 2605154 601790572 107807527" 
    lstFilenamesAndCounts = [ f"{ext_filename} {ext_row_counts}" for ext_filename, ext_row_counts in zip(COPY_INTO_FILENAMES, ROW_COUNTS)]
    
    #filenamesAndCounts = "\n".join(lstFilenamesAndCounts).lstrip("\n")

    rootLogger.info("")
    rootLogger.info(f"{lstFilenamesAndCounts=}")

    return lstFilenamesAndCounts
 

def getListofFiles2Process(prmStart_ts, prmEnd_ts):

    #################################################################################
    # Get list of Log Files that are between START_DT and END_DT
    #
    # NOTE: Ignore logs for utility scripts, load finder file scripts, python database logs, and support processing logs.
    #       Also, ignore certin application child logs. 
    #
    # NOTE-2:!!!!PSPS_Split_files  - No record counts - The following file(s) were created:
    #  Should I ignore or use old logic - for awk scripts that split files.
    #################################################################################

    ##############################################
    # Create list of log files within Date range
    ##############################################
    lstLogFiles4DtRange = []
    
    # iterate thru log files in LOG directory
    for sDirPath, _, files in os.walk(LOGDIR):
        for file in files:
            sPathNFilename = os.path.join(sDirPath, file)
            try:
                mtime = os.path.getmtime(sPathNFilename)
                if prmStart_ts <= mtime <= prmEnd_ts:
                    lstLogFiles4DtRange.append(file)  

            except FileNotFoundError:
                # Handle race condition if file was deleted mid-scan
                pass
    
    ##############################################
    # If not log files found return empty list
    ##############################################    
    if len(lstLogFiles4DtRange) == 0:
        return lstLogFiles4DtRange

    ##############################################
    # Convert list to string with line-breaks
    #  for easy log display
    ##############################################
    sLogFiles4DtRange =  "\n".join(lstLogFiles4DtRange)

    rootLogger.info("")
    rootLogger.info(f"List of Log Files found for date range:\n-----------------------------------------\n{sLogFiles4DtRange}")

    ##############################################
    # Remove non-Dashboard log files
    ##############################################
    lstLogFiles2Process = []

    for sFilename in lstLogFiles4DtRange:
        # Do not process TESTING_ logs
        if sFilename.find("TESTING_") >= 0:
            continue

        # Ignore logs for utility scripts, load finder file scripts, python database logs
        if sFilename.find("CombineS3Files") >= 0:
            continue
        if sFilename.find("CreateManifestFile") >= 0:
            continue
        if sFilename.find("ProcessFiles2EFT") >= 0:
            continue
        if sFilename.find("_SF.") >= 0:
            continue
        if sFilename.find("LOAD_") >= 0:
            continue  

        # Remove support scripts
        if  sFilename.find("DashboardInfo") >= 0:
            continue
        if  sFilename.find("DashboardVolReports") >= 0:
            continue            
        if  sFilename.find("BuildRunExtCalendar") >= 0:
            continue
        if  sFilename.find("KIA") >= 0:
            continue
        if  sFilename.find("CleanUp") >= 0:
            continue
        if  sFilename.find("ListXTRProcess") >= 0:
            continue
        if  sFilename.find("GitHub") >= 0:
            continue
        if  sFilename.find("EFT_Files_") >= 0:
            continue

        # Remove reporting log files
        if  sFilename.find("Manifest") >= 0:
            continue
        if  sFilename.find("FinderFiles") >= 0:
            continue
        if  sFilename.find("CalendarExtReports") >= 0:
            continue
        if sFilename.find("DSH_Extract_ArchiveFiles") >= 0:
            continue

        # Remove SF Load table logs
        if  sFilename.find("_SF_Table_Load") >= 0:
            continue

        # Remove child logs from list of log files to process
        if  sFilename.find("DemoFinderFilePrep") >= 0:
            continue
        if  sFilename.find("DEMOFNDR_PT") >= 0:
            continue
        
        # Remove specific Driver logs from list of log files to process. 
        # Ones to keep are VAPTD_Driver, VARTN_Driver, OPMHI_Driver
        if  sFilename.find("NYSPAP_Extract_Driver") >= 0:
            continue
        if  sFilename.find("PTD_DUAL_Daily_Driver") >= 0:
            continue
        if sFilename.find("PTD_DUAL_Monthly_Driver") >= 0:
            continue
        if sFilename.find("SAF_ENC_(INP|SNF)_Driver") >= 0:
            continue
        if re.match("SAF_ENC_(INP|SNF)_Driver",sFilename):
            continue

        # We want to process this log file
        lstLogFiles2Process.append(sFilename)  


    ###################################################
    # Convert list to string with line-breaks
    #  for easy log display
    ###################################################
    sLogFiles2Process =  "\n".join(lstLogFiles2Process)

    rootLogger.info(f"\nList of Log Files to process:\n-------------------------------------------\n{sLogFiles2Process}")
    
    return lstLogFiles2Process


def getExtNameFromLogFilename(prmLogfilename):
    
    ##########################################################
	# Parse log filename for Extract name and Run Timestamp
    # Ex. blbtn_clm_ext_20231020.134153.log
    ##########################################################
    lstLogfilenameNodes = prmLogfilename.split("_")
    iNOFNodes = len(lstLogfilenameNodes)

    # Remove timestamp from logFilename. blbtn_clm_ext_20231020.134153.log  --> blbtn_clm_ext
    sExt_Name = "_".join(lstLogfilenameNodes[0:(iNOFNodes - 1)]).lstrip("_")
    rootLogger.info(f"Original extract name from log file: {sExt_Name}")

	# Clean ext_name --> remove verbiage "Driver" and "Extract"
    sExt_Name = sExt_Name.replace("_Driver","")
    sExt_Name = sExt_Name.replace("_Extracts","")
    sExt_Name = sExt_Name.replace("_EXTRACTS","")
    sExt_Name = sExt_Name.replace("_Extract","")
    sExt_Name = sExt_Name.replace("_EXTRACT","")  
    sExt_Name = sExt_Name.replace("_extract","") 
    rootLogger.info(f"Cleansed Ext name: {sExt_Name}")
    
    return sExt_Name


def getRunTimestampFromLogFilename(prmLogfilename):

    # Capture timestamp. blbtn_clm_ext_20231020.134153.log  --> 20231020.134153
    lstLogfilenameNodes = prmLogfilename.split("_")
    iNOFNodes = len(lstLogfilenameNodes)

    sRunTmpstmp = str(lstLogfilenameNodes[(iNOFNodes - 1)]).replace(".log","")
    rootLogger.info(f"{sRunTmpstmp=}")
    
    return sRunTmpstmp
    

def createJobInfoKeyValuePairs(prmLogfilename, prmJobSuccess): 

    rootLogger.info("")

    rootLogger.info(f"{prmLogfilename=}")
    rootLogger.info(f"{prmJobSuccess=}") 
	
    ############################################################	
    # Create Key/value pairs for Job Info
    ############################################################
    #Ex. blbtn_clm_ext_20231020.134153.log, Fri Oct 20 13:41:53 EDT 2023,Fri Oct 20 13:42:05 EDT 2023
    #Ex. OFM_PDE_Extract_20231018.163447.log, Wed Oct 18 16:34:47 EDT 2023,Wed Oct 18 16:56:11 EDT 2023	

    rootLogger.info("")
    rootLogger.info("Parse for Key Values")

    ##########################################################
	# Parse log filename for Extract name and Run Timestamp
    # Ex. blbtn_clm_ext_20231020.134153.log
    ##########################################################
    sExtName = getExtNameFromLogFilename(prmLogfilename)
    sRunTmpstmp = getRunTimestampFromLogFilename(prmLogfilename)

    # Extract the RunDate from runTimestamp
    sRunDate = sRunTmpstmp.split(".",1)[0]
    rootLogger.info(f"{sRunDate=}")

	##########################################
	# Build JobInfo load record in json format
	##########################################	
    sDashboardJobInfoRec = fr'{{"log": "{prmLogfilename}", "ext": "{sExtName}", "runTmstmp": "{sRunTmpstmp}", "success": "{prmJobSuccess}" }} '

    return sDashboardJobInfoRec
    



def getExtractFilenamesAndCountsDashboardInfo(prmLogfileNPath, prmExt): 

    rootLogger.info("")

    rootLogger.info(f"{prmLogfileNPath=}")
    rootLogger.info(f"{prmExt=}")  	

    #####################################################################################
    # eye-catcher:filename, record-count, unzipped-bytes, zipped-bytes | convert commas to spaces
    #
    # Ex. DASHBOARD_INFO:blbtn_drug_ext_20250106.131600.txt.gz 584307,234307107,18067235
    # --> blbtn_drug_ext_20250106.131600.txt.gz 584307,234307107,18067235   
    # Ex. DASHBOARD_INFO:DEMOFNDR_PTA_H0137_202305_20241022.151515.txt.gz 376,239136,8335 
    # --> DEMOFNDR_PTA_H0137_202305_20241022.151515.txt.gz 376 239136 8335
    #####################################################################################
    lstDASHBOARD_INFO = egrep(prmLogfileNPath, "DASHBOARD_INFO:", ignore_case=False)

    # if DASHBOARD_INFO: does not exist in log file--> use alternate search for ROW_COUNTS for older log files.	
    if len(lstDASHBOARD_INFO) == 0:
        rootLogger.info("")
        rootLogger.info("DASHBOARD_INFO was not found. Use older method for record counts")
        
        # Try alternate search method to get filenames and record counts
        lstFilenamesNCounts = ExtractFilenamesAndCountsLegacy(prmLogfileNPath, prmExt)
        
        return lstFilenamesNCounts        

    #############################################################
    # DASHBOARD_INFO exists
    #############################################################
    rootLogger.info("")
    rootLogger.info(f"{lstDASHBOARD_INFO=}")

    #############################################################
    # Remove "DASHBOARD_INFO" eye-catcher from each item in list
    #  of extract filenames and counts.
    #############################################################
    lstFilenamesNCounts = []    
   
    for sDASHBOARD_INFO in lstDASHBOARD_INFO:
        sFilenamesNCounts = (sDASHBOARD_INFO.split(":")[1]).strip().replace(","," ")
        #rootLogger.debug(f"{sFilenamesNCounts=}")
        lstFilenamesNCounts.append(sFilenamesNCounts)        


    rootLogger.info("")
    rootLogger.info(f"{lstFilenamesNCounts=}")

    return lstFilenamesNCounts


def calcByteCount(prmExtFilename, iprmRecCount, prmExt): 

    # iprmRecCount is expected to an integer and not a string
    
    rootLogger.info("")

    rootLogger.info(f"{prmExtFilename=}")
    rootLogger.info(f"{iprmRecCount=}")
    rootLogger.info(f"{prmExt=}")
    
    if type(iprmRecCount) is not int:
        raise TypeError("iprmRecCount is expected to be an integer.")

	# Get LRECLs for exception logs
    if prmExt == "DEMO":
        if prmExtFilename.find('_PTA_') >= 0:
            LRECL = 635
        elif prmExtFilename.find('_PTB_') >= 0:
            LRECL = 625
        elif prmExtFilename.find('_PTD_') >= 0:
            LRECL = 253
        else:
            LRECL = 0
    elif prmExt == "PTD_DUALS":
            LRECL = 185
    elif prmExt == "PSPS_NPI":
            LRECL = 126
    elif prmExt == "PSPS_SPLIT":
            LRECL = 129			


    rootLogger.info(f"{LRECL=}")
	
    # Calculate total NOF bytes
    sByteSize = str(iprmRecCount * LRECL)

    rootLogger.info(f"{sByteSize=}")

    rootLogger.info("")

    return sByteSize


def sedFilenamesNCounts(prmLogfileNPath, sStartStr, sEndStr):

    #############################################################################################################################
    # NOTE: sStartStr is inclusive. sEndStr is not inclusive
    #############################################################################################################################
    # Ex. this is repeated for PTA, PTB, and PTD. PTA and PTB terminated by "function logname:" and PTD terminated by blank line
    #	
    #filenamesAndCounts: DEMOFNDR_PTA_H0137_202308_20231116.111346.txt.gz        146,697
    #DEMOFNDR_PTA_H0192_202308_20231116.111346.txt.gz          6,184
    #DEMOFNDR_PTA_H0480_202308_20231116.111346.txt.gz          2,090
    #DEMOFNDR_PTA_H8786_202308_20231116.111346.txt.gz         45,647
    #DEMOFNDR_PTA_H9239_202308_20231116.111346.txt.gz        127,435
    #DEMOFNDR_PTA_H9712_202308_20231116.111346.txt.gz          2,951
    #DEMOFNDR_PTA_H9869_202308_20231116.111346.txt.gz             21
    #function logname: /app/IDRC/XTR/CMS/logs/DEMOFNDR_PTB_Extract_20231116.111346.log
    ###############################################################################################

    ###############################################################################################
    # Example: 
    #REC_CNTS=PTDDUALS_MONTHLY_AZ_Y2023M09_20231101.090818.txt      1,137,893
    #PTDDUALS_MONTHLY_CA_Y2023M09_20231101.090818.txt     41,779,337
    #PTDDUALS_MONTHLY_CT_Y2023M09_20231101.090818.txt        690,709
    #PTDDUALS_MONTHLY_SC_Y2023M09_20231101.090818.txt      1,320,118
    #PTDDUALS_MONTHLY_VA_Y2023M09_20231101.090818.txt      1,348,188
    #PTDDUALS_MONTHLY_WA_Y2023M09_20231101.090818.txt      1,523,892
    #PTDDUALS_MONTHLY_WI_Y2023M09_20231101.090818.txt      1,447,899
    #total     83,350,936 
    #
    # PTD DUALS Historical
    # REC_CNTS=PTDDUALS_HIST_TX_Y2025M06_20250915.095644.txt      2,022,932
    #    send: 'ehlo ip-10-152-101-118.ec2.internal\r\n'
    #
    ###############################################################################################
    with open(prmLogfileNPath, "r") as fLogFile:
        bCapture = False
        lstExtFilesNCounts = []

        for sRec in fLogFile:
            sRec = sRec.strip()

            if sRec.startswith(sStartStr):
                bCapture = True  # start capturing

            if sRec.startswith(sEndStr) or sRec == "":
                bCapture = False  # stop capturing

            if bCapture:
                # Remove eye-catcher string if on line with filename and counts
                sRec = sRec.replace(sStartStr,"").strip()
                lstExtFilesNCounts.append(sRec)

        return lstExtFilesNCounts

    
def ExtractFilenamesAndCountsLegacy(prmLogfileNPath, prmExt):

    global TOT_WARNINGS
    
    rootLogger.info("")

    rootLogger.info(f"{prmLogfileNPath=}")
    rootLogger.info(f"{prmExt=}")

    sLogFilename = os.path.basename(prmLogfileNPath)
	
	###########################################	
	# Get Extract filenames and record counts
	###########################################
    # Ex. DEMO -->  this is repeated for PTA, PTB, and PTD. PTA and PTB terminated by "function logname:" and PTD terminated by blank line
    #	
    #filenamesAndCounts: DEMOFNDR_PTA_H0137_202308_20231116.111346.txt.gz        146,697
    #DEMOFNDR_PTA_H0192_202308_20231116.111346.txt.gz          6,184
    #DEMOFNDR_PTA_H0480_202308_20231116.111346.txt.gz          2,090
    #DEMOFNDR_PTA_H8786_202308_20231116.111346.txt.gz         45,647
    #DEMOFNDR_PTA_H9239_202308_20231116.111346.txt.gz        127,435
    #DEMOFNDR_PTA_H9712_202308_20231116.111346.txt.gz          2,951
    #DEMOFNDR_PTA_H9869_202308_20231116.111346.txt.gz             21
    #function logname: /app/IDRC/XTR/CMS/logs/DEMOFNDR_PTB_Extract_20231116.111346.log
    #################################################################################
    rootLogger.info("")
    rootLogger.info(f"Get extract filenames and record counts for {prmExt}.")

    if prmExt == "DEMO":
        lstExtFilesNRecCountNoBytes = sedFilenamesNCounts(prmLogfileNPath, "filenamesAndCounts:", "function logname:")

    elif prmExt == "PTD_DUALS":
        if sLogFilename.find("Historical") == -1:
            lstExtFilesNRecCountNoBytes = sedFilenamesNCounts(prmLogfileNPath, "REC_CNTS=", "total")
        else:
            # when its Historical
            lstExtFilesNRecCountNoBytes = sedFilenamesNCounts(prmLogfileNPath, "REC_CNTS=", "send:")

    elif prmExt == "PSPS_NPI":
        lstExtFilesNRecCountNoBytes = sedFilenamesNCounts(prmLogfileNPath, "filenamesAndCounts:", "function logname:")

    elif prmExt == "PSPS_SPLIT":
        lstExtFilesNRecCountNoBytes = sedFilenamesNCounts(prmLogfileNPath, "filenamesAndCounts:", "total")

    ####################################################
    # No extract filenames or record counts were found
    ####################################################
    if len(lstExtFilesNRecCountNoBytes) == 0:
        rootLogger.info(f"WARNING: {sLogFilename} does not contain any extract filenames or record counts. Script may need to be modified.")
        rootLogger.info("Exiting function ExtractFilenamesAndCountsLegacy")

        TOT_WARNINGS += 1

        # This is an empty list
        return lstExtFilesNRecCountNoBytes


    ####################################################
    # Add the byte size to each list element to create 
    #  new list
    ####################################################
    lstExtFilesNRecCounts = []
    
    for sExtFileNRecCountNoBytes in lstExtFilesNRecCountNoBytes:
        
        # Need to treat consecutive spaces as single delimiter
        lstFlds = sExtFileNRecCountNoBytes.split()
        sExtFilename = lstFlds[0]
        sRecCount = str(lstFlds[1]).replace(",","")
			
        rootLogger.info(f"{sExtFilename=}")
        rootLogger.info(f"{sRecCount=}")			

        # Calculate byte size
        sByteCount = calcByteCount(sExtFilename, int(sRecCount), prmExt)
			
        # Build new list item   
        sNewListItem = f"{sExtFilename} {sRecCount} {sByteCount}"    
        lstExtFilesNRecCounts.append(sNewListItem)


    rootLogger.info("")
    
    return lstExtFilesNRecCounts
		

def main_processing_loop():
    
    try:    
    
        # Keep track of warnings
        global TOT_WARNINGS
        TOT_WARNINGS = 0
        
        global reEndedAt
        reEndedAt = None
        
        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        print(f"{TMSTMP=}")

        global LOGNAME
        LOGNAME = f"{LOGDIR}DashboardInfo_MS_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDashboardInfo_MS_Driver.py started at {TMSTMP}")

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
        rootLogger.info(f"Get Parameters")

        ##################################################################
        # Extract can run in default-mode, or using override parms
        ##################################################################
        iNOFParms = len(sys.argv) - 1
        if not (iNOFParms == 1 or iNOFParms == 2):
            rootLogger.info(f"Incorrect # of parameters sent to script. NOF parameters: {iNOFParms}")    
            sys.exit(12)


        ##################################################################
        # Set date range of log files to process
        #   (Y)esterday: process yesterday's log files
        #   (T)oday: process today's log files
        #   (O)verride date range: supply user-supplied date range 
        # 
        # Do not need NOT_INCLUSIVE for python logic
        # RUN_THRU_DT_NOT_INCLUSIVE = dttmRunToDt
        ##################################################################
        lstParms = sys.argv
            
        if iNOFParms == 1:
            rootLogger.info(f"{lstParms[1]=}")
            
            sMode = lstParms[1]
            rootLogger.info(f"{sMode=}")
            
            if sMode == "Y":
                rootLogger.info(" ")
                rootLogger.info("Using script calculated dates for yesterday. ")
                
                # get yesterday's date
                dttmRunFromDt = (date.today() + timedelta(days=-1))
                dttmRunToDt   = (date.today() + timedelta(days=-1))
                
            elif sMode == "T":
                rootLogger.info(" ")
                rootLogger.info("Using script calculated dates for today. ")
                
                # get yesterday's date
                dttmRunFromDt = (date.today())
                dttmRunToDt   = (date.today())
            else:
                # Invalid Mode
                rootLogger.info("")
                rootLogger.info(f"DashboardInfo_MS_Driver.py failed. Invalid parameter Mode: {sMode}")

                SUBJECT=f"DashboardInfo_MS ({ENVNAME})" 
                MSG=f"DashboardInfo_MS_Driver.py failed. Invalid parameter Mode: {sMode}."
               
                try:
                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                    write_sp_info_2_log(sp_info)

                    sys.exit(12) 
                    
                except subprocess.CalledProcessError as e:
                    rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                    rootLogger.error(e.output)
                    sys.exit(12) 
                    
                
        elif iNOFParms == 2:
            rootLogger.info(" ")
            rootLogger.info("Using override dates ")
            rootLogger.info(f"{lstParms[1]}=")
            rootLogger.info(f"{lstParms[2]}=")

            # Convert string to datetime object
            dttmRunFromDt = datetime.strptime(lstParms[1],'%Y%m%d').date()
            dttmRunToDt   = datetime.strptime(lstParms[2],'%Y%m%d').date()
            

        ##############################################
        # Establish Date range of files to process
        ##############################################
        start_date = datetime.combine(dttmRunFromDt, time(0, 0, 0))   
        end_date   = datetime.combine(dttmRunToDt, time(23, 59, 59)) 
    
        rootLogger.info(f"start_date={start_date.strftime('%Y%m%d.%H%M%S')}")
        rootLogger.info(f"end_date={end_date.strftime('%Y%m%d.%H%M%S')}")
        
        # --- Convert to timestamps ---
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
                

        ##################################################################
        # Ensure that DASHBOARD BUCKET and SF load files exist
        ##################################################################
        rootLogger.info(" ")
        rootLogger.info(f"{DASHBOARD_BUCKET_FLDR=}")

        DASHBOARD_JOBINFO_FILE = f"DASHBOARD_JOB_INFO_{TMSTMP}.json"
        DASHBOARD_JOBDTLS_FILE = f"DASHBOARD_JOB_DTLS_EXTRACT_FILES_{TMSTMP}.json"

        rootLogger.info(f"{DASHBOARD_JOBINFO_FILE=}")
        rootLogger.info(f"{DASHBOARD_JOBDTLS_FILE=}")

        # simulate linux bash touch command   
        #Path(os.path.join(DATADIR, DASHBOARD_JOBINFO_FILE).touch(exist_ok=True)
        #Path(os.path.join(DATADIR, DASHBOARD_JOBDTLS_FILE).touch(exist_ok=True)

            
        #################################################################################
        # Get log files to process
        #################################################################################
        lstLogFiles2Process = getListofFiles2Process(start_ts, end_ts)

        #################################################################################
        # Are there log files available to process?
        #################################################################################
        if len(lstLogFiles2Process) == 0:
            rootLogger.info("")
            rootLogger.info(f"No log files to process for load of Dashboard tables for period {dttmRunFromDt} to {dttmRunToDt}.")

            SUBJECT=f"DashboardInfo_MS ({ENVNAME})" 
            MSG=f"There are no extract log files to process from {dttmRunFromDt} thru {dttmRunToDt}.\n\nThere are {TOT_WARNINGS} warnings in script log."
           
            try:
                sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
                write_sp_info_2_log(sp_info)

                sys.exit(0) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error(e.output)

                sys.exit(12)                


        #################################################################################
        # Process log files
        #################################################################################        
        # Create Bytes buffer to store file contents
        ioJobInfoFile = io.BytesIO()
        ioJobDtlsFile = io.BytesIO()

        rootLogger.info("")        
        rootLogger.info("Start processing log files")
                
        for sLogFile2Process in lstLogFiles2Process:

            rootLogger.info("")
            rootLogger.info("********************************************")

            rootLogger.info(f"{sLogFile2Process=}")            

            sLogfileNPath =  os.path.join(LOGDIR, sLogFile2Process)        
            rootLogger.info(f"{sLogfileNPath=}")               
            
            ################################
            ## Get Start Time for script
            ################################
            # Ex. "started at Mon Aug  7 15:22:12 EDT 2023"
            lstHead = head(sLogfileNPath, iFirstNOFRecs = 3) 
            
            lstJobStartLine = [line for line in lstHead if "started at" in line ]
            rootLogger.info(f"{lstJobStartLine=} ")

            # Extract the start date from the log message
            if len(lstJobStartLine) == 1:
                sJobStartTime = str(lstJobStartLine[0]).split("started at")[-1].strip()
            else: 
                sJobStartTime = ""
            
            rootLogger.info(f"{sJobStartTime=}")
    
            ################################	
            # Get End Time for script
            ################################	
            # Example: "Ended at Mon Aug  7 15:23:01 EDT 2023" or "VAPTD_Driver.sh ended at: 20240902.170605" or "Script PSPS_Split_files.sh completed successfully"
            lstTail = tail(sLogfileNPath, iLastNOFRecs = 5) 
            
            # compile regex once    
            if reEndedAt is None:
                reEndedAt = re.compile("(E|e)nded at")
            
            lstJobEndLine = [line for line in lstTail if reEndedAt.search(line) ]
            rootLogger.info(f"{lstJobEndLine=} ")

            # Extract the end date from the log message
            if len(lstJobEndLine) == 1:
                sJobEndTime = str(lstJobEndLine[0]).split("(nded at")[-1].strip()
            else: 
                sJobEndTime = ""

            rootLogger.info(f"{sJobEndTime=}")


            ######################################################	
            # Create Job Info key/value pairs
            # NOTE: Job variables set in function are used 
            #       to create File Extract Key/value pair records
            ######################################################	
            if sJobEndTime == "": 
                rootLogger.info(f"INFO: Could not find Job End Line. Job associated with {sLogFile2Process} did not complete. ")

                sJobInfoRec = createJobInfoKeyValuePairs(sLogFile2Process, prmJobSuccess = "N")
            else:
                sJobInfoRec = createJobInfoKeyValuePairs(sLogFile2Process, prmJobSuccess = "Y")

            # write JobInfo load file record
            rootLogger.info("Write JobInfo record")
            rootLogger.info(f"{sJobInfoRec}\n")
            ioJobInfoFile.write( (f"{sJobInfoRec}\n").encode("utf-8") )


            ###################################################################################################################
            #
            #  Get Extract filenames and record counts: (JobDtls information)
            #  ==============================================================
            #
            # 1) For all extracts that extract files using a SF SELECT statment (except DEMO Finder), we replicate the FilenameCounts.bash 
            #    logic to extract and combine the SF Filenames, record counts, and byte counts which were always displayed in the logs.
            #
            # 2) For Demo Finder going forward: modified the FilenameCounts.bash to display filenames, record counts, and byte counts with label 
            #    "DASHBOARD_INFO:" for each extract file. The logic from #1 would not work since that information was in separate DEMO PTA, PTB, and PTD log files. 
            #    And, the Dashboard script is only parsing the main Demo Finder log file for simplicity. (Demo is one extract not 3 separate extracts).
            #
            #    For older Demo Finder log files, find label "filenamesAndCounts:", extract filenames and record counts, then for each file, calculate byte count
            #    by multiplying record count * hard-coded LRECL. Add byte count to end of each "filename record count" to add to table.	
            #
            # 3) For extracts that use awk to split the extract file, we will find label "filenamesAndCounts:", extract filenames and record counts, then for each file
            #    calculate byte count by multiplying record count * hard-coded LRECL. Add byte count to end of each "filename record count" to add to table.
            #    While we can use the logic for #1 to get the all-in-one-extract counts, we would miss out on the split files created by the awk script.	
            #    Includes PSPS_NPI_Extract.sh, PSPS_Split_files.bash 
            #
            # PTD Duals Monthly  - 1) Normal one file extract where we get counts; 2) split extract file into smaller files - will ignore this. (Too complex)
            #
            ###################################################################################################################
            rootLogger.info("")
            rootLogger.info("Get Extract filenames and record counts from log file")

            if sLogFile2Process.find("DemoFinderFileExtracts_") >= 0:
                lstFilenamesNCounts = getExtractFilenamesAndCountsDashboardInfo(sLogfileNPath, prmExt = "DEMO")

            elif sLogFile2Process.find("PSPS_NPI_Extract_") >= 0:	
                lstFilenamesNCounts = getExtractFilenamesAndCountsDashboardInfo(sLogfileNPath, prmExt = "PSPS_NPI")

            elif sLogFile2Process.find("PSPS_Split_files_") >= 0:	
                lstFilenamesNCounts = getExtractFilenamesAndCountsDashboardInfo(sLogfileNPath, prmExt = "PSPS_SPLIT")
    
            elif sLogFile2Process.find("PTD_Duals_Extract_") >= 0:	
                lstFilenamesNCounts = getExtractFilenamesAndCountsDashboardInfo(sLogfileNPath, prmExt = "PTD_DUALS")

            else:
                lstFilenamesNCounts = getExtractFilenamesAndCounts(sLogfileNPath)

            rootLogger.info(f"{len(lstFilenamesNCounts)=}")
            
            # Were extract filenames found?	
            if len(lstFilenamesNCounts) == 0:
                rootLogger.info(f"WARNING: COPY_INTO_FILENAMES is blank. Cannot get extract filenames. Script associated with {sLogFile2Process} may need to be modified.")

                TOT_WARNINGS += 1
                
                # process next log file 2 process
                continue	


            #########################################################	
            # Loop thru start Positions to get sets of extract files
            #########################################################
            rootLogger.info("")
            rootLogger.info("Loop thru Extract File information found")
                
            for sFilenameNCount in lstFilenamesNCounts:

                ############################################################	
                # Extract Filename, rec count, and byte count from sFilenameNCount
                # Ex. Extract_file_20240102.151515.txt.gz 376 239136 8335
                ############################################################	
                rootLogger.info("") 
                rootLogger.info(f"{sFilenameNCount=}") 

                lstFilenameNCount = sFilenameNCount.split(" ")

                # RecCount is missing. Cannot be empty str/Null
                if len(lstFilenameNCount) == 1: 
                    rootLogger.info("RecCount is empty str. Assign RecCount and g_ByteSize 0 default value.")
                    sExtractFile = lstFilenameNCount[0]
                    sRecCount=0
                    sByteSize=0
                    # DASHBOARD_INFO --> filename recCount ByteCount     
                elif len(lstFilenameNCount) == 3: 
                    sExtractFile = lstFilenameNCount[0]
                    sRecCount = lstFilenameNCount[1]
                    sByteSize = lstFilenameNCount[2]                    
                    # From current logs: filename recCount ByteCount zippedByteCount  
                elif len(lstFilenameNCount) == 4: 
                    sExtractFile = lstFilenameNCount[0]
                    sRecCount = lstFilenameNCount[1]
                    sByteSize = lstFilenameNCount[2]
                    #sByteZipSize = lstFilenameNCount[3]
                else:    
                    ## Send Failure email	
                    SUBJECT=f"DashboardInfo_MS_Driver.py - Failed ({ENVNAME})"
                    MSG=f"lstFilenameNCount has wrong number of fields. Should have a filename and three record counts. DashboardInfo_MS_Driver.py failed."
                    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
                    write_sp_info_2_log(sp_info)

                    sys.exit(12)

            
                # Display the extracted values
                rootLogger.info(f"{sExtractFile=}")
                rootLogger.info(f"{sRecCount=}")
                rootLogger.info(f"{sByteSize=}")
                #rootLogger.info(f"{sByteSize=}")
                    
                ############################################################	
                # Convert bytes to Human Readable value
                ############################################################	
                sHumanFileSize =  convertBytes2ReadableSize(int(sByteSize))

                ############################################################	
                # Create Key/value pairs for ExtractFiles and record counts
                ############################################################
                sExtName = getExtNameFromLogFilename(sLogFile2Process)
                sRunTmpstmp = getRunTimestampFromLogFilename(sLogFile2Process)
                
                sJobDtlsRec = fr'{{"log": "{sLogFile2Process}", "ext": "{sExtName}", "runTmstmp": "{sRunTmpstmp}", "ExtractFile": "{sExtractFile}", "RecCount": "{sRecCount}" , "FileByteSize": "{sByteSize}", "HumanFileSize": "{sHumanFileSize}"  }} '   

                # write JobDtls load file record
                rootLogger.info("Write JobDtls record")
                rootLogger.info(f"{sJobDtlsRec}\n")
                ioJobDtlsFile.write(f"{sJobDtlsRec}\n".encode("utf-8"))


        #############################################################
        # Get S3 reference
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Get s3 Client object")
        
        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")

        
        #############################################################
        # Put JobInfo file into S3
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Put {DASHBOARD_JOBINFO_FILE} file contents to S3 bucket {DASHBOARD_BUCKET_FLDR}")

        # set to beginning of Bytes stream
        ioJobInfoFile.seek(0)
        
        # Set bucket-folder/filename
        destKey = DASHBOARD_BUCKET_FLDR + DASHBOARD_JOBINFO_FILE

        rootLogger.info(f"Put file {destKey} into S3")
        resp = s3_client.put_object(Bucket=XTR_BUCKET, Key=destKey, Body=ioJobInfoFile, ContentType="application/json")

        rootLogger.debug(f"{resp=}")
        
        if resp == None:
            ## Send Failure email	
            SUBJECT=f"DashboardInfo_MS_Driver.py - Failed ({ENVNAME})"
            MSG=f"Put Dashboard JobInfo file {destKey} into s3 failed."
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)

            sys.exit(12)

        
        #############################################################
        # Put JobInfo file into S3
        #############################################################
        rootLogger.info("")
        rootLogger.info(f"Put {DASHBOARD_JOBDTLS_FILE} file contents to S3 bucket {DASHBOARD_BUCKET_FLDR}")
        
        # set to beginning of Bytes stream
        ioJobDtlsFile.seek(0)
        
        # Set bucket-folder/filename
        destKey = DASHBOARD_BUCKET_FLDR + DASHBOARD_JOBDTLS_FILE

        rootLogger.info(f"Put file {destKey} into S3")
        
        resp = s3_client.put_object(Bucket=XTR_BUCKET, Key=destKey, Body=ioJobDtlsFile.getvalue(), ContentType="application/json")

        rootLogger.debug(f"{resp=}")
        
        if resp == None:
            ## Send Failure email	
            SUBJECT=f"DashboardInfo_MS_Driver.py - Failed ({ENVNAME})"
            MSG=f"Put Dashboard JobInfo file {destKey} into s3 failed."
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)

            sys.exit(12)      

        
        #############################################################
        # Make variables available for substitution in Python code
        #############################################################
        # Convert dttm variables to string YYYYMMDD format
        os.environ["RUN_FROM_DT"] = datetime.strftime(dttmRunFromDt,'%Y%m%d')
        os.environ["RUN_TO_DT"] = datetime.strftime(dttmRunToDt,'%Y%m%d')
        os.environ["DASHBOARD_JOBINFO_FILE"] = DASHBOARD_JOBINFO_FILE
        os.environ["DASHBOARD_JOBDTLS_FILE"] = DASHBOARD_JOBDTLS_FILE

        #################################################################################
        # Execute Python code to load Extract data to SF
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Start execution of DashboardInfo_MS.py program")

        try:
            sp_info = subprocess.run(['python3', 'DashboardInfo_MS.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling DashboardInfo_MS.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"Python program DashboardInfo_MS.py - Failed ({ENVNAME})"
            MSG=f"Python program DashboardInfo_MS.py failed."

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info) 

            sys.exit(12)    

        rootLogger.info("")
        rootLogger.info("Python script DashboardInfo_MS.py completed successfully.")
        

        ############################################################
        # Move Dashboard JOBINFO json file to S3 archive folder.
        ############################################################
        rootLogger.info("")
        rootLogger.info(f"Move S3 {DASHBOARD_JOBINFO_FILE} file to S3 {DASHBOARD_BUCKET_FLDR}archive folder")

        sSourceKey = DASHBOARD_BUCKET_FLDR + DASHBOARD_JOBINFO_FILE
        sDestinationKey = DASHBOARD_BUCKET_FLDR + "archive/" + DASHBOARD_JOBINFO_FILE

        rootLogger.info(f"{sSourceKey=}")
        rootLogger.info(f"{sDestinationKey=}")

        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, sSourceKey, sDestinationKey)

        ############################################################
        # Move Dashboard JOBINFO json file to S3 archive folder.
        ############################################################
        rootLogger.info("")
        rootLogger.info(f"Move S3 {DASHBOARD_JOBDTLS_FILE} file to S3 {DASHBOARD_BUCKET_FLDR}archive folder")

        sSourceKey = DASHBOARD_BUCKET_FLDR + DASHBOARD_JOBDTLS_FILE
        sDestinationKey = DASHBOARD_BUCKET_FLDR + "archive/" + DASHBOARD_JOBDTLS_FILE

        rootLogger.info(f"{sSourceKey=}")
        rootLogger.info(f"{sDestinationKey=}")

        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, sSourceKey, sDestinationKey)


        ############################################################
        # Success email. 
        ############################################################
        rootLogger.info("")
        rootLogger.info("Send success email for load of Dashboard tables for period {dttmRunFromDt} to {dttmRunToDt}.")

        SUBJECT=f"DashboardInfo_MS ({ENVNAME})" 
        MSG=f"The loading of the Dashboard tables with extract log information from {dttmRunFromDt} to {dttmRunToDt} has completed successfully.\n\nThere are {TOT_WARNINGS} warnings in script log."
       
        try:
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    

        #############################################################
        # script clean-up
        #############################################################
        rootLogger.info("")
        rootLogger.info("Remove temporary text files from data directory") 


        #############################################################
        # end script
        #############################################################
        rootLogger.info("")
        rootLogger.info("DashboardInfoDriver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in DashboardInfo_MS_Driver.py\n {e}")

        rootLogger.error("Exception occured in DashboardInfo_MS_Driver.")
        rootLogger.error("\n%s", e)

        ## Send Failure email	
        SUBJECT=f"DashboardInfo_MS_Driver.py  - Failed ({ENVNAME})"
        MSG=f"Exception occured in DashboardInfo_MS_Driver.py {e}. Process failed. "
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)  
        
        
if __name__ == "__main__":

    main_processing_loop()