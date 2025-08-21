#!/usr/bin/env python
############################################################################################################
# Name:  FilenameCounts.py
#
# NOTE: This module is intended to included in another python module. 
#       The caller will pass an instance of its logger which this function will use to write log messages.
#
# Modified: 
#
# Paul Baranoski 2025-07-15 Create python version of shell script
#############################################################################################################
import re
import os


#############################################################
# Functions
#############################################################
def getExtractFilenamesAndCounts(rootLogger, fLogFilenameNPath): 

    # Read contents of log file and store in string that will be used to search
    with open(fLogFilenameNPath, "r") as logfile:
        strLogFileContents = logfile.read()
    
    rootLogger.info("In function getExtractFilenamesAndCounts()")

    ##################################################
    # COPY_INTO_FILENAMES
    ##################################################
    # Ex. "COPY INTO @BIA_DEV.CMS_STAGE_XTR_DEV.BIA_DEV_XTR_DUALS_MA_STG/DUALS_MedAdv_AH_MD_202407_202409_20250128.085118.txt.gz"
    reCOPY_INTO_FILENAMES = re.compile('^Executing: COPY INTO @BIA_[a-zA-Z0-9_.]+[/]{1}[a-zA-Z0-9_.]+\.gz$', re.MULTILINE)
    lstReResults = reCOPY_INTO_FILENAMES.findall(strLogFileContents)
    
    # We want the 2nd part of the split command which is the extract filename - excludes S3 Stage 
    lstExtractFilenames = [ S3StageNFilename.split("/")[1] for S3StageNFilename in lstReResults]
    COPY_INTO_FILENAMES = (os.linesep).join([extFilename for extFilename in lstExtractFilenames])
    COPY_INTO_FILENAMES += os.linesep
    rootLogger.info(f"COPY_INTO_FILENAMES: {COPY_INTO_FILENAMES}")  

    ##################################################
    # ROW_COUNTS and ROW_INFO
    ##################################################
    # Ex. 'rows_unloaded,input_bytes,output_bytes\n147148,13979060,181562' 
    reRowsUnloaded = re.compile('^rows_unloaded,input_bytes,output_bytes\n[0-9]+,[0-9]+,[0-9]+', re.MULTILINE)
    lstReResults = reRowsUnloaded.findall(strLogFileContents)
    rootLogger.debug(f"{lstReResults=}")

    # Ex. '147148,13979060,181562' 
    lstFileCounts  = [EyeCatcherNCounts.split("\n")[1] for EyeCatcherNCounts in lstReResults]
    lstRecCounts = [fileCounts.split(",")[0]  for fileCounts in lstFileCounts]
    
    rootLogger.info(f"{lstFileCounts=}")
    rootLogger.info(f"{lstRecCounts=}")

    # isolate the first number -> the record count from the three other counts (byte count,zipped byte count)
    ROW_COUNTS = (os.linesep).join([recCount for recCount in lstRecCounts])
    ROW_COUNTS += os.linesep
        
    ROW_INFO = (os.linesep).join([sFileCounts for sFileCounts in lstFileCounts])
    ROW_INFO += os.linesep

    rootLogger.info(f"ROW_COUNTS: {ROW_COUNTS}")
    rootLogger.info(f"ROW_INFO: {ROW_INFO}")
	

    lstFilenamesAndCounts = [f"{filename.ljust(50)} {int(count): >14,d}" for filename, count in zip (lstExtractFilenames, lstRecCounts) ]
    strFilenamesAndCounts = "\n".join(sFilenameAndCount for sFilenameAndCount in lstFilenamesAndCounts)  + "\n"

    #strFilenamesAndCounts = "\n".join(FilenameAndCounts for FilenameAndCounts in lstFilenamesAndCounts )
    rootLogger.info(f"{lstFilenamesAndCounts=}")
    rootLogger.info(strFilenamesAndCounts)

    # print DASHBOARD info 
    # Ex. DASHBOARD_INFO:DUALS_MedAdv_AH_AZ_202407_202409_20250128.085118.txt.gz 26143,18666102,421248 
    # NOTE: the \n before DASHBOARD_INFO is to ensure that the DASHBOARD information line is on a separate line, left-justified without any other logging info preceding it
    lstDashboardInfo = [f"\nDASHBOARD_INFO:{filename} {counts}" for filename, counts in zip (lstExtractFilenames, lstFileCounts) ]
    strDashboardInfo = "\n".join(sDashboardInfo for sDashboardInfo in lstDashboardInfo)  + "\n"
    rootLogger.info("")
    rootLogger.info(strDashboardInfo)
    
    # caller will display the filenames and record counts in email
    return strFilenamesAndCounts
	



