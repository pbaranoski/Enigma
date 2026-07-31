#!/usr/bin/env python
########################################################################################################
# Name:  DashboardVolReportsDriver.py
#
# Description: This script will generate three email reports.
#
# Execute as "python3 DashboardVolReportsDriver.py {RptPeriod} {OverrideRptDate} "
#
#     Note: Parameter_1 Required (RptPeriod): CY, FY, or M
#
#           Parameter_2 Optional (OverrideRptYYYY): Reports will be generated using current Year unless and "Override Rpt Date" is included
#           when program is executed. Date can be YYYY or YYYYMM
#
# CY and "01" Month --> RPT_YYYY = Prior Year YYYY     Ex. CurrentDate=2025-01-08 --> reporting on 2024-01-01 thru 2024-12-31
# FY and "10" Month --> RPT_YYYY = current date YYYY   Ex. CurrentDate=2024-10-07 --> reporting on 2023-10-01 thru 2024-09-30
# M                 --> RPT_MM   = Month report would run; CurrentDate=2025-01-07 --> reporting on 2024-12-01 thru 2024-12-31
#
# Overrider values must follow above rules: 
# Ex: CY --> OverrideDate YYYY = 2025 will report on 2024-01-01 thru 2024-12-31; To report on 2025 data OverrideDate YYYY = 2026
# Ex:  M --> OverrideDate YYYYMM = 202501 will report on 2024-12-01 thru 2024-12-31
#   
# Paul Baranoski   2025-12-01 Create Module.   
# Paul Baranoski   2026-01-02 Set variable RPT_MM to an empty string when using an override for CY or MY reports.                        
########################################################################################################

import logging
import sys
import argparse

import boto3 
import gzip
from io import StringIO

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess

import tempfile
# Set a different temp directory than the default "/tmp"
tempfile.tempdir = "/app/IDRC/XTR/CMS/data/"

# Our common module with variable constants
from SET_XTR_ENV import *

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

# Our include members
import LoggerStandard as EnigmaLog

CALENDAR_BUCKET = rf"{XTR_BUCKET}/{CALENDAR_BUCKET_FLDR}"


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Constants
#############################################################
# Parm Dates to be in YYYYMMDD format


#############################################################
# Functions
#############################################################
def write_sp_info_2_log(sp_info):
        
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    rootLogger.info("\n%s", sp_info.stdout) 
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    rootLogger.info("\n%s", sp_info.stderr) 
    rootLogger.info(f"{sp_info.returncode=}")  


def validate_dt(sDate2Validate, sFormat):

    try:

        datetime_obj = datetime.strptime(sDate2Validate, sFormat)
       
        return datetime_obj
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"DashboardVolReportsDriver.py - Failed ({ENVNAME})"
        MSG=f"Parameter date {sDate2Validate} is either an invalid date or not formatted correctly. Date must be in YYYY-MM-DD format. Process failed. "
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
        write_sp_info_2_log(sp_info)
        
        sys.exit(12)


def creTempTextFile(sFilename, sFileSuffix, sFileContents):

    with tempfile.NamedTemporaryFile(prefix=sFilename, suffix=sFileSuffix, delete=False) as tmp_csv:
        csvExtFilePath = tmp_csv.name

        tmp_csv.write(sFileContents.encode("utf-8"))

    return csvExtFilePath
    

def main_processing_loop():

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}DashboardVolReports_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nDashboardVolReportsDriver.py started at {TMSTMP}")

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


        #######################################################
        # Tell python these are global and not local variables
        #######################################################
        global p_RptPeriod
        global p_OverrideRptYear
        
        #############################################################
        # Retrieve positional parameters for Calendar Report process
        #############################################################            
        rootLogger.info(f"Parameters to script:")
        rootLogger.info(f"NOF parameters for script: {iNOFParms}")

        lstParms = sys.argv

        #############################################################
        # Display parameters passed to script
        # NOTE: RptPeriod: CY, FY, M, CQ, FQ  
        #       If parm2 not sent, calculate it
        #############################################################
        p_RptPeriod = lstParms[1]
        
        # Optional parameter - Start Date for reporting periods
        if iNOFParms ==  2:
            p_OverrideRptYear = lstParms[2]
        else:
            p_OverrideRptYear = ""                
            
        rootLogger.info(f"parameter->{p_RptPeriod=}")
        rootLogger.info(f"parameter->{p_OverrideRptYear=}")        

        #################################################################################
        # Determine Reporting Year for scheduled and on-demand reports
        # 
        # CY and "01" --> RPT_YYYY = Prior Year YYYY     Ex. CurrentDate=2025-01-08 --> reporting on 2024-01-01 thru 2024-12-31
        # FY and "10" --> RPT_YYYY = current date YYYY   Ex. CurrentDate=2024-10-07 --> reporting on 2023-10-01 thru 2024-09-30
        # M           --> RPT_MM   = Month report would run; CurrentDate=2025-01-07 --> reporting on 2024-12-01 thru 2024-12-31
        #
        # Overrider values must follow above rules: 
        # Ex: CY --> OverrideDate YYYY = 2025 will report on 2024-01-01 thru 2024-12-31; To report on 2025 data OverrideDate YYYY = 2026
        # Ex:  M --> OverrideDate YYYYMM = 202501 will report on 2024-12-01 thru 2024-12-31
        #################################################################################
        rootLogger.info("") 

        EXT_TYPE = p_RptPeriod
        rootLogger.info(f"{EXT_TYPE=}")
        
        # Determine RPT_YYYY based on type of report requested
        if p_OverrideRptYear == "":
            dttmCalcDate = datetime.today()
            CUR_MONTH = dttmCalcDate.strftime('%m')
            CUR_YYYY = dttmCalcDate.strftime('%Y')
            
            rootLogger.info(f"{CUR_MONTH=}")
            rootLogger.info(f"{CUR_YYYY}")
            
            if CUR_MONTH == "01" and EXT_TYPE == "CY":
                LAST_DAY_LAST_YYYY = (datetime.strptime(f"{CUR_YYYY}-01-01","%Y-%m-%d") +  timedelta(days=-1)).strftime('%Y-%m-%d')
                RPT_YYYY = LAST_DAY_LAST_YYYY[:4]
            else:
                # If monthly processing OR (FY and OCT) --> use RPT_YYYY=CUR_YYYY
                RPT_YYYY = CUR_YYYY
                RPT_MM = CUR_MONTH

        else:
            if EXT_TYPE == "M":
                RPT_YYYY = p_OverrideRptYear [:4]
                RPT_MM = p_OverrideRptYear [4:6]
            else:
                RPT_YYYY = p_OverrideRptYear
                RPT_MM = ""


        rootLogger.info(f"{RPT_YYYY=}")
        rootLogger.info(f"{RPT_MM=}")
        
        #################################################################################
        # Calculate Report date range and other variables based on Reporting year
        #################################################################################
        rootLogger.info("")

        if EXT_TYPE == "CY":

            EXT_FROM_DT = f"{RPT_YYYY}-01-01"
            EXT_THRU_DT = f"{RPT_YYYY}-12-31"
            RPT_EMAIL_TITLE = f"Calendar Year {RPT_YYYY}"
            
            DASHBOARD_VOL_RPT_TXT_FILE = f"DashboardVolRptData_{RPT_YYYY}_{EXT_TYPE}_{TMSTMP}.txt" 
            HTML_RPT_FILE = f"DashboardVolRptHTML_{RPT_YYYY}_{EXT_TYPE}_{TMSTMP}.txt"
            
        elif EXT_TYPE == "FY":
            PRIOR_YYYY = str(int(RPT_YYYY) - 1)
            
            EXT_FROM_DT = f"{PRIOR_YYYY}-10-01"
            EXT_THRU_DT = f"{RPT_YYYY}-09-30"
            RPT_EMAIL_TITLE = f"Fiscal Year {RPT_YYYY}"
            
            DASHBOARD_VOL_RPT_TXT_FILE = f"DashboardVolRptData_{RPT_YYYY}_{EXT_TYPE}_{TMSTMP}.txt" 
            HTML_RPT_FILE = f"DashboardVolRptHTML_{RPT_YYYY}_{EXT_TYPE}_{TMSTMP}.txt"
            
        elif EXT_TYPE == "M":
            LAST_DAY_LAST_MONTH = (datetime.strptime(f"{RPT_YYYY}-{RPT_MM}-01","%Y-%m-%d") +  timedelta(days=-1)).strftime('%Y-%m-%d')
            rootLogger.info(f"{LAST_DAY_LAST_MONTH=}") 

            LAST_MONTH_YYYY = LAST_DAY_LAST_MONTH [:4]  
            LAST_MONTH_MM = LAST_DAY_LAST_MONTH [5:7]  
            FIRST_DAY_LAST_MONTH = f"{LAST_MONTH_YYYY}-{LAST_MONTH_MM}-01"
            rootLogger.info(f"{FIRST_DAY_LAST_MONTH=}") 
            
            EXT_FROM_DT = FIRST_DAY_LAST_MONTH
            EXT_THRU_DT = LAST_DAY_LAST_MONTH
            RPT_EMAIL_TITLE = f"month of {LAST_MONTH_YYYY}-{LAST_MONTH_MM} "

            DASHBOARD_VOL_RPT_TXT_FILE = f"DashboardVolRptData_{LAST_MONTH_YYYY}_{LAST_MONTH_MM}_{TMSTMP}.txt" 
            HTML_RPT_FILE = f"DashboardVolRptHTML_{LAST_MONTH_YYYY}_{LAST_MONTH_MM}_{TMSTMP}.txt"
            
        else:
            rootLogger.info("")
            rootLogger.info(f"Invalid Report Period {EXT_TYPE} was passed. Script DashboardVolReports.sh failed.")

            # Send Failure email	
            SUBJECT=f"DashboardVolReports.sh - Failed (ENVNAME)"
            MSG=f"Invalid Report Period {EXT_TYPE} was passed. Script DashboardVolReports.sh failed."
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )

            rootLogger.info(sp_info.stdout)        

            sys.exit(12)
        

        rootLogger.info(f"{EXT_FROM_DT=}")
        rootLogger.info(f"{EXT_THRU_DT=}")

        rootLogger.info("")
        rootLogger.info(f"{DASHBOARD_VOL_RPT_TXT_FILE=}") 

        DASHBOARD_VOL_RPT_TXT_FILE_ZIP = f"{DASHBOARD_VOL_RPT_TXT_FILE}.gz" 
        rootLogger.info(f"{DASHBOARD_VOL_RPT_TXT_FILE_ZIP=}") 

        DASHBOARD_VOL_RPT_CSV_FILE = DASHBOARD_VOL_RPT_TXT_FILE.replace(".txt",".csv") 
        rootLogger.info(f"{DASHBOARD_VOL_RPT_CSV_FILE=}") 

        rootLogger.info(f"{HTML_RPT_FILE}")


        #############################################################
        # Export variables for python code
        #############################################################
        os.environ["EXT_FROM_DT"] = EXT_FROM_DT            
        os.environ["EXT_THRU_DT"] = EXT_THRU_DT            
        os.environ["DASHBOARD_VOL_RPT_TXT_FILE_ZIP"] = DASHBOARD_VOL_RPT_TXT_FILE_ZIP            
        os.environ["EXT_TYPE"] = EXT_TYPE    


        #############################################################
        # Execute python script  
        #############################################################
        rootLogger.info("")
        rootLogger.info("Start execution of DashboardVolReports.py program")

        try:
            sp_info = subprocess.run(['python3', 'DashboardVolReports.py'], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info) 
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"Calling DashboardVolReports.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"DashboardVolReports.py - Failed ({ENVNAME})"
            MSG=f"ython script DashboardVolReports.py has failed. "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info) 

            sys.exit(12)    


        rootLogger.info("")
        rootLogger.info("Python script DashboardVolReports.py completed successfully.")


        ##########################################
        # Establish S3 connection
        ##########################################
        rootLogger.info("")
        rootLogger.info("Get s3 connection.")

        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")

        
        ###############################################################
        # Get S3 gzip file object and extract record
        ###############################################################
        s3DashboardFolder_n_filename = f"{DASHBOARD_BUCKET_FLDR}{DASHBOARD_VOL_RPT_TXT_FILE_ZIP}"
        s3DashboardArchiveFolder_n_filename = f"{DASHBOARD_BUCKET_FLDR}archive/{DASHBOARD_VOL_RPT_TXT_FILE_ZIP}"
        
        rootLogger.info(f"{s3DashboardFolder_n_filename=}")
        rootLogger.info(f"{s3DashboardArchiveFolder_n_filename=}")

        rootLogger.info(f"Get s3 file {s3DashboardFolder_n_filename}")
        gzip_file = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3DashboardFolder_n_filename)
         
        if gzip_file == None:
            ## Send Failure email	
            SUBJECT=f"DashboardVolReports.py - Failed ({ENVNAME})"
            MSG=f"Calendar extract file {s3DashboardFolder_n_filename} could not be retrieved from S3. Process failed. "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            rootLogger.info(sp_info)              


        rootLogger.info(f"Read byte stream for file {s3DashboardFolder_n_filename}")
        strmBytes = gzip_file["Body"].read()
        
        rootLogger.info(f"S3 gz file size: {len(strmBytes)}")

 
        ###############################################################
        # Unzip S3 gzip file. Convert byte stream to utf-8 string.
        ###############################################################
        rootLogger.info("Unzip S3 file byte stream and convert to string " )
        
        stxtDashboardVolRpt = gzip.decompress(strmBytes).decode('utf-8')

        unzipped_content_len = len(stxtDashboardVolRpt)
        rootLogger.info(f"Unzipped file size: {unzipped_content_len}")


        #############################################################
        # Move S3 extract file to archive directory 
        #############################################################
        rootLogger.info(f"Move extract file to archive folder")
        rootLogger.info(f"Copy S3 file {s3DashboardFolder_n_filename} to {s3DashboardArchiveFolder_n_filename}")
        s3_client.copy_object(Bucket=XTR_BUCKET, Key=s3DashboardArchiveFolder_n_filename, CopySource={'Bucket': XTR_BUCKET, 'Key': s3DashboardFolder_n_filename} )

        rootLogger.info(f"Delete s3 file {s3DashboardFolder_n_filename}")
        s3_client.delete_object(Bucket=XTR_BUCKET, Key=s3DashboardFolder_n_filename)
 
 
        ###############################################################
        # Convert string into a list of records.
        ###############################################################
        rootLogger.info("Convert string to List or records for processing " )
        lstDashboardRecs = stxtDashboardVolRpt.splitlines()


        #############################################################
        # Build HTML for report by looping thru Calendar recs 
        #############################################################	
        rootLogger.info("*-----------------------*") 
        rootLogger.info("Build HTML report        ")

        # Define loop variables
        bFirstRec = True
        
        sioHTMLReport = StringIO("")
            
        #################################################################################
        # Loop Extract records
        #################################################################################
        for extractRec in lstDashboardRecs:

            rootLogger.info(f"{extractRec=}")
            
            # create list of fields for extRec
            lstExtRecFlds = extractRec.split("|")  

            #######################################
            # set tag type
            #######################################
            if bFirstRec == True: 
                bFirstRec = False
                
                fldTag = "th"

                # Write out HTML header.
                sioHTMLReport.write("<html><body><table cellspacing='1px' border='1' >\n")
                sioHTMLReport.write("<tr bgcolor='#00B0F0'>\n") 

            else:
                fldTag = "td"
                
                sioHTMLReport.write("<tr>") 

            #######################################
            # Loop thru fields in record
            #######################################
            for i, fld in enumerate(lstExtRecFlds):
                if i < 2:
                    sioHTMLReport.write(f"<{fldTag}>{fld}</{fldTag}>")
                else:
                    # Right align numbers
                    sioHTMLReport.write(f"<{fldTag} align='right'>{fld}</{fldTag}>")

            sioHTMLReport.write("</tr>\n")
    

        #################################################################################
        # Write out HTML trailer.
        #################################################################################
        sioHTMLReport.write("</table>\n")
        sioHTMLReport.write("</body></html>")


        #############################################################
        # Create CSV version of file from pipe-delimited file
        #############################################################
        rootLogger.info("")
        rootLogger.info("Create CSV version of report file")

        # position to beginning of the buffer
        sioHTMLReport.seek(0)
        # extract the contents
        sHTMLReport = sioHTMLReport.read()

        tmpTxtExtFilenameNPath = creTempTextFile(DASHBOARD_VOL_RPT_TXT_FILE, ".txt", stxtDashboardVolRpt)

        try:
            sp_info = subprocess.run(['python3', 'utilConvertPipeFile2CSVFile.py', tmpTxtExtFilenameNPath], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)
            
            # utility creates file in same path and filename except with different extension
            tmpCsvExtFilenameNPath = tmpTxtExtFilenameNPath.replace(".txt",".csv")
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"utilConvertPipeFile2CSVFile.py failed with return code {e.returncode}")
            rootLogger.error(e.output)
            
            ## Send Failure email	
            SUBJECT=f"DashboardVolReports.py - Failed ({ENVNAME})"
            MSG=f"Python script DashboardVolReports.py has failed. "
            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info) 

            sys.exit(12)  


        #############################################################
        # Email report 
        #############################################################
        rootLogger.info("")
        rootLogger.info("sHTMLReport=\n%s", sHTMLReport) 
        
        rootLogger.info("")
        rootLogger.info("Send report email")

        SUBJECT = f"Dashboard report for {RPT_EMAIL_TITLE} ({ENVNAME})"
        MSG = f"Dashboard report for {RPT_EMAIL_TITLE}. . .<br><br>{sHTMLReport}"
        
        try:
            sp_info = subprocess.run(['python3', 'sendEmailHTML.py', CMS_EMAIL_SENDER, DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG, tmpCsvExtFilenameNPath], capture_output=True, text=True, check=True )
            write_sp_info_2_log(sp_info)
            
        except subprocess.CalledProcessError as e:
            rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
            rootLogger.error(e.output)

            sys.exit(12)    


        #############################################################
        # script clean-up
        #############################################################
        rootLogger.info("") 
        rootLogger.info("Remove temp files from data directory")  

        os.remove(tmpTxtExtFilenameNPath)
        os.remove(tmpCsvExtFilenameNPath)


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script DashboardVolReportsDriver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in DashboardVolReportsDriver.py\n {e}")

        rootLogger.error("Exception occured in DashboardVolReportsDriver.py.")
        rootLogger.error(e)
        
        ## Send Failure email	
        SUBJECT=f"DashboardVolReports.py - Failed ({ENVNAME})"
        MSG=f"Python script DashboardVolReports.py has failed. Exception occured. \n {e}"
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True )
        write_sp_info_2_log(sp_info) 

        # send email
        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()