#!/usr/bin/bash
#
########################################################################
# Name: PartABExtract.sh
#
# Desc:
#
# Created: Sean Whitelock
# Modified:
#
# Paul Baranoski 2025-12-16 Added "Ended at `date` " to log file. This is the phrase the Dashboard script/program 
#                           is looking for to determine if extract ended successfully.  
# Paul Baranoski 2026-03-18 Converted bash script to python.
#########################################################################

########################################################################################################
# Set TESTING status 
########################################################################################################
import os
os.environ["TESTING"] = "N"

# Our common module with variable constants
from SET_XTR_ENV import *

########################################################################################################
# IMPORTS
########################################################################################################
import boto3

import logging
import sys
import argparse
import re
import io


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


def build_html_report(sPath, sInputFilename, sOutputFilename):
    
    rootLogger.info(f"{sPath=}")
    rootLogger.info(f"Building HTML report: {sOutputFilename} from {sInputFilename}")

    with open(f"{sPath}{sOutputFilename}", "w") as outfile:

        # modified to set font info at table cell level instead of for each td cell 
        outfile.write("<html><body><table cellspacing='1px' border='1' style='font: bold 8pt Arial;'>\n")
        swFirstRecord = "Y"

        with open(f"{sPath}{sInputFilename}", "r") as infile:
            for sLine in infile:

                # write <tr> tag
                if swFirstRecord == "Y":
                    swFirstRecord  = "N"
                    outfile.write("<tr bgcolor='#C5D9F1'>") 
                    TAG = "th"
                else:
                    outfile.write("<tr>") 
                    TAG = "td"
                
                # Create array
                lstFields = sLine.split("|")

                # Iterate thru fields in record
                colNum=0
                for colNum, fld in enumerate(lstFields):

                    # Apply numeric formatting only on data rows (TAG == td). Skipping first 3 columns (0-based)
                    if 3 <= colNum <= 8 and TAG == "td":
                        # convert "fld" from string to float. Format float field with 1) commas as thousand separators; 2) two decimal digits
                        formattedFld = f"{float(fld):,.2f}"
                        ###outfile.write(f"<{TAG} style='font-family:Arial;font-size:8pt;font-weight:bold;' align='right'>{formattedFld}</{TAG}>")
                        outfile.write(f"<{TAG} align='right'>{formattedFld}</{TAG}>")

                    else:
                        # Non-numberic column or header row
                        ###outfile.write(f"<{TAG} style='font-family:Arial;font-size:8pt;font-weight:bold;'>{fld}</{TAG}>")
                        outfile.write(f"<{TAG} align='left'>{fld}</{TAG}>")
 
                # write row ending tag. Added newline to improve readability when debugging
                outfile.write("</tr>\n")

        # Write ending html tags
        outfile.write("</table></body></html>")


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

        LOGNAME = f"{LOG_DIR}{TESTLOG}Part_AB_{TMSTMP}.log" 


        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        #global rootLogger
        rootLogger = EnigmaLog.setLogging(LOGNAME)
        rootLogger.info(f"\nPart_AB.sh started at {TMSTMP}")

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
        

        ####################################################
        # Calculate Date Variables
        ####################################################
        dttmCurrentDt = date.today()
        #DATETIME = datetime.today().strftime("%m%d%y_%H%M%S") 
        
        CUR_DT = dttmCurrentDt.strftime("%Y-%m-%d")

        CUR_YR = CUR_DT[0:4]
        PRIOR_YR = str(int(CUR_YR) - 1)
        NEXT_YR = str(int(CUR_YR) + 1)

        CUR_MNTH = CUR_DT[5:7]

        if int(CUR_MNTH) >= 11:
            YEAR = f"{CUR_YR}-10-01"
          
            # get the prior month dates: 1st day and last day
            ENDMONTH = (dttmCurrentDt.replace(day=1) - relativedelta(days=1)).strftime("%Y-%m-%d")
            BEGMONTH = (dttmCurrentDt - relativedelta(months=1)).strftime("%Y-%m-01")

            RPTMTH = dttmCurrentDt.strftime("%m/%d/%Y")
            MONRPTMTH = (dttmCurrentDt - relativedelta(months=1)).strftime("%B")
            FFRPTYEAR = NEXT_YR
        else: 
            YEAR = f"{PRIOR_YR}-10-01"

            # get the prior month dates: 1st day and last day
            ENDMONTH = (dttmCurrentDt.replace(day=1) - relativedelta(days=1)).strftime("%Y-%m-%d")
            BEGMONTH = (dttmCurrentDt - relativedelta(months=1)).strftime("%Y-%m-01")

            RPTMTH = dttmCurrentDt.strftime("%m/%d/%Y")
            MONRPTMTH = (dttmCurrentDt - relativedelta(months=1)).strftime("%B")
            FFRPTYEAR = CUR_YR


        ####################################################
        # Set variables
        ####################################################
        MNTH = ENDMONTH[5:7]
        RPTYEAR = ENDMONTH[0:4]

        Sheet1 = f"YTD{FFRPTYEAR}"
        Sheet2 = f"{RPTYEAR}{MNTH}"

        YEAR_FILE_ZIP = f"PartAB_Year_{TMSTMP}.txt.gz"
        MONTH_FILE_ZIP = f"PartAB_Month_{TMSTMP}.txt.gz"

        ####################################################
        # Display variable to log
        ####################################################
        rootLogger.info(f"Reporting Month={MNTH}") 
        rootLogger.info(f"Report Run Date={RPTMTH}") 
        rootLogger.info(f"YTD_Year={YEAR}") 
        rootLogger.info(f"BegMonth={BEGMONTH}") 
        rootLogger.info(f"EndMonth={ENDMONTH}") 
        rootLogger.info(f"Federal Fiscal Year={FFRPTYEAR}") 
        rootLogger.info(f"Sheet1=YTD{FFRPTYEAR}") 
        rootLogger.info(f"Sheet2={RPTYEAR}{MNTH}") 
        
        ####################################################
        # Export variables needed by Python SQL program
        ####################################################
        os.environ["YEAR"] = YEAR  
        os.environ["BEGMONTH"] = BEGMONTH  
        os.environ["ENDMONTH"] = ENDMONTH  
        os.environ["FISCAL_YEAR_START"] = YEAR  
        os.environ["TMSTMP"] = TMSTMP  
        os.environ["YEAR_FILE_ZIP"] = YEAR_FILE_ZIP  
        os.environ["MONTH_FILE_ZIP"] = MONTH_FILE_ZIP  

        ############################################################################################################
        # Run Python Report Script
        ############################################################################################################
        # check=True has been removed from call.
        # check=True will generate an exception if RC != 0; I want to handle bad RC myself so any stdout and stderr output is saved to the log file 
        ############################################################################################################
        sp_info = subprocess.run(['python3', 'PartABExtract.py'], capture_output=True, text=True)
        write_sp_info_2_log(sp_info)  
        
        if getRC(sp_info) != 0:
            rootLogger.info("")
            rootLogger.info(f"Shell script PartABExtract.sh failed.")
            
            # Send Failure email	
            SUBJECT=f"Shell script PartABExtract.sh failed. ({ENVNAME}{TESTEMAIL})"
            MSG=f"Shell script PartABExtract.sh failed. "

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENGIMA_EMAIL_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
            write_sp_info_2_log(sp_info)  
                            
            raise RuntimeError(sp_info.stderr)        

        # Extract ran successfully
        rootLogger.info("PartABExtract.py completed successfully")


        #############################################################
        # Download extract files from S3 to linux data directory  
        #############################################################
        rootLogger.info("")

        # Download YEAR file
        rootLogger.info("Download S3 Extract Year file to linux data directory ")
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{PARTAB_BUCKET_FLDR}{YEAR_FILE_ZIP}", f"{DATADIR}{YEAR_FILE_ZIP}")

        # Download MONTH file
        rootLogger.info("Download S3 Extract Month file to linux data directory ")
        downloadFileFromS3(s3_client, XTR_BUCKET, f"{PARTAB_BUCKET_FLDR}{MONTH_FILE_ZIP}", f"{DATADIR}{MONTH_FILE_ZIP}")


        ############################################################################################################
        # Unzip YEAR and MONTH files
        ############################################################################################################
        rootLogger.info("")
        
        rootLogger.info("Unzip Extract Year file on data directory ")
        YEAR_TXT_FILE = unzipFile(DATADIR, YEAR_FILE_ZIP)

        rootLogger.info("Unzip Extract Month file on data directory ")
        MONTH_TXT_FILE = unzipFile(DATADIR, MONTH_FILE_ZIP)


        #############################################################
        # Move S3 Year and Month extract files to archive directory
        #############################################################
        rootLogger.info("")

        # Move Year file to archive folder
        rootLogger.info("Move PartAB Year Extract file to S3 archive folder")
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{PARTAB_BUCKET_FLDR}{YEAR_FILE_ZIP}", f"{PARTAB_BUCKET_FLDR}archive/{YEAR_FILE_ZIP}")

        rootLogger.info("Move PartAB Month Extract file to S3 archive folder")
        s3MoveFile2NewFolder(s3_client, XTR_BUCKET, f"{PARTAB_BUCKET_FLDR}{MONTH_FILE_ZIP}", f"{PARTAB_BUCKET_FLDR}archive/{MONTH_FILE_ZIP}")

        rootLogger.info("Successfully moved PartAB Year and Month extract files to S3 archive folder.")


        ############################################################################################################
        # Build HTML Reports
        ############################################################################################################
        rootLogger.info("")
        
        HTML_YEAR_FILE = f"PartAB_Year_Report_{TMSTMP}.html"
        HTML_MONTH_FILE = f"PartAB_Month_Report_{TMSTMP}.html"

        rootLogger.info("Build Part AB Year Report.")
        build_html_report(DATADIR, YEAR_TXT_FILE, HTML_YEAR_FILE)

        rootLogger.info("Build Part AB Month Report.")
        build_html_report(DATADIR, MONTH_TXT_FILE, HTML_MONTH_FILE)


        #############################################################
        # Create CSV version of file from pipe-delimited file.
        # These files will be attachments to the email.
        #############################################################
        rootLogger.info("")

        rootLogger.info("Create CSV version of Year report file")
        createCSVFileFromDelimitedFile(DATADIR, YEAR_TXT_FILE, f"PartAB_Year_{TMSTMP}.csv", "|")

        rootLogger.info("Create CSV version of Month report file")
        createCSVFileFromDelimitedFile(DATADIR, MONTH_TXT_FILE, f"PartAB_Month_{TMSTMP}.csv", "|")


        #############################################################
        # Load HTML files into variables so the contents can be part 
        #  of the body of the email.
        #############################################################
        rootLogger.info("")

        rootLogger.info("Load HTML YEAR file into variable to display in email.")
        RPT_YEAR_HTML = loadVariableWithFileContents(DATADIR, HTML_YEAR_FILE)

        rootLogger.info("Load HTML MONTH file into variable to display in email.")
        RPT_MONTH_HTML = loadVariableWithFileContents(DATADIR, HTML_MONTH_FILE)
       
        
        #############################################################
        # Email report with CSV attachment
        #############################################################
        rootLogger.info("")
        rootLogger.info("Send email with attached CSVs")


        SUBJECT = f"Complete--->Remedy_374923 Part AB Payments {MONRPTMTH} {RPTYEAR} - ({ENVNAME}{TESTEMAIL})"
        MSG = f"""<p>Hi All,</p>
        <p>Please find the attached Medicare Part A/B Payments reports for the month of {MONRPTMTH} {RPTYEAR} and Federal Fiscal Year {FFRPTYEAR} YTD. (Report Run Date={RPTMTH}).</p>
        <p>Please let us know if you have any questions.</p>
        <p>Thanks,<br>
        BIT Support Team</p>
        <br>
        <p><b>YTD Report:</b></p>
        {RPT_YEAR_HTML}
        <br>
        <p><b>Monthly Report:</b></p>
        {RPT_MONTH_HTML}"""

        ############################################################################################################
        # check=True has been removed from call.
        # check=True will generate an exception if RC != 0; I want to handle bad RC myself so any stdout and stderr output is saved to the log file 
        ############################################################################################################
        sp_info = subprocess.run(['python3', 'sendEmailHTML.py', CMS_EMAIL_SENDER, PART_AB_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG, f"{DATADIR}PartAB_Year_{TMSTMP}.csv,{DATADIR}PartAB_Month_{TMSTMP}.csv"], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info)  

        if getRC(sp_info) != 0:
            rootLogger.info("")
            rootLogger.info(f"Shell script PartABExtract.sh failed.")
            
            # Send Failure email	
            SUBJECT=f"Shell script PartABExtract.sh failed. ({ENVNAME}{TESTEMAIL})"
            MSG=f"Shell script PartABExtract.sh failed. "

            sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENGIMA_EMAIL_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True)
            write_sp_info_2_log(sp_info)  
                            
            raise RuntimeError(sp_info.stderr)        


        ############################################################################################################
        # Script Clean-up
        ############################################################################################################
        rootLogger.info("")
        rootLogger.info("Clean up temporary report files from data directory")

        deleteFileFromLinux(f"{DATADIR}{YEAR_TXT_FILE}")
        deleteFileFromLinux(f"{DATADIR}{MONTH_TXT_FILE}")

        deleteFileFromLinux(f"{DATADIR}{HTML_YEAR_FILE}")
        deleteFileFromLinux(f"{DATADIR}{HTML_MONTH_FILE}")

        deleteFileFromLinux(f"{DATADIR}PartAB_Year_{TMSTMP}.csv")
        deleteFileFromLinux(f"{DATADIR}PartAB_Month_{TMSTMP}.csv")


        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################     -
        rootLogger.info("")
        rootLogger.info("PartABExtractDriver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )
        sys.exit(0)


  except Exception as e:
        print (f"Exception occured in PartABExtractDriver.py\n {e}")

        rootLogger.error("Exception occured in PartABExtractDriver.py.")
        rootLogger.error("\n%s", str(e))
        
        ## Send Failure email	
        SUBJECT=f"PartABExtractDriver.py  - Failed ({ENVNAME}{TESTEMAIL})"
        MSG=f"Exception occured in PartABExtractDriver.py {e}. Process failed. "

        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        rootLogger.info(sp_info)        

        sys.exit(12)    


if __name__ == "__main__":

        main_processing_loop()
