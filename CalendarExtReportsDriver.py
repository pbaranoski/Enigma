#!/usr/bin/env python
########################################################################################################
# Name:  CalendarExtReportsDriver.py
#
# Description: This script will generate three email reports.
#
# Execute as "python3 CalendarExtReportsDriver.py {RptPeriods} {Starting_Dt} "
#
#     Note: Parameter_1 Required (RptPeriods): Parameter is comma-delimited list of numbers, but can be a single number.
#           The list is the number of days to include from the start date for each report to generate.
#
#           Parameter_2 Optional (Starting_Dt): Reports will be generated using current date unless and "override date" is included
#           when program is executed. Date must be in YYYY-MM-DD format.
#      
#   
# Paul Baranoski   2025-07-22 Create Module.
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

# Our common module with variable constants
from SET_XTR_ENV import *

# contains function to extract extract filenames and record counts
from FilenameCounts import getExtractFilenamesAndCounts

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
def setLogging(LOGNAME):

    # Configure root logger
    #logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(funcName)-22s %(message)s",
        encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
        #filename=f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"
        handlers=[
        logging.FileHandler(f"{LOGNAME}"),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)
 
    global rootLogger
    rootLogger = logging.getLogger() 
  
    os.chmod(LOG_DIR, 0o777)  # for Python3
    
    #logger.setLevel(logging.INFO)


def validate_dt(sDate2Validate, sFormat):

    try:

        datetime_obj = datetime.strptime(sDate2Validate, sFormat)
       
        return datetime_obj
    
    except Exception as ex:
        print(f"Invalid date or date format: {ex}")
        
        ## Send Failure email	
        SUBJECT=f"CalendarExtReportsDriver.py - Failed ({ENVNAME})"
        MSG=f"Parameter date {sDate2Validate} is either an invalid date or not formatted correctly. Date must be in YYYY-MM-DD format. Process failed. "
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        rootLogger.info(sp_info) 
        
        sys.exit(12)
 

def main_processing_loop():

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}CalendarExtReports_{TMSTMP}.log"

        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        setLogging(LOGNAME)
        rootLogger.info(f"\nCalendarExtReportsDriver.py started at {TMSTMP}")

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
        global p_rptperiods
        global p_overrideStartDt
        
        #############################################################
        # Retrieve positional parameters for Calendar Report process
        #############################################################            
        rootLogger.info(f"Parameters to script:")
        rootLogger.info(f"NOF parameters for script: {iNOFParms}")

        lstParms = sys.argv
                    
        # This parameter is required and is a comma delimited list
        p_rptperiods = lstParms[1]
        
        # Optional parameter - Start Date for reporting periods
        if iNOFParms ==  2:
            p_overrideStartDt = lstParms[2]
        else:
            p_overrideStartDt = None                
            
        rootLogger.info(f"parameter->{p_rptperiods=}")
        rootLogger.info(f"parameter->{p_overrideStartDt=}")        
        
        #################################################################################
        # Create list of NOF DAYS reporting periods  
        #################################################################################
        rootLogger.info("")
        rootLogger.info("Create list of NOF Days for each reporting period")

        lstNOFDaysRptPeriods = p_rptperiods.split(",")
        
        rootLogger.info(f"{lstNOFDaysRptPeriods=}")  

        #################################################################################
        # Set begStartRptDt. Use current date by default. Otherwise, use override date.  
        #################################################################################
        if p_overrideStartDt == None:
            rootLogger.info("No override date")
            begStartRptDt = datetime.today().strftime('%Y-%m-%d') 

        else:
            rootLogger.info("Using Override date")

            # If override date is invalid --> program ends
            validate_dt(p_overrideStartDt, sFormat = "%Y-%m-%d")
            begStartRptDt = p_overrideStartDt
            

        #################################################################################
        # Loop thru NOF DAYS reporting periods  
        #################################################################################
        for NOF_DAYS in lstNOFDaysRptPeriods:
            #idx = RptPeriod

            rootLogger.info("")
            rootLogger.info("*-----------------------------------*")
            
            rootLogger.info(f"{NOF_DAYS=}")

            #############################################################
            # Build Reporting date range.
            #############################################################	
            EXT_FROM_DT = begStartRptDt

            # Calculate end date
            dttmStartDt = datetime.strptime(begStartRptDt,'%Y-%m-%d')
            dttmCalcDate = (dttmStartDt + timedelta(days=int(NOF_DAYS)))
            EXT_TO_DT = dttmCalcDate.strftime('%Y-%m-%d')

            rootLogger.info(f"{EXT_FROM_DT=}") 
            rootLogger.info(f"{EXT_TO_DT=}") 
            
            CALENDAR_EXTRACT_RPT_FILE = f"CalendarExtRptData_{NOF_DAYS}Days_{TMSTMP}.txt" 
            CALENDAR_EXTRACT_RPT_FILE_ZIP = f"{CALENDAR_EXTRACT_RPT_FILE}.gz" 

            rootLogger.info(f"{CALENDAR_EXTRACT_RPT_FILE=}") 
            rootLogger.info(f"{CALENDAR_EXTRACT_RPT_FILE_ZIP=}") 

            HTML_RPT = f"CalendarHTMLReport_{NOF_DAYS}_{TMSTMP}.txt"

            
            #############################################################
            # Export variables for python code
            #############################################################
            os.environ["EXT_FROM_DT"] = EXT_FROM_DT            
            os.environ["EXT_TO_DT"] = EXT_TO_DT            
            os.environ["CALENDAR_EXTRACT_RPT_FILE_ZIP"] = CALENDAR_EXTRACT_RPT_FILE_ZIP            


            #############################################################
            # Execute python script  
            #############################################################
            rootLogger.info("")
            rootLogger.info("Start execution of CalendarExtReports.py program")

            try:
                sp_info = subprocess.check_output(['python3', 'CalendarExtReports.py'], text=True)
                rootLogger.info(sp_info) 
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"Calling CalendarExtReports.py failed with return code {e.returncode}")
                rootLogger.error(e.output)
                
                ## Send Failure email	
                SUBJECT=f"CalendarExtReports.py - Failed ({ENVNAME})"
                MSG=f"ython script CalendarExtReports.py has failed. "
                sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
                rootLogger.info(sp_info) 

                sys.exit(12)    


            rootLogger.info("")
            rootLogger.info("Python script CalendarExtReports.py completed successfully.")


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
            s3CalendarFolder_n_filename = f"{CALENDAR_BUCKET_FLDR}{CALENDAR_EXTRACT_RPT_FILE_ZIP}"
            s3CalendarArchiveFolder_n_filename = f"{CALENDAR_BUCKET_FLDR}archive/{CALENDAR_EXTRACT_RPT_FILE_ZIP}"
            
            rootLogger.info(f"{s3CalendarFolder_n_filename=}")
            rootLogger.info(f"{s3CalendarArchiveFolder_n_filename=}")

            rootLogger.info(f"Get s3 file {s3CalendarFolder_n_filename}")
            gzip_file = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3CalendarFolder_n_filename)
            
            if gzip_file == None:
                ## Send Failure email	
                SUBJECT=f"CalendarExtReports_Driver.py  - Failed ({ENVNAME})"
                MSG=f"Calendar extract file {s3CalendarFolder_n_filename} could not be retrieved from S3. Process failed. "
                sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
                rootLogger.info(sp_info)              

 
            logging.info(f"Read byte stream for file {s3CalendarFolder_n_filename}")
            strmBytes = gzip_file["Body"].read()
            
            logging.info(f"S3 gz file size: {len(strmBytes)}")

 
            ###############################################################
            # Decompress S3 gzip file
            ###############################################################
            logging.info("Unzip S3 file byte stream " )
            
            unzipped_content = gzip.decompress(strmBytes)

            unzipped_content_len = len(unzipped_content)
            logging.info(f"Unzipped byte size: {unzipped_content_len}")


            ###############################################################
            # Convert byte stream to string and then list of records.
            ###############################################################
            logging.info("Convert byte stream to string " )
            lstCalendarRecs = unzipped_content.decode('utf-8').splitlines()


            #############################################################
            # Move S3 extract file to archive directory 
            #############################################################
            logging.info(f"Copy S3 file {s3CalendarFolder_n_filename} to {s3CalendarArchiveFolder_n_filename}")
            s3_client.copy_object(Bucket=XTR_BUCKET, Key=s3CalendarArchiveFolder_n_filename, CopySource={'Bucket': XTR_BUCKET, 'Key': s3CalendarFolder_n_filename} )

            logging.info(f"Delete s3 file {s3CalendarFolder_n_filename}")
            s3_client.delete_object(Bucket=XTR_BUCKET, Key=s3CalendarFolder_n_filename)


            #############################################################
            # Build HTML for report by looping thru Calendar recs 
            #############################################################	
            logging.info("*-----------------------*") 
            logging.info("Build HTML report        ")

            # Define loop variables
            bFirstRec = True
            fld11 : int = 10
            
            sioHTMLReport = StringIO("")

            
            #################################################################################
            # Loop Extract records
            #################################################################################
            for extractRec in lstCalendarRecs:

                logging.info("") 
                logging.info(f"{extractRec=}")
                
                # create list of fields for extRec
                lstExtRecFlds = extractRec.split("|")  
                logging.info(f"NOF fields = {len(lstExtRecFlds)}")


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
                    TIMEFRAME_IND = lstExtRecFlds[fld11]
                    logging.info(f"{TIMEFRAME_IND=}")
                
                    fldTag = "td"
                    
                    if TIMEFRAME_IND == "W":
                        sioHTMLReport.write("<tr>") 
                    else:
                        # highlight non-weekly extracts in yellow to stand out to the eye
                        sioHTMLReport.write("<tr bgcolor='#FFFF00'>")
               

                #######################################
                # Loop thru fields in record
                #######################################
                for fld in lstExtRecFlds:
                    sioHTMLReport.write(f"<{fldTag}>{fld}</{fldTag}>")

                sioHTMLReport.write("</tr>\n")


            #################################################################################
            # Write out HTML trailer.
            #################################################################################
            sioHTMLReport.write("</table>\n")
            sioHTMLReport.write("<p>Legend: (W)eekly; (M)onthly; (Q)uarterly; (S)emi-Annual; (A)nnual; MF=Mainframe; FW=First Working Day; LW=Last Working Day</p>\n")
            sioHTMLReport.write("</body></html>")


            #############################################################
            # Email report 
            #############################################################
            # Set pointer to beginning of string
            sioHTMLReport.seek(0)
            RPT_INFO = sioHTMLReport.read()
            rootLogger.info("")
            rootLogger.info(f"{RPT_INFO=}")
            
            rootLogger.info("")
            rootLogger.info("Send report email")
            
            SUBJECT=f"Pending Extracts in the next {NOF_DAYS} days Report ({ENVNAME})"
            MSG=f"Pending Extracts in the next {NOF_DAYS} days from {begStartRptDt}. . .<br><br>{RPT_INFO}"

            try:
                sp_info = subprocess.check_output(['python3', 'sendEmailHTML.py', CMS_EMAIL_SENDER, CALENDAR_EMAIL_SUCCESS_RECIPIENT, SUBJECT, MSG], text=True)
                rootLogger.info(sp_info)
                
            except subprocess.CalledProcessError as e:
                rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
                rootLogger.error(e.output)

                sys.exit(12)    



        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script CalendarExtReportsDriver.py completed successfully.")
        rootLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in CalendarExtReportsDriver.py\n {e}")

        rootLogger.error("Exception occured in CalendarExtReportsDriver.py.")
        rootLogger.error(e)

        sys.exit(12)    


if __name__ == "__main__":
    
    main_processing_loop()