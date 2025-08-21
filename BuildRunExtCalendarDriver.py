#!/usr/bin/env python
########################################################################################################
# Name:  BuildRunExtCalendarDriver.py
#
# Desc: Create a Run Extract calendar based on the values in a configuration file and 
#       Year parameter.
#       
#       Execute: python3 BuildRunExtCalendarDriver.py --ProcessingYear 2025
#
# DOW_DOM: For weekly extracts, valid values are: 1) M-F 2) individual days 3) series of days delimited by semi-colon
# !! What about Tue, Thu
#          For non-weekly extracts, valid values are: 
#              LW = Last working day of month    LD = Last day of month 
#              FW = First working day of month   FD = First day of month 
#
# Author     : Paul Baranoski	
# Created    : 07/07/2025
#
# Paul Baranoski 2025-07-07 Create Module.
# Paul Baranoski 2025-07-17 Move placement of code setting the current working directory to be right after
#                           establishing the log file.
########################################################################################################

import boto3 
import logging
import sys
import argparse
import re

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess

# Our common module with variable constants
from SET_XTR_ENV import *


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


#############################################################
# Constants
#############################################################
# M-F
WORKING_DAYS_MASK="12345"
FLD_DELIM="|"

MON_ABREVS="JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC"
MON_ABREVS_DELIM="JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC"

DAYS4MM_NON_LEAP_YR="31|28|31|30|31|30|31|31|30|31|30|31"
DAYS4MM_LEAP_YR="31|29|31|30|31|30|31|31|30|31|30|31"
#DAYS4MM=""
VALID_DOM_LF_VALUES="LWFWLDFD"

ValidNumeredDay = '^[0-9]+$'
ValidDayAbrevAndOcc = '^(SUN|MON|TUE|WED|THU|FRI|SAT)-(1|2|3|4|L|F)$'
ValidLWFWLDFD = '^(LW|FW|LD|FD)$'

		
def setLogging():

    # Configure root logger
    #logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(funcName)-22s %(message)s",
        encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
        #filename=f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"
        handlers=[
        logging.FileHandler(f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)
 
    global rootLogger
    rootLogger = logging.getLogger() 
  
    os.chmod(LOG_DIR, 0o777)  # for Python3
    
    #logger.setLevel(logging.INFO)


def getMatchingDOWDate(parmStartDt, parmDOWMask, parmOcc, parmSign):

	#################################################################################################################	
	# This function will find the 1st, 2nd, 3rd, last Day-of-week (FRI, MON, etc) and set that as the return date.
	# parmStartDt =  (YYYY-MM-01 format) or (YYYY-MM-31 - last day of month) 
	# parmDOWMask = 0-6 reprsenting the day(s) of the week like 5=Fri. "12345" -> every work day; "15" --> Mon and Fri' 
	#           "5" --> Friday
	# parmOcc = 1,2,3 (1st, 2nd, 3rd); 'FW' or 'FD' --> parmOCC = '1' (sign=+); 'LW' or 'LD' --> parmOCC = '1' (sign=-)
	# parmSign = '+' or '-'
	#
	#################################################################################################################	
	
    rootLogger.info(f"{parmStartDt=} ") 
    rootLogger.info(f"{parmDOWMask=} ") 
    rootLogger.info(f"{parmOcc=} ") 
    rootLogger.info(f"{parmSign=} ") 
	
	###########################################################
	# if parmStartDt the search dow? (like Fri) --> skip loop
	# if not dow looking for --> find 
	###########################################################
    NOF_Occ=0

    rootLogger.info("starting loop ") 

    for days_sub in range(0, 31):

        # convert parmStartDt to datetime object
        dttmStartDt = datetime.strptime(parmStartDt,'%Y-%m-%d')

        NOF_DAYS = int(parmSign + str(days_sub) )
        rootLogger.info(f"{NOF_DAYS=}")  

        # Calculate new date
        dttmCalcDate = (dttmStartDt + timedelta(days=NOF_DAYS))
        sCalcDate_YYYYMMDD = dttmCalcDate.strftime('%Y-%m-%d')
        # 0-6 Sun-Sat
        dow_nbr = dttmCalcDate.strftime('%w')

        rootLogger.info(f"{sCalcDate_YYYYMMDD=}")  
        rootLogger.info(f"{dow_nbr=}")  

		# is the date dow = requested DOW?
        MatchIdx = parmDOWMask.find(dow_nbr)
        rootLogger.info(f"{MatchIdx=}")  

		# date dow = requested DOW
        if  MatchIdx != -1:
            NOF_Occ+=1  

            rootLogger.info(f"{NOF_Occ=}")   

            if NOF_Occ == int(parmOcc):
                rootLogger.info("NOF OCC criteria met")    

                # criteria satisfied
                break
    
    return sCalcDate_YYYYMMDD


def getDaysMatchMask(DOW_parm):

    # initialize match Mask
    global REQ_DAYS_MASK 
    REQ_DAYS_MASK = ""

    if DOW_parm == "M-F":
        REQ_DAYS_MASK=WORKING_DAYS_MASK	
    else:
		# parse days by delimiter (,)
		# MON,TUE,WED,THU,FRI,SAT,SUT
		
        DAYS_ARRAY = DOW_parm.split(",")
        rootLogger.info(f"{len(DAYS_ARRAY)=} ") 
        rootLogger.info(f"{DAYS_ARRAY=} ") 

		# Build Days Mask
        for DAY in DAYS_ARRAY:
            rootLogger.info(f"{DAY=} ") 

			# convert Days array to use 3-char day names
            if DAY == "MON":
                REQ_DAYS_MASK=REQ_DAYS_MASK + "1"
            elif DAY ==	"TUE":	
                REQ_DAYS_MASK=REQ_DAYS_MASK + "2"				
            elif DAY ==	"WED":	
                REQ_DAYS_MASK=REQ_DAYS_MASK + "3"				
            elif DAY ==	"THU":	
                REQ_DAYS_MASK=REQ_DAYS_MASK + "4"				
            elif DAY ==	"FRI":	
                REQ_DAYS_MASK=REQ_DAYS_MASK + "5"				
            elif DAY ==	"SAT":	
                REQ_DAYS_MASK=REQ_DAYS_MASK + "6"				
            elif DAY ==	"SUN":	
                REQ_DAYS_MASK=REQ_DAYS_MASK + "0"				

    rootLogger.info(f"{REQ_DAYS_MASK=} ") 

    return REQ_DAYS_MASK
	

def getMonthNumber(p_searchMon: str): 
	
    searchMon = p_searchMon.upper()
    rootLogger.info(f"Search Month parameter {p_searchMon} was upper-cased to {searchMon}.")

    idx = MON_ABREVS.find(searchMon)
    if idx == -1:
        rootLogger.error(f"Search Month parameter {p_searchMon} is not a valid month.")
        sys.exit(12)


    rootLogger.info(f"{searchMon} was found at {idx=}")

    monthNbr = int((idx / 3) + 1)
    rootLogger.info(f"{monthNbr=}")

    MMFormatted = "{:02d}".format(monthNbr)
    rootLogger.info(f"{MMFormatted=}")

    return MMFormatted


def setNOFDaysForYear(sYYYY):

    import calendar

    global DAYS4MM     

    if calendar.isleap(int(sYYYY)):
        NOF_Days_in_Year = 366
        DAYS4MM=DAYS4MM_LEAP_YR
    else:    
        NOF_Days_in_Year = 365
        DAYS4MM=DAYS4MM_NON_LEAP_YR        


    rootLogger.info(f"{NOF_Days_in_Year=}")
    rootLogger.info(f"{DAYS4MM=}")

    return NOF_Days_in_Year


def buildWkCal4Yr(lstCalendarOutputRecs, p_out_rec):

    rootLogger.info(f"{p_out_rec=} ")      

    # Set date to first day of year
    sStartDate = sProcessingYYYY + "-01-01"
    dttmStartDt = datetime.strptime(sStartDate,'%Y-%m-%d')
    rootLogger.info(f"{sStartDate=} ")   

 
	# loop thru days of year to create appropriate calendar records
    for iDays in range(0, 365):

        # Calculate new date
        dttmCalcDate = (dttmStartDt + timedelta(days=iDays))
        sNextDt = dttmCalcDate.strftime('%Y-%m-%d')
        rootLogger.info(f"{sNextDt=} ") 

		# skip if year is not for current processing year
        if sNextDt[:4] != sProcessingYYYY:
            break
		
		# get day of week
        dow_nbr = dttmCalcDate.strftime('%w')    
        rootLogger.info(f"{dow_nbr=} ") 


		# if day of week we need --> create date
        if REQ_DAYS_MASK.find(dow_nbr) != -1: 
		########################################################
		# Build Calendar record --> append calendar info to 
		#                           config record
		########################################################	
            sDOWAbbrev = dttmCalcDate.strftime('%a')   
            sExtDt = sNextDt 

            rootLogger.info(f"This date matches criteria: {sExtDt=} ")  
            rootLogger.info(f"This date matches criteria: {sDOWAbbrev=} ")  
			
			# output record and add Extract day and NOD
            sCalendarOutputRec = sExtDt + FLD_DELIM + sDOWAbbrev + FLD_DELIM + p_out_rec 
            lstCalendarOutputRecs.append(sCalendarOutputRec)


def buildQtrCal4Yr(lstCalendarOutputRecs, p_Months, p_Month_Day, p_out_rec):

    ###################################################
    # p_Months like: "JAN,APR,JUL,OCT" or "JAN,JUL"
    # p_Month_Day: 2-digit day number 
    #             LW,FW,LD,FD,
    #             "FRI-2" (2nd FRI) "FRI-L" (last FRI)
    #             "FRI-F" (first FRI)
    ###################################################
    rootLogger.info(f"{p_Months=} ")
    rootLogger.info(f"{p_Month_Day=} ")
    rootLogger.info(f"{p_out_rec=} ")     

    ###################################################	
    # Local variables 
    ###################################################		
    sQtrDate = ""

    ###################################################	
    # Validate Month_Day
    # valid values --> month number (DD)
    #                 (LW|FW|FD|LD) 
    #                 (FRI-2|FRI-L) etc.
    ###################################################
    reValidNumeredDay = re.compile(ValidNumeredDay)
    reValidDayAbrevAndOcc = re.compile(ValidDayAbrevAndOcc, re.IGNORECASE)
    reValidLWFWLDFD = re.compile(ValidLWFWLDFD, re.IGNORECASE)

	
    if reValidNumeredDay.match(p_Month_Day):
        rootLogger.info(f"Valid month day {p_Month_Day} ")   
		 
    elif reValidDayAbrevAndOcc.match(p_Month_Day):
        rootLogger.info(f"Valid Day and Occurrence - Ex. FRI-2 --> {p_Month_Day} ") 
		
    elif reValidLWFWLDFD.match(p_Month_Day):
        rootLogger.info(f"Valid value (LW|FW|FD|LD) --> {p_Month_Day} ") 

    else:
        rootLogger.info(f"Invalid DOM value {p_Month_Day} in config file record {p_out_rec} ") 	
	
        # Send Failure email	
        SUBJECT=f"BuildRunExtCalendarDriver.py  - Failed ({ENVNAME})"
        MSG=f"Invalid DOM value {p_Month_Day} in config file record {p_out_rec}. Process failed. "
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        rootLogger.info(sp_info)        

        sys.exit(12)


	###################################################	
	# Convert Months comma-delimited string to space-delimited
	# Ex. "JAN,APR,JUL,OCT" --> "JAN APR JUL OCT"
	###################################################
    lsMonthsArray=p_Months.split(",")
    rootLogger.info(f"{lsMonthsArray=} ")  
    rootLogger.info(f"Number of elements in the array: {len(lsMonthsArray)=} ")	
	
	###################################################	
	# Loop thru Months array
	###################################################
    for sMON in lsMonthsArray:
        rootLogger.info("*------------------------------*") 
        rootLogger.info(f"{sMON=}") 

		# Convert Month abbrev to month number
        sMMFormatted = getMonthNumber(sMON)
        rootLogger.info(f"{sMMFormatted=}") 

		#################################################
		# Build date using Month number and month day
		# or Get appropriate date
		# --> Format date in YYYY-MM-DD format
		#################################################
		# last working day for month
        if  p_Month_Day == "LW":

            idx = int(sMMFormatted) - 1
            dd = DAYS4MM.split("|") [idx]

            rootLogger.info(f"{dd=}")
			
            parmStartDt = f"{sProcessingYYYY}-{sMMFormatted}-{dd}"
            parmDOWMask = WORKING_DAYS_MASK
            parmOcc = 1
            parmSign = "-"
				
            sQtrDate = getMatchingDOWDate (parmStartDt, parmDOWMask, parmOcc, parmSign)

        elif  p_Month_Day == "FW":		
			# first working day for month

            parmStartDt = f"{sProcessingYYYY}-{sMMFormatted}-01"
            parmDOWMask = WORKING_DAYS_MASK
            parmOcc = 1
            parmSign = "+"

            sQtrDate = getMatchingDOWDate (parmStartDt, parmDOWMask, parmOcc, parmSign)
            rootLogger.info(f"{sQtrDate=}")         

        elif p_Month_Day == "LD": 	
			# find last day for month

            idx = int(sMMFormatted) - 1
            dd = DAYS4MM.split("|") [idx]
            rootLogger.info(f"{dd=}")
			
			# Set Extract date variable	
            sQtrDate=f"{sProcessingYYYY}-{sMMFormatted}-{dd}"

        elif p_Month_Day == "FD":				
			# find first day for month
            sQtrDate=f"{sProcessingYYYY}-{sMMFormatted}-01"

        elif  reValidDayAbrevAndOcc.match(p_Month_Day):
			# Ex. (FRI-2) --> 2nd FRI of month 
			
			# separate DOW_DAY from modifier
            lsDayAbrevAndOcc = p_Month_Day.split("-")
            # SUN,MON, FRI etc.
            DOW_DAY = lsDayAbrevAndOcc[0]
            # [1234LW]
            DOW_MODIFIER = lsDayAbrevAndOcc[1]

            rootLogger.info(f"{DOW_DAY=}")			
            rootLogger.info(f"{DOW_MODIFIER=}")			

			# Build DOW mask
            sDOWMask = getDaysMatchMask(DOW_DAY)
            parmDOWMask = sDOWMask
			
			# Set parms for function
            if DOW_MODIFIER == "L":

                dd = DAYS4MM.split("|") [int(sMMFormatted - 1)]
                rootLogger.info(f"{dd=}")
			
                parmStartDt = f"{sProcessingYYYY}-{sMMFormatted}-{dd}"
                parmOcc= "1"			
                parmSign = "-"

            elif DOW_MODIFIER == "F": 
                parmStartDt = f"{sProcessingYYYY}-{sMMFormatted}-01"
                parmOcc = "1"	
                parmSign = "+"				
			
            else:
                parmStartDt = f"{sProcessingYYYY}-{sMMFormatted}-01"
                parmOcc = DOW_MODIFIER 
                parmSign = "+"

			# Get matching DOW	
            sQtrDate = getMatchingDOWDate (parmStartDt, parmDOWMask, parmOcc, parmSign) 
			
        else:
			# p_Month_Day is number 

			# !!!!If p_Month_Day is > NOF days per month --> substitute correct NOF days per month
            idx = int(sMMFormatted) - 1
            dd = DAYS4MM.split("|") [idx]
            rootLogger.info(f"{dd=}")

            # p_Monty_Day = '5'  > dd='31' --> Values must be converted to int for comparison to properly work
            if int(p_Month_Day) > int(dd): 
                parmStartDt=f"{sProcessingYYYY}-{sMMFormatted}-{dd}"
            else:
                # format DD = '5' to '05'
                DDFormatted = "{:02d}".format(int(p_Month_Day))
                parmStartDt=f"{sProcessingYYYY}-{sMMFormatted}-{DDFormatted}"
                
            rootLogger.info(f"{parmStartDt=}")    
			
			# find nearest prior working day for config day
            parmDOWMask = WORKING_DAYS_MASK
            parmOcc = "1"
            parmSign = "-"
				
            sQtrDate = getMatchingDOWDate (parmStartDt, parmDOWMask, parmOcc, parmSign)


        rootLogger.info(f"{sQtrDate=}")

		#################################################
		# Build output record	
		#################################################		
        dttmQtrDt = datetime.strptime(sQtrDate,'%Y-%m-%d')	
        sDOWAbbrev = dttmQtrDt.strftime('%a')   

        # output record and add Extract day and NOD
        sCalendarOutputRec = sQtrDate + FLD_DELIM + sDOWAbbrev + FLD_DELIM + p_out_rec 
        lstCalendarOutputRecs.append(sCalendarOutputRec)    


def main_processing_loop():

    try:    

        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        print(f"{TMSTMP=}")

        ##########################################
        # Establish log file
        ##########################################
        setLogging()
        rootLogger.info(f"BuildRunExtCalendarDriver.py started at {TMSTMP}")

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
        parser = argparse.ArgumentParser(description="BuildCalDriver parms")
        parser.add_argument("--ProcessingYear", help="Year to create calendar for: YYYY")
        args = parser.parse_args()

        global sProcessingYYYY 
        sProcessingYYYY = str(args.ProcessingYear)

        ##########################################
        # Set NOF Days for Year
        ##########################################
        # Accept override parameter
        setNOFDaysForYear(sProcessingYYYY)

        ##########################################
        # Establish variables
        ##########################################
        sConfigRec = ""
        lstCalendarOutputRecs = []

        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")
    
        ##########################################
        # main processing
        ##########################################
        rootLogger.info("Processing Configuration File") 

        s3ConfigFolder_n_filename = f"{CONFIG_BUCKET_FLDR}CalendarConfigFile.csv"
        rootLogger.info(f"{s3ConfigFolder_n_filename=}")

        # Is config file in s3?         
        resp = s3_client.list_objects_v2(Bucket=XTR_BUCKET, Prefix=s3ConfigFolder_n_filename)
        if resp == None:
            ## Send Failure email	
            SUBJECT=f"BuildRunExtCalendarDriver.py  - Failed ({ENVNAME})"
            MSG=f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info)        

        
        # Get config file from S3    
        calendarConfigFile = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3ConfigFolder_n_filename)

        if calendarConfigFile == None:
            ## Send Failure email	
            SUBJECT=f"BuildRunExtCalendarDriver.py  - Failed ({ENVNAME})"
            MSG=f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
            #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            rootLogger.info(sp_info)        


        # S3 Body is byte array. Convert byte array to utf-8 string. Splitlines recognizes "\r\n" as end-of-record markers     
        lstConfigRecs = calendarConfigFile["Body"].read().decode('utf-8').splitlines()
        rootLogger.info(f"{lstConfigRecs=}") 

        #username=os.getenv('USER')
        #rootLogger.info(f"{username=}")


        #########################################################
        # iterate thru config records to build calendar entries
        #########################################################       
        for sConfigRec in lstConfigRecs:
        
            rootLogger.info("*****************************") 
            rootLogger.info("read next Config Record") 
            rootLogger.info(f"{sConfigRec=}")  
            lstConfigRecFlds = sConfigRec.split("|") 

            ####################################################
            # parse input record into separte fields by '|'
            # Example: Blbtn,Blue Button,W,M;F,,,N,EFT 
            ####################################################
            ExtractID = lstConfigRecFlds[0].strip() 
            Ext_Desc = lstConfigRecFlds[1].strip()
            TimeFrame = lstConfigRecFlds[2].strip()
            DOW_DOM = lstConfigRecFlds[3].strip()
            Months = lstConfigRecFlds[4].strip()
            Month_Day = lstConfigRecFlds[5].strip()
            FinderFileReq = lstConfigRecFlds[6].strip()
            FF_Pre_Processing = lstConfigRecFlds[7].strip()
            DeliveryMethod = lstConfigRecFlds[8].strip()

            rootLogger.info(f"{ExtractID=}")  	
            rootLogger.info(f"{Ext_Desc=}")  	
            rootLogger.info(f"{TimeFrame=}")        
            rootLogger.info(f"{DOW_DOM=}")        
            rootLogger.info(f"{Months=}")        
            rootLogger.info(f"{Month_Day=}")        
            rootLogger.info(f"{FinderFileReq=}")        
            rootLogger.info(f"{FF_Pre_Processing=}")        
            rootLogger.info(f"{DeliveryMethod=}")         

            ####################################################
            # Create year calendar records for extract
            ####################################################
            if TimeFrame == 'W':
                rootLogger.info("Processing Weekly Extract")   

                DaysMatchMask = getDaysMatchMask(DOW_DOM)
                rootLogger.info(f"{DaysMatchMask=}")  
                buildWkCal4Yr(lstCalendarOutputRecs, sConfigRec) 

            elif TimeFrame == 'M':
                rootLogger.info("Processing Monthly Extract")  
                buildQtrCal4Yr(lstCalendarOutputRecs, MON_ABREVS_DELIM, DOW_DOM, sConfigRec)
                
            elif TimeFrame == 'Q' or TimeFrame == 'S' or TimeFrame == 'A':   
                buildQtrCal4Yr(lstCalendarOutputRecs, Months, Month_Day, sConfigRec)

            else:
                rootLogger.info(f"Invalid extract time frame: {TimeFrame} in config file record {sConfigRec}") 	
            
                ## Send Failure email	
                SUBJECT=f"BuildRunExtCalendarDriver.py  - Failed ({ENVNAME})"
                MSG=f"Invalid extract time frame: {TimeFrame} in config file record {config_rec}. Process failed. "
                #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
                sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
                rootLogger.info(sp_info)
    
                sys.exit(12)	


        ################################################################
        # Convert list of output records into string than byte array 
        ################################################################
        rootLogger.info(f"Convert List of Output records into string and byte array")
            
        sCalendarOutputRec = (os.linesep).join([OutputRec for OutputRec in lstCalendarOutputRecs])
        # join command does not add end-of-record marker after last record
        sCalendarOutputRec += os.linesep
        bCalendarOutputRec = sCalendarOutputRec.encode('utf-8')

        ################################################################
        # Put Calendar file for requested year into S3
        ################################################################  
        s3CalendarOutputFile = f"RunCalendar_{sProcessingYYYY}.txt"
        s3CalendarFolder_n_filename = f"{CALENDAR_BUCKET_FLDR}{s3CalendarOutputFile}"

        rootLogger.info(f"S3 Put object Bucket={XTR_BUCKET} and Key={s3CalendarFolder_n_filename}")
        s3_client.put_object(Bucket=XTR_BUCKET, Key=s3CalendarFolder_n_filename, Body=bCalendarOutputRec )


        ####################################################################
        # Building calendar records based on configuration file is complete. 
        ####################################################################         
        rootLogger.info("All records processed in Configuration File") 


        ####################################################################
        # Load Calendar file into S3 table. 
        ####################################################################  
        rootLogger.info(f"Load SF table with calendar file information for {sProcessingYYYY}") 

        os.environ["ProcessingYYYY"] = sProcessingYYYY
        os.environ["RUN_CALENDAR_OUTPUT_FILE"] = s3CalendarOutputFile

        sp_info = subprocess.check_output(['python3', 'BuildRunExtCalendar.py'], text=True)
        rootLogger.info(sp_info)


        ####################################################################
        # Send success email 
        ####################################################################          
        SUBJECT=f"BuildRunExtCalendarDriver.py completed successfully for {sProcessingYYYY}. ({ENVNAME})"
        MSG="BuildRunExtCalendarDriver.py completed successfully."
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        rootLogger.info(sp_info)

        ####################################################################
        # End of Processing
        ####################################################################          
        # Need these messages for Dashboard
        rootLogger.info("Script BuildRunExtCalendarDriver.py completed successfully.")
        rootLogger.info(f"Ended at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in BuildCalendarDriver.py\n {e}")

        rootLogger.error("Exception occured in BuildCalendarDriver.py.")
        rootLogger.error(e)

        ## Send Failure email	
        SUBJECT=f"BuildRunExtCalendarDriver.py  - Failed ({ENVNAME})"
        MSG=f"Exception occured in BuildCalendarDriver.py. {e} Process failed. "
        #sendEmail.py CMS_EMAIL_SENDER ENIGMA_EMAIL_FAILURE_RECIPIENT SUBJECT MSG 
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        rootLogger.info(sp_info)        

        sys.exit(12)    

if __name__ == "__main__":
    
    main_processing_loop()