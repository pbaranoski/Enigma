#!/usr/bin/env python
########################################################################################################
# Name:   DashboardInfo_MS.py
# DESC:   This python script loads json file data to UTIL_EXT_RUNS and UTIL_EXT_RUN_EXT_FILES tables.
#
# Created: Paul Baranoski 4/5/2024
# Modified: 
#
# Paul Baranoski 2024-04-05 Created program.
# Paul Baranoski 2025-04-11 Added filter "AND NOT LOG_NAME LIKE '%_NOLOG_%'" to DELETE commands from tables.
#                           This will prevent SFUI extracts from being removed from table since the
#                           information is not extracted from log files. The SFUI extract information 
#                           can be updated using Dashboard_SFUI.sh script using specific override parameters.
#
########################################################################################################
# IMPORTS
########################################################################################################
import os
import sys
import datetime
from datetime import datetime

currentDirectory = os.path.dirname(os.path.realpath(__file__))
rootDirectory = os.path.abspath(os.path.join(currentDirectory, ".."))
utilDirectory = os.getenv('CMN_UTIL')

sys.path.append(rootDirectory)
sys.path.append(utilDirectory)

import snowconvert_helpers
from snowconvert_helpers import Export

########################################################################################################
# VARIABLE ASSIGNMENT
########################################################################################################
script_name = os.path.basename(__file__)
con = None 
now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

ENVNAME=os.getenv('ENVNAME')

RUN_FROM_DT=os.getenv('RUN_FROM_DT')
RUN_TO_DT=os.getenv('RUN_TO_DT')
DASHBOARD_JOBINFO_FILE=os.getenv('DASHBOARD_JOBINFO_FILE')
DASHBOARD_JOBDTLS_FILE=os.getenv('DASHBOARD_JOBDTLS_FILE')

# boolean - Python Exception status
bPythonExceptionOccurred=False

########################################################################################################
# RUN
########################################################################################################
print('')
print("Run date and time: " + date_time  )
print

try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

   # Delete Rows that may exist on tables for current run dates - in case of re-run 
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".UTIL_EXT_RUNS WHERE CAST(RUN_TMSTMP AS DATE) BETWEEN TO_DATE('{RUN_FROM_DT}','YYYYMMDD') AND TO_DATE('{RUN_TO_DT}','YYYYMMDD') AND NOT LOG_NAME LIKE '%_NOLOG_%' """, con, exit_on_error=True)
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".UTIL_EXT_RUN_EXT_FILES WHERE CAST(RUN_TMSTMP AS DATE) BETWEEN TO_DATE('{RUN_FROM_DT}','YYYYMMDD') AND TO_DATE('{RUN_TO_DT}','YYYYMMDD')  AND NOT LOG_NAME LIKE '%_NOLOG_%' """, con, exit_on_error=True)
 
   ## INSERT DATA INTO UTIL_EXT_RUNS TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.UTIL_EXT_RUNS
	(EXT_NAME, LOG_NAME, RUN_TMSTMP, DOW, SUCCESS_IND)
	FROM (SELECT $1:ext,
                 $1:log,
                 TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS'), 
                 DAYNAME(TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS')),
                 $1:success                 
          FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DASHBOARD_STG/{DASHBOARD_JOBINFO_FILE} ) 
	FILE_FORMAT = (TYPE = JSON) FORCE=TRUE """, con,exit_on_error = True)
 
 
      ## INSERT DATA INTO UTIL_EXT_RUN_EXT_FILES TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.UTIL_EXT_RUN_EXT_FILES
	(EXT_NAME, LOG_NAME, RUN_TMSTMP, EXT_FILENAME, REC_COUNT, BYTE_COUNT, HUMAN_FILE_SIZE)
	FROM (SELECT $1:ext,
                 $1:log,
                 TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS') ,
                 $1:ExtractFile,
                 $1:RecCount,  
                 $1:FileByteSize, 
                 $1:HumanFileSize                  
          FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DASHBOARD_STG/{DASHBOARD_JOBDTLS_FILE} ) 
	FILE_FORMAT = (TYPE = JSON) FORCE=TRUE """, con,exit_on_error = True)
 
   snowconvert_helpers.quit_application()

except Exception as e:
   print(e)

   # Let shell script know that python code failed.
   bPythonExceptionOccurred=True  

finally:
   if con is not None:
      con.close()

   # Let shell script know that python code failed.      
   if bPythonExceptionOccurred == True:
      sys.exit(12) 
   else:   
      snowconvert_helpers.quit_application()