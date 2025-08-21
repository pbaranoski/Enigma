#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_MNUP_Monthly_FNDR_FILE.py
# DESC:   This python program loads the MNUP finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MNUP_Monthly_FF table.
#
# Created: Viren Khanna
# Modified: 07/22/2024
#
# Viren Khanna 2024-07-22 Create logic to load finder file from S3 instead of data directory
# Viren Khanna 2024-10-01 - Updated STG table location
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

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')
#LANDING=os.getenv('DATADIR')
MNUP_Monthly_FNDR_FILE=os.getenv('LOAD_MNUP_Monthly_FINDER_FILE')


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
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".MNUP_Monthly_FF""", con, exit_on_error=True)
   
   ## PUT FINDER FILE TO FNDR TABLE ##
   ###snowconvert_helpers.execute_sql_statement(f"""PUT file://{LANDING}{MNUP_Monthly_FNDR_FILE} @~ OVERWRITE = TRUE""", con,exit_on_error = True)

   ## INSERT FINDER FILE WITH DERIVED FIELDS TO THE TARGET TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.MNUP_Monthly_FF
	(BENE_HIC_NUM, BENE_BRTH_DT)
	FROM (SELECT TRIM(SUBSTR(f.$1, 1, 11)) as BENE_HIC_NUM, to_date(SUBSTR(f.$1, 12, 8),'YYYYMMDD') as BENE_BRTH_DT  
          FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_SSA_STG/{MNUP_Monthly_FNDR_FILE} f)
	      FORCE=TRUE FILE_FORMAT = (TYPE = CSV)""", con,exit_on_error = True)


   ## REMOVE FINDER FILE FROM USER STAGE ##
   snowconvert_helpers.execute_sql_statement(f"""REMOVE @~/{MNUP_Monthly_FNDR_FILE}.gz""", con,exit_on_error = True)
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