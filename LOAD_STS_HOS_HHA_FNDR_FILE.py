#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_STS_HOS_HHA_FNDR_FILE.py
# DESC:   This python program loads the STS_HOS_HHA finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.STS_HOS_HHA_FF table.
#
# Created: Viren Khanna 
# Modified: 02/07/2025
#
# Viren Khanna 2025-02-07 Create script to load STS_HOS_HHA_FF table
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
STS_HOS_HHA_FNDR_FILE=os.getenv('STS_HOS_HHA_FINDER_FILE')


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
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".STS_HOS_HHA_FF""", con, exit_on_error=True)
   
   ## PUT FINDER FILE TO FNDR TABLE ##
   ###snowconvert_helpers.execute_sql_statement(f"""PUT file://{LANDING}{STS_HOS_HHA_FNDR_FILE} @~ OVERWRITE = TRUE""", con,exit_on_error = True)

   ## INSERT FINDER FILE WITH DERIVED FIELDS TO THE TARGET TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.STS_HOS_HHA_FF
         (PRVDR_NUM, PRVDR_NAME, PRVDR_TYPE_ID, HOSPICE_TYPE_CD, HOSPICE_TYPE_DESC, HHA_TYPE_CD, HHA_TYPE_DESC)
FROM (
    SELECT 
        f.$1 AS PRVDR_NUM,
        f.$2 AS PRVDR_NAME,
        f.$6 AS PRVDR_TYPE_ID,
        CASE 
            WHEN f.$6 = '12' AND NULLIF(TRIM(f.$75), '') IS NOT NULL THEN '12'
            ELSE NULL
        END AS HOSPICE_TYPE_CD,
        CASE 
            WHEN f.$6 = '12' AND NULLIF(TRIM(f.$75), '') IS NOT NULL THEN f.$75
            ELSE NULL
        END AS HOSPICE_TYPE_DESC,
        CASE
            WHEN f.$6 = '3' AND NULLIF(TRIM(f.$102), '') IS NOT NULL THEN
                CASE
                    WHEN f.$102 IN ('1', '2', '3', '4', '5', '6', '7') THEN f.$102
                    ELSE NULL  -- Set to NULL if not a valid value
                END
            ELSE NULL
        END AS HHA_TYPE_CD,
        CASE 
            WHEN f.$6 = '3' AND f.$102 = '1' THEN 'VISITING NURSE ASSOCIATION'
            WHEN f.$6 = '3' AND f.$102 = '2' THEN 'COMBINATION GOVT & VOL AGENCY'
            WHEN f.$6 = '3' AND f.$102 = '3' THEN 'OFFICIAL HEALTH'
            WHEN f.$6 = '3' AND f.$102 = '4' THEN 'REHAB FACILITY BASED'
            WHEN f.$6 = '3' AND f.$102 = '5' THEN 'HOSPITAL BASED'
            WHEN f.$6 = '3' AND f.$102 = '6' THEN 'SNF BASED'
            WHEN f.$6 = '3' AND f.$102 = '7' THEN 'OTHER FACILITIES'
            ELSE NULL
        END AS HHA_TYPE_DESC
           FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_STG/{STS_HOS_HHA_FNDR_FILE} f)
	      FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"') FORCE=TRUE """, con,exit_on_error = True)



  ## REMOVE FINDER FILE FROM USER STAGE ##
   snowconvert_helpers.execute_sql_statement(f"""REMOVE @~/{STS_HOS_HHA_FNDR_FILE}.gz""", con,exit_on_error = True)
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