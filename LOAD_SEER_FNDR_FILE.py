#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_SEER_FNDR_FILE.py
# DESC:   This python program loads a SEER extract finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.SEER_FF table.
#
# Created: Paul Baranoski  
# Modified: 09/17/2024
#
# Paul Baranoski 2024-09-17 Create extract.
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
SEER_FNDR_FILE=os.getenv('SEER_FNDR_FILE')


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
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".SEER_FF""", con, exit_on_error=True)
   
   ## PUT FINDER FILE TO FNDR TABLE ##
   ###snowconvert_helpers.execute_sql_statement(f"""PUT file://{LANDING}{SEER_FNDR_FILE} @~ OVERWRITE = TRUE""", con,exit_on_error = True)

   ## INSERT FINDER FILE WITH DERIVED FIELDS TO THE TARGET TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.SEER_FF
                        (BENE_SSN_NUM, BENE_DOB, BENE_SEX_CD, BENE_MBI_ID, REGISTRY_ID, PATIENT_ID  )
                        FROM (SELECT SUBSTR(f.$1,1,9)   AS BENE_SSN_NUM
                                    ,SUBSTR(f.$1,10,8)  AS BENE_DOB
                                    ,SUBSTR(f.$1,18,1)  AS BENE_SEX_CD
                                    ,SUBSTR(f.$1,19,11) AS BENE_MBI_ID
                                    ,SUBSTR(f.$1,30,02) AS REGISTRY_ID
                                    ,SUBSTR(f.$1,32,8)  AS PATIENT_ID

                        FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FINDER_FILE_STG/{SEER_FNDR_FILE}  f)
                    FILE_FORMAT = (TYPE=CSV ) FORCE=TRUE """, con,exit_on_error = True)


   ## REMOVE FINDER FILE FROM USER STAGE ##
   snowconvert_helpers.execute_sql_statement(f"""REMOVE @~/{SEER_FNDR_FILE}.gz""", con,exit_on_error = True)
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