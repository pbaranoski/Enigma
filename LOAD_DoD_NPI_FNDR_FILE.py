#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_DOD_NPI_FNDR_FILE.py
# DESC:   This python program loads the DOD NPI finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.DOD_NPI_FF table.
#
# Paul Baranoski 2025-09-11 Create script.
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
LOAD_FNDR_FILE=os.getenv('LOAD_FINDER_FILE')


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
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".DOD_NPI_YEAR_FF""", con, exit_on_error=True)
   
   # 123-11-1234  - pos 32= 6 digit # what is it?
   ## INSERT FINDER FILE WITH DERIVED FIELDS TO THE TARGET TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DOD_NPI_YEAR_FF
	(SSN_NUM, EMP_ID)
	FROM (SELECT SUBSTR(f.$1, 1, 3)||SUBSTR(f.$1,5,2)||SUBSTR(f.$1,8,4) AS SSN_NUM, SUBSTR(f.$2,1,7) AS EMP_ID  
          FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_STG/{LOAD_FNDR_FILE} f)
	      FORCE=TRUE FILE_FORMAT = (TYPE = CSV)""", con,exit_on_error = True)

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