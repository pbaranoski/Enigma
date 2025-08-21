#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_TRICARE_FNDR_FILE.py
# DESC:   This script uploads a Tricare finder file to TRICARE_FINDER_FILE table.
#
# Created: Paul Baranoski 9/12/2023
# Modified: 
#
# Paul Baranoski 2023-09-12 Created program.
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
LANDING=os.getenv('DATADIR')
TRICARE_FINDERFILE=os.getenv('SORTED_COMBINED_TRICARE_FNDR_FILE')


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
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".TRICARE_FINDER_FILE""", con, exit_on_error=True)
   
   ## PUT FINDER FILE TO FNDR TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""PUT file://{LANDING}{TRICARE_FINDERFILE} @~ OVERWRITE = TRUE""", con,exit_on_error = True)

   ## INSERT FINDER FILE WITH DERIVED FIELDS TO THE TARGET TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.TRICARE_FINDER_FILE
	(SSN_NUM)
	FROM (SELECT SUBSTR(f.$1,1,9) as SSN_NUM
	      FROM @~/{TRICARE_FINDERFILE}.gz f) 
	FILE_FORMAT = (TYPE = CSV)""", con,exit_on_error = True)

    
   ## REMOVE FINDER FILE FROM USER STAGE ##
   snowconvert_helpers.execute_sql_statement(f"""REMOVE @~/{TRICARE_FINDERFILE}.gz""", con,exit_on_error = True)
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