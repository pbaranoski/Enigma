#!/usr/bin/env python
########################################################################################################
# Name:   OPMHI_LOAD_CPT_EXCL_FNDR_FILE.py
# DESC:   This python program loads the CPT EXCL finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.OPMHI_CPT_EXCL.
#
# Created: Joshua Turner
# Modified: 05/31/2023
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
DATADIR=os.getenv('DATADIR')
OPMHI_CPT_EXCL_FF=os.getenv('OPMHI_CPT_EXCL_FF')


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
   
   ########################################################################################################
   # Delete everything from OPMHI_CPT_EXCL
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".OPMHI_CPT_EXCL""", con, exit_on_error=True)
   
   ######################################################################################################## 
   # Put the finder file to the local stage
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""PUT file://{DATADIR}{OPMHI_CPT_EXCL_FF} @~ OVERWRITE = TRUE""", con,exit_on_error = True)

   ########################################################################################################
   # Insert finder file data to the table
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.OPMHI_CPT_EXCL
	(OPMHI_CPT_EXCL)
	FROM (SELECT SUBSTR(f.$1,1,5)
	      FROM @~/{OPMHI_CPT_EXCL_FF} f) 
	FILE_FORMAT = (TYPE = CSV)""", con,exit_on_error = True)

   ########################################################################################################
   # Remove the finder file from the stage
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""REMOVE @~/{OPMHI_CPT_EXCL_FF}.gz""", con,exit_on_error = True)
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