#!/usr/bin/env python
########################################################################################################
# Name:   BAYSTATE_LOAD_SSN_FNDR_FILE.py
# DESC:   This python program loads the MEDPAR BAYSTATE SSN finder file to 
#         BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MEDPAR_BAYSTATE_SSN from S3
#
# Author: Joshua Turner
# Created: 10/05/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2024-02-02   Updated SQL to load FF directly from /Finder_Files folder. 
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
BAYSTATE_SSN_FF=os.getenv('BAYSTATE_SSN_FF')


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
   # Delete everything from MEDPAR_BAYSTATE_SSN
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".MEDPAR_BAYSTATE_SSN""", con, exit_on_error=True)
   
   ########################################################################################################
   # Insert finder file data to the table
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.MEDPAR_BAYSTATE_SSN
	(SSN_NUM)
	FROM (SELECT SUBSTR(f.$1,1,9)
	      FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FINDER_FILE_STG/{BAYSTATE_SSN_FF} f) 
	FILE_FORMAT = (TYPE = CSV)""", con,exit_on_error = True)

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