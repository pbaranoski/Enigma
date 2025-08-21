#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_NYSPAP_FNDR_FILE.py
# DESC:   This python program loads the NYSPAP finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.NYSPAP_FF table.
#
# Created: Paul Baranoski  
# Modified: 09/28/2022
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

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')
LANDING=os.getenv('DATADIR')
NYSPAP_FNDR_FILE=os.getenv('LOAD_NYSPAP_FINDER_FILE')


# set email variables
sender=os.getenv('NYSPAP_EMAIL_SENDER')
success_receivers=os.getenv('NYSPAP_EMAIL_SUCCESS_RECIPIENT') 

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
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".NYSPAP_FF""", con, exit_on_error=True)
   
   ## PUT FINDER FILE TO FNDR TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""PUT file://{LANDING}{NYSPAP_FNDR_FILE} @~ OVERWRITE = TRUE""", con,exit_on_error = True)

   ## INSERT FINDER FILE WITH DERIVED FIELDS TO THE TARGET TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.NYSPAP_FF
	(BENE_CAN_NUM, BIC_CD, SSN, GENDER, NYEPIC_NUM)
	FROM (SELECT SUBSTR(f.$1, 1, 9) as BENE_CAN_NUM, SUBSTR(f.$1, 10, 2) as BIC_CD, SUBSTR(f.$1, 12, 9) as SSN, 
                 SUBSTR(f.$1, 21, 1) as GENDER, SUBSTR(f.$1, 22, 9) as NYEPIC_NUM  
	      FROM @~/{NYSPAP_FNDR_FILE}.gz f) 
	FILE_FORMAT = (TYPE = CSV)""", con,exit_on_error = True)


   ## REMOVE FINDER FILE FROM USER STAGE ##
   snowconvert_helpers.execute_sql_statement(f"""REMOVE @~/{NYSPAP_FNDR_FILE}.gz""", con,exit_on_error = True)
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