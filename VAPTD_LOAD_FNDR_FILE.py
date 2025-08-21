#!/usr/bin/env python
########################################################################################################
# Name:   VAPTD_LOAD_FNDR_FILE.py
# DESC:   This python program loads the VA Part D finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.VA_PTD_MOA_FFTAB table.
#
# Author    : Joshua Turner
# Created   : 12/19/2022
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2024-03-29   Updated Finder File load SQL to read file directly from Finder_Files
#                                    S3 folder. 
# Joshua Turner         2024-05-21   Updated load sql to only include SSN. SSN is the only field used
#                                    and the other fields could cause issue if not submitted right in FF.
#                                    ! Just ensure SSN is in position 3
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

YEAR = os.getenv('CURR_YEAR')
ENVNAME = os.getenv('ENVNAME')
FF_FILENAME = os.getenv('FF_FILENAME')
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
    # Delete all records previously loaded in VA_PTD_MOA_FFTAB
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".VA_PTD_MOA_FFTAB""", con, exit_on_error=True)

    ########################################################################################################
    # Insert finder file data using SI stage connected to the /Finder_Files folder
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".VA_PTD_MOA_FFTAB
        (SSN)
        FROM (SELECT SUBSTR(f.$3,1,9)
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FINDER_FILE_STG/{FF_FILENAME} f)
        FILE_FORMAT = (TYPE=CSV  FIELD_DELIMITER='\\\136' SKIP_HEADER=1) FORCE=TRUE;""", con, exit_on_error=True)
        
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
