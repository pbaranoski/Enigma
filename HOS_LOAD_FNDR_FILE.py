#!/usr/bin/env python
########################################################################################################
# Name:  HOS_LOAD_FNDR_FILE.py
# DESC:  This script executes SQL that loads the finder files for H and M contract types to 
#        to the BIA_{ENV}.CMS_TARGET_XTR_{ENV}.HOSHFF and HOSMFF tables.
#
# Author:  Joshua Turner
# Created: 03/28/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2023-10-26   Changed the stage name in the load SQL to read from /Finder_Files
#                                    instead of /HOS (eliminates unneeded copies)          
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

HOSHFF_FILENAME = os.getenv('HOSHFF')
HOSMFF_FILENAME = os.getenv('HOSMFF')
ENVNAME = os.getenv('ENVNAME')
DATADIR = os.getenv('DATADIR')
TMSTMP = os.getenv('TMSTMP')

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
    # Delete all data from both of the FF tables
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".HOSHFF""", con, exit_on_error=True)
    snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".HOSMFF""", con, exit_on_error=True)

    ########################################################################################################
    # Load the files to the staging tables 
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."HOSHFF"
                      (CNTRCT_NUM) FROM
                      (SELECT f.$1 AS CNTRCT_NUM FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FINDER_FILE_STG/{HOSHFF_FILENAME} f)
                      FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=1)
                      FORCE=TRUE;""", con, exit_on_error=True)


    snowconvert_helpers.execute_sql_statement(f"""COPY INTO "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."HOSMFF"
                      (CNTRCT_NUM) FROM
                      (SELECT f.$1 AS CNTRCT_NUM FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FINDER_FILE_STG/{HOSMFF_FILENAME} f)
                      FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=1)
                      FORCE=TRUE;""", con, exit_on_error=True)
    
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