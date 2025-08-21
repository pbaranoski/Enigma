#!/usr/bin/env python
########################################################################################################
# Name:   LOAD_PSA_FINDER_FILES.py
#
# DESC:   This python program loads the PSA finder file tables in Snowflake from S3 Finder Files.
#
#         1) PSA_HCPCS_APC_CAT_FF  --> PSA_FINDER_FILE_HCPCS_APC_CATEGORIES_YYYYMMDD.csv
#         2) PSA_APC_CAT_FF        --> PSA_FINDER_FILE_APC_Categories_YYYYMMDD.csv
#         3) PSA_DRG_MDC_FF        --> PSA_FINDER_FILE_DRG_MDC_YYYYMMDD.csv 
#         4) PSA_PRVDR_SPCLTY_FF   --> PSA_FINDER_FILE_PRVDR_SPCLTY_CDS_YYYYMMDD.csv
#
# Created: Paul Baranoski  
# Modified: 12/08/2023   
#
# Paul Baranoski 2023-12-08 Create script
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

PSA_FF_HCPCS_APC_CAT_S3FILENAME=os.getenv('PSA_FF_HCPCS_APC_CAT_S3FILENAME')
PSA_FF_APC_CAT_S3FILENAME=os.getenv('PSA_FF_APC_CAT_S3FILENAME')
PSA_FF_DRG_MDC_S3FILENAME=os.getenv('PSA_FF_DRG_MDC_S3FILENAME')
PSA_FF_PRVDR_SPCLTY_S3FILENAME=os.getenv('PSA_FF_PRVDR_SPCLTY_S3FILENAME')


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

  
   #----------------------------------------------------------
   # Load PSA_HCPCS_APC_CAT_FF
   #----------------------------------------------------------
   print("")
   print("Load PSA_HCPCS_APC_CAT_FF SF Finder File Table")

   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".PSA_HCPCS_APC_CAT_FF""", con, exit_on_error=True)
       
   ## INSERT Finder File Values to the Target SF Finder File Table
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.PSA_HCPCS_APC_CAT_FF
        (HCPCS_CD, APC_CD, CAT_CD, CAT_DESC)
        FROM (SELECT f.$1, '0'||f.$2, f.$3, f.$4        
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_STG/{PSA_FF_HCPCS_APC_CAT_S3FILENAME} f)
         FORCE=TRUE      
        FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER=1 )"""
    , con,exit_on_error = True)

   #----------------------------------------------------------
   # Load PSA_APC_CAT_FF
   #----------------------------------------------------------
   print("")
   print("Load PSA_APC_CAT_FF SF Finder File Table")

   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".PSA_APC_CAT_FF""", con, exit_on_error=True)
       
   ## INSERT Finder File Values to the Target SF Finder File Table
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.PSA_APC_CAT_FF
        (APC_CD, CAT_CD, CAT_DESC)
        FROM (SELECT '0'||f.$1, f.$2, f.$3        
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_STG/{PSA_FF_APC_CAT_S3FILENAME} f)
         FORCE=TRUE      
        FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER=1 )"""
    , con,exit_on_error = True)

   #----------------------------------------------------------
   # Load PSA_DRG_MDC_FF
   #----------------------------------------------------------
   print("")
   print("Load PSA_DRG_MDC_FF SF Finder File Table")
    
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".PSA_DRG_MDC_FF""", con, exit_on_error=True)
       
   ## INSERT Finder File Values to the Target SF Finder File Table
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.PSA_DRG_MDC_FF
        (DRG_CD, MDC_CD, MDC_DESC)
        FROM (SELECT f.$1, f.$2, f.$3
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_STG/{PSA_FF_DRG_MDC_S3FILENAME} f)
         FORCE=TRUE      
        FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER=1 )"""
    , con,exit_on_error = True)
    
   #----------------------------------------------------------
   # Load PSA_PRVDR_SPCLTY_FF
   #----------------------------------------------------------
   print("")
   print("Load PSA_PRVDR_SPCLTY_FF SF Finder File Table")
    
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}".PSA_PRVDR_SPCLTY_FF""", con, exit_on_error=True)
       
   ## INSERT Finder File Values to the Target SF Finder File Table
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.PSA_PRVDR_SPCLTY_FF
        (PRVDR_SPCLTY_CD, PRVDR_SPCLTY_DESC)
        FROM (SELECT f.$1, f.$2
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FF_STG/{PSA_FF_PRVDR_SPCLTY_S3FILENAME} f)
         FORCE=TRUE      
        FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER=1 )"""
    , con,exit_on_error = True)
    
    
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