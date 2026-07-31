#!/usr/bin/env python
########################################################################################################
# Name:   DSH_LOAD_DSH_EDX_STAY_TBL.py
# DESC:   This script uploads a CSV file into DSH_EDX_STAY table.
#
# Created: Paul Baranoski 4/23/2024
# Modified: 
#
# Paul Baranoski 2024-04-23 Created program.
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
DSH_EDX_STAY_LOAD_FILE='dsh_data_V1.csv'


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

##       TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS'), 

   ## INSERT DATA INTO UTIL_EXT_RUNS TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DSH_EDX_STAY
    (DSCHRG_DT,	MEDPAR_VSN, HICN, ADM_DT, SPCL_UNIT_CD, LENGTH_OF_STAY, PRE_RLNG_SSI_DAYS, UTLZTN_DAYS, POST_RLNG_SSI_DAYS, 
     GHO_PD_CD, IME_AMT, DRG_AMT, MA_STUS, DSCHRG_DAYS, ADMT_DAYS, FED_FY, PRVDR_ID, MBI_ID, PRVDR_ID_ORIG, ETL_LOAD_TS, ETL_UPDT_TS )
    
        FROM (SELECT SUBSTR(f.$1,1,6)   AS PRVDR_ID
                        ,TO_DATE(SUBSTR(f.$1,7,10),'YYYY-MM-DD') AS DSCHRG_DT
                        ,SUBSTR(f.$1,17,1)  AS MEDPAR_VSN
                        ,SUBSTR(f.$1,18,11) AS HICN
                        ,TO_DATE(SUBSTR(f.$1,29,10),'YYYY-MM-DD') AS ADM_DT
                        ,SUBSTR(f.$1,39,1) AS SPCL_UNIT_CD
                        ,SUBSTR(f.$1,40,7) AS LENGTH_OF_STAY
                        ,SUBSTR(f.$1,47,7) AS PRE_RLNG_SSI_DAYS  
                        ,SUBSTR(f.$1,54,7) AS UTLZTN_DAYS  
                        ,SUBSTR(f.$1,61,7) AS POST_RLNG_SSI_DAYS  
                        ,SUBSTR(f.$1,68,1) AS GHO_PD_CD          
                        ,SUBSTR(f.$1,69,8) AS IME_AMT 
                        ,SUBSTR(f.$1,77,8) AS DRG_AMT 
                        ,SUBSTR(f.$1,85,1) AS MA_STUS 
                        ,SUBSTR(f.$1,86,7) AS DSCHRG_DAYS 
                        ,SUBSTR(f.$1,93,7) AS ADMT_DAYS 
                        ,SUBSTR(f.$1,100,4)  AS FED_FY 
                        ,SUBSTR(f.$1,104,11) AS MBI_ID
                        ,SUBSTR(f.$1,115,6)  AS PRVDR_ID_ORIG
                        ,CURRENT_TIMESTAMP AS ETL_LOAD_TS
                        ,NULL AS ETL_UPDT_TS
          FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DSH_STG/{DSH_EDX_STAY_LOAD_FILE} f ) 
	FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"') FORCE=TRUE """, con,exit_on_error = True)

    
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