#!/usr/bin/env python
########################################################################################################
# Name:  MEDPAC_Extract.py
# DESC:  This python program pulls data from IDRC for the MEDPAC HOSPICE extract
#
# Created: Joshua Turner
# Modified: 01/27/2023
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

YEAR = os.getenv('YEAR')
ENVNAME = os.getenv('ENVNAME')
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
    # Extract MEDPAC HOSPICE data and write to S3 as a flat file
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_MEDPACHOS_STG/MEDPAC_Y{YEAR}_FILE_{TMSTMP}.csv.gz
            FROM (
                SELECT DISTINCT
                    RPAD(COALESCE(H.BENE_CAN_NUM, ''),9,' ') AS BENE_CAN_NUM,
                    RPAD(COALESCE(H.BENE_BIC_CD, ''),2, ' ') AS BIC_CD,
                    LPAD(COALESCE(TO_CHAR(H.BENE_LINK_KEY),''),11,' ') AS BENE_LINK_KEY,
                    RPAD(COALESCE(TO_CHAR(BH.BENE_RNG_BGN_DT, 'YYYYMMDD'),''),8,' ') AS HOSPC_EFCTV_DT,
                    RPAD(COALESCE(BH.BENE_HOSPC_PRVDR_NUM,''),13,' ') AS HOSPC_PRVDR_NUM,
                    COALESCE(BH.BENE_HOSPC_RVCTN_CD,' ') AS HOSPC_RVCTN_CD,
                    RPAD(COALESCE(TO_CHAR(BH.BENE_RNG_END_DT, 'YYYYMMDD'),''),8,' ') AS HOSPC_TERMNTN_DT,
                    RPAD(COALESCE(BH.BENE_HOSPC_NPI_NUM ,''),10,' ') AS NPI_CD,
                    RPAD(COALESCE(TO_CHAR(BH.IDR_INSRT_TS , 'YYYYMMDD HH:MI:SS.FF6'),''),26,' ') AS IDR_INSRT_TS,
                    RPAD(COALESCE(TO_CHAR(BH.IDR_UPDT_TS , 'YYYYMMDD HH:MI:SS.FF6'),''),26,' ') AS IDR_UPDT_TS,
                    RPAD(COALESCE(TO_CHAR(H.BENE_DEATH_DT, 'YYYYMMDD'),''),8,' ') AS BENE_DEATH_DT,
                    RPAD(COALESCE(H.BENE_DOD_PROOF_CD,''),2,' ') AS BENE_DOD_PROOF_CD,
                    COALESCE(H.BENE_VRFY_DEATH_DAY_SW,' ') AS VRFY_DEATH_DAY_SW,
                    RPAD(COALESCE(H.BENE_SSN_NUM,''),9,' ') AS SSN_NUM,
                    RPAD(COALESCE(H.BENE_MBI_ID,''),11,' ') AS MBI_ID,
                    RPAD('',55,' ') AS FILLER
                FROM "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE_HOSPC" BH
                INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE" H
                    ON H.BENE_SK = BH.BENE_SK
                WHERE
                /* the following date need to be changed based on user's requirement */
                    (H.BENE_DEATH_DT  > '2008-12-31' OR H.BENE_DEATH_DT IS NULL)
                    AND BH.IDR_LTST_TRANS_FLG = 'Y'
                ORDER BY BENE_CAN_NUM,BIC_CD,HOSPC_EFCTV_DT,HOSPC_TERMNTN_DT
            ) FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER=NONE ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
              max_file_size=5368709120 """, con, exit_on_error=True)
              
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
