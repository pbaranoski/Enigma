#!/usr/bin/env python
########################################################################################################
# Name:   BAYSTATE_Extract.py
# DESC:   This python program extracts the MEDPAR Bay State SSA (full SSN) file 
#
# Author    : Joshua Turner
# Created   : 10/05/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2024-01-09   Updated SQL to pull SSN from BENE_ID_TRKNG instead of BENE. 
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
TMSTMP=os.getenv('TMSTMP')
FNAME_SUFFIX=os.getenv('FNAME_SUFFIX')

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
   # Extract MEDPBAY Bay State SSA file
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_MEDPARBAYST_STG/MEDPAR_BAYSTATE_SSA_{FNAME_SUFFIX}_{TMSTMP}.txt.gz
            FROM (
                WITH CTE_CNT AS (
                    SELECT 
                        BTRK.BENE_XREF_EFCTV_SK, 
                        COUNT(DISTINCT BENE_ID_NUM) AS HICN_COUNT
                    FROM IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE_ID_TRKNG BTRK
                    INNER JOIN
                        (SELECT DISTINCT BENE_XREF_EFCTV_SK
                         FROM IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE_ID_TRKNG
                         WHERE BENE_SSN_NUM IN (SELECT SSN_NUM FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.MEDPAR_BAYSTATE_SSN)) BTRK1
                    ON BTRK.BENE_XREF_EFCTV_SK = BTRK1.BENE_XREF_EFCTV_SK AND
                       BENE_ID_TYPE_CD IN ('H','I','R','U')
                    GROUP BY 1
                    HAVING HICN_COUNT >= 1
                ),
                CTE_SSN AS (
                    SELECT
                        BENE_XREF_EFCTV_SK,
                        BENE_ID_NUM,
                        MAX(IDR_TRANS_EFCTV_TS) AS EFCTV_TS
                    FROM IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE_ID_TRKNG
                    WHERE BENE_XREF_EFCTV_SK IN (SELECT BENE_XREF_EFCTV_SK FROM CTE_CNT)
                      AND BENE_ID_TYPE_CD IN ('H','I','R','U')
                    GROUP BY 1, 2
                ),
                CTE_NUMBERED AS (
                    SELECT 
                        BENE_XREF_EFCTV_SK,
                        BENE_ID_NUM,
                        'HICN-' ||
                        TRIM(ROW_NUMBER()
                        OVER(PARTITION BY BENE_XREF_EFCTV_SK ORDER BY EFCTV_TS DESC)) AS HICN_XREF
                    FROM CTE_SSN
                ),
                CTE_PIVOT AS (
                    SELECT * 
                    FROM CTE_NUMBERED
                    PIVOT(MAX(BENE_ID_NUM) FOR HICN_XREF IN ('HICN-1',
                    'HICN-2','HICN-3','HICN-4',
                    'HICN-5','HICN-6','HICN-7',
                    'HICN-8','HICN-9','HICN-10')) P
                    (BENE_XREF_EFCTV_SK,HICN1,HICN2,HICN3,HICN4,HICN5,HICN6,HICN7,HICN8,HICN9,HICN10)
                )
                SELECT  
                    RPAD(COALESCE(TRKNG.BENE_SSN_NUM,''),9,' ') AS BENE_SSN_NUM,
                    RPAD(COALESCE(BENE_CAN_NUM,''),9,' ') AS BENE_CAN_NUM,
                    RPAD(COALESCE(BENE_BIC_CD,''),2,' ') AS BENE_BIC_CD,
                    RPAD(COALESCE(BENE_SEX_CD,''),1,' ') AS BENE_SEX_CD,
                    RPAD(COALESCE(CTE.HICN1,''),11,' ') AS HICN1,
                    RPAD(COALESCE(CTE.HICN2,''),11,' ') AS HICN2,
                    RPAD(COALESCE(CTE.HICN3,''),11,' ') AS HICN3,
                    RPAD(COALESCE(CTE.HICN4,''),11,' ') AS HICN4,
                    RPAD(COALESCE(CTE.HICN5,''),11,' ') AS HICN5,
                    RPAD(COALESCE(CTE.HICN6,''),11,' ') AS HICN6,
                    RPAD(COALESCE(CTE.HICN7,''),11,' ') AS HICN7,
                    RPAD(COALESCE(CTE.HICN8,''),11,' ') AS HICN8,
                    RPAD(COALESCE(CTE.HICN9,''),11,' ') AS HICN9,
                    RPAD(COALESCE(CTE.HICN10,''),11,' ') AS HICN10,
                    LPAD(TO_CHAR(COALESCE(HICN_COUNT,0)),2,'0') AS HICN_COUNT,
                    RPAD(COALESCE(BENE_LAST_NAME,''),24,' ') AS BENE_LAST_NAME,
                    RPAD(COALESCE(BENE_1ST_NAME,''),15,' ') AS BENE_1ST_NAME,
                    RPAD(COALESCE(BENE_MIDL_NAME,''),1,' ') AS BENE_MIDL_NAME
                FROM CTE_PIVOT CTE
                INNER JOIN CTE_CNT CTE_CNT
                 ON CTE.BENE_XREF_EFCTV_SK = CTE_CNT.BENE_XREF_EFCTV_SK
                INNER JOIN IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_BENE  BENE
                 ON CTE.BENE_XREF_EFCTV_SK = BENE.BENE_XREF_EFCTV_SK
                INNER JOIN 
                    (SELECT BENE_XREF_EFCTV_SK,
                            BENE_SSN_NUM
                     FROM IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE_ID_TRKNG
                     WHERE BENE_SSN_NUM IN (SELECT SSN_NUM FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.MEDPAR_BAYSTATE_SSN)
                     GROUP BY 1,2
                    ) TRKNG
                 ON CTE.BENE_XREF_EFCTV_SK = TRKNG.BENE_XREF_EFCTV_SK
                WHERE BENE.IDR_TRANS_OBSLT_TS = '9999-12-31'
                  AND BENE.IDR_LTST_TRANS_FLG = 'Y'     
            ) FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER=NONE ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
              SINGLE = TRUE max_file_size=5368709120 """, con, exit_on_error=True)
   

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