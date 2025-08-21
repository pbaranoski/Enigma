#!/usr/bin/env python
########################################################################################################
# Name:  VARTN_Extract.py
# DESC:  This python program data from IDRC for the VA Return File extract
#
# Author:  Joshua Turner
# Created: 01/19/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  ----------------------------------------------------------------------
# Joshua Turner 	2023-01-19   New script.
# Joshua Turner         2023-11-08   Changed output file from CSV to TXT
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

CLNDR_YR = os.getenv('YEAR')
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
    # Extract VA Return File data and write to S3 as a flat file
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_VARTRN_STG/VARETURN_Y{CLNDR_YR}_FILE_{TMSTMP}.txt.gz
            FROM (
                SELECT
                    RPAD(COALESCE(TO_CHAR(BENE.BENE_BRTH_DT, 'YYYYMMDD'),''),8,' ') AS BENE_BRTH_DT,
                    RPAD(COALESCE(BENE.BENE_CAN_NUM, ''),9,' ') AS BENE_CAN_NUM,
                    RPAD(COALESCE(BENE.BENE_BIC_CD, ''),2,' ') AS BIC_CD,
                    RPAD(COALESCE(TRANS.BENE_SSN_NUM, ''),9,' ') AS BENE_SSN_NUM,
                    COALESCE(BENE.BENE_SEX_CD, ' ') AS  GNDR_CD,
                    COALESCE(TRANS.BENE_PTD_ASGN_TYPE_CD, ' ') AS ASGN_TYPE_CD,
                    COALESCE(TRANS.BENE_LIS_IND_CD, ' ') AS LIS_IND_ID,
                    COALESCE(TRANS.BENE_DEEMD_IND_CD, ' ') AS DEEMD_IND_ID,
                    COALESCE(TRANS.BENE_MDCD_STUS_CD, ' ') AS MDCD_STUS_ID,
                    COALESCE(TRANS.BENE_RTRMT_RX_BNFT_CD, ' ') AS RTR_DRUG_BNFT_SW,
                    RPAD(COALESCE(TRANS.CNTRCT_PTD_NUM, ''),5,' ') AS PTD_CNTRCT_NUM,
                    RPAD(COALESCE(TRANS.CNTRCT_PBP_PTD_NUM, ''),3,' ') AS PTD_PBP_NUM,
                    RPAD(COALESCE(TO_CHAR(TRANS.BENE_PTD_STRT_DT, 'YYYYMMDD'),''),8,' ') AS PTD_STRT_DT,
                    COALESCE(TRANS.BENE_PTA_STUS_CD, ' ') AS BENE_PTA_STUS_CD,
                    COALESCE(TRANS.BENE_PTB_STUS_CD, ' ') AS BENE_PTB_STUS_CD,
                    COALESCE(CASE WHEN TRANS.CNTRCT_PTC_NUM = 'UNK' 
                                  THEN 'N' 
                                  ELSE 'Y' END, ' ') AS BENE_PTC_STUS_CD,
                    COALESCE(TRANS.BENE_PTD_STUS_CD, ' ') AS BENE_PTD_STUS_CD,
                    RPAD(COALESCE(TRANS.BENE_DUAL_STUS_CD, ''),2,' ') AS DUAL_STUS_CD,
                    RPAD(COALESCE(BENE.BENE_1ST_NAME, ''),30,' ') AS BENE_1ST_NAME,
                    RPAD(COALESCE(BENE.BENE_MIDL_NAME, ''),15,' ') AS MDL_NAME,
                    RPAD(COALESCE(BENE.BENE_LAST_NAME, ''),40,' ') AS BENE_LAST_NAME,
                    RPAD(COALESCE(BENE.BENE_LINE_1_ADR, ''),45,' ') AS DRVD_LINE_1_ADR,
                    RPAD(COALESCE(BENE.BENE_LINE_2_ADR, ''),45,' ') AS DRVD_LINE_2_ADR,
                    RPAD(COALESCE(BENE.BENE_LINE_3_ADR, ''),45,' ') AS DRVD_LINE_3_ADR,
                    RPAD(COALESCE(ZIP5.GEO_ZIP_PLC_NAME, ''),100,' ') AS DRVD_ADR_CITY_NAME,
                    RPAD(COALESCE(FIPS.GEO_USPS_STATE_CD,''),2,' ') AS DRVD_PSTL_STATE_CD,
                    RPAD(COALESCE(ZIP5.GEO_ZIP5_CD, ''),5,' ') || RPAD(COALESCE(BENE.GEO_ZIP4_CD,''),4,' ') AS DRVD_ADR_ZIP_CD,
                    RPAD(COALESCE(BENE.BENE_MBI_ID, ''),11,' ') AS BENE_MBI_ID
                FROM "BIA_{ENVNAME}"."CMS_DIM_BEPSD_{ENVNAME}"."BENE_CRDTBL_CVRG" FNDR
                 
                INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE" BENE
                    ON FNDR.BENE_LINK_KEY = BENE.BENE_LINK_KEY
                    
                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_BENE_MTRLZD_{ENVNAME}"."BENE_FCT_TRANS" TRANS
                    ON BENE.BENE_LINK_KEY = TRANS.BENE_SK
                    
                /*INNER JOIN  CMS_VIEW_CLNDR_PRD.V1_CLNDR_DT B
                 ON TRANS.CLNDR_DT = B.CLNDR_DT
                 AND TRANS.CLNDR_CY_MO_NUM = B.CLNDR_CY_MO_NUM */
                 
                INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_ZIP5_CD" ZIP5
                    ON BENE.GEO_SK = ZIP5.GEO_SK
                    
                INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_FIPS_STATE_CD" FIPS 
                    ON FIPS.GEO_FIPS_STATE_CD = ZIP5.GEO_FIPS_STATE_CD
                    
                WHERE
                /*ENTER THE YEAR, MONTH AND DAY SHOULD BE ALWAYS JANUARY 01*/
                '{CLNDR_YR}-01-01'BETWEEN TRANS.BENE_FCT_EFCTV_DT AND TRANS.BENE_FCT_OBSLT_DT
                AND TO_DATE(TRANS.IDR_TRANS_OBSLT_TS) ='9999-12-31'
                AND FNDR.BENE_NATL_VA_SW='1' AND
                /*ENTER THE YEAR FROM CREDITABLE COVERAGE TABLE'S LATEST YEAR'*/
                FNDR.CLNDR_CY_NUM='{CLNDR_YR}'
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
