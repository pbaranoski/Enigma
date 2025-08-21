#!/usr/bin/env python
########################################################################################################
# Name:  OPMHI_ENRLMNT_Extract.py
# DESC:  This python script executes the enrollment extract for OPM-HI
#
# Created: Joshua Turner
# Modified: 06/13/2023
#
# 10/02/2023   Paul Baranoski       Modified extract filename to use imported EXT_FILENAME.
# 10/18/2023   Paul Baranoski       Modified filter for F12.BENE_DEATH_DT to have first date be hard-coded as '2018-01-01'.
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

ENVNAME = os.getenv('ENVNAME')
TMSTMP = os.getenv('TMSTMP')
START_DATE = os.getenv('START_DATE')
END_DATE = os.getenv('END_DATE')
STAGE_NAME = os.getenv('STAGE_NAME')
EXT_FILENAME=os.getenv('EXT_FILENAME')

# boolean - Python Exception status
bPythonExceptionOccurred=False

try:
    snowconvert_helpers.configure_log()
    con = snowconvert_helpers.log_on()
    snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
    snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

    ########################################################################################################
    # Extract OPM-HI ENROLLMENT data and write to S3
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_{STAGE_NAME}_STG/{EXT_FILENAME}
            FROM (
            SELECT DISTINCT                                                         
                'START' AS ST_OF_FILE,
                RPAD(COALESCE(F12.BENE_CAN_NUM, B.BENE_CAN_NUM),9,' ') AS BENE_CAN_NUM,
                RPAD(COALESCE(F12.BENE_BIC_CD, B.BENE_BIC_CD),2,' ') AS BENE_BIC_CD,
                RPAD(FNDR.SSN_NUM,9,' ') AS OPM_SSN_NUM,
                RPAD(COALESCE(TO_CHAR(F12.BENE_BIRTH_DT, 'YYYYMMDD'),TO_CHAR(B.BENE_BRTH_DT,'YYYYMMDD')),8,' ') AS BENE_BIRTH_DT,
                LPAD(BENE_AGE_CNT,3,'0') AS BENE_AGE_CNT,
                RPAD(COALESCE(COALESCE(TO_CHAR(F12.BENE_DEATH_DT, 'YYYYMMDD'),TO_CHAR(B.BENE_DEATH_DT,'YYYYMMDD')),''),8,' ') AS BENE_DEATH_DT,
                RPAD(COALESCE(BENE_MA_ELGBL_CD,''),1,' ') AS BENE_MA_ELGBL_CD,
                RPAD(COALESCE(F12.BENE_MBI_ID, B.BENE_MBI_ID),11,' ') AS BENE_MBI_ID,
                RPAD(COALESCE(BENE_MDCR_STUS_CD,''),2,' ') AS BENE_MDCR_STUS_CD,
                RPAD(COALESCE(BENE_PTA_STUS_CD,''),1,' ') AS BENE_PTA_STUS_CD,
                RPAD(COALESCE(BENE_PTB_STUS_CD,''),1,' ') AS BENE_PTB_STUS_CD,
                RPAD(COALESCE(TO_CHAR(BENE_PTAPTB_STRT_DT, 'YYYYMMDD'),''),8,' ') AS BENE_PTAPTB_STRT_DT,
                RPAD(COALESCE(TO_CHAR(BENE_PTAPTB_END_DT , 'YYYYMMDD'),''),8,' ') AS BENE_PTAPTB_END_DT,
                RPAD(COALESCE(BENE_PTD_STUS_CD,''),1,' ') AS BENE_PTD_STUS_CD,
                RPAD(COALESCE(TO_CHAR(BENE_PTD_STRT_DT, 'YYYYMMDD'),''),8,' ') AS BENE_PTD_STRT_DT,
                RPAD(COALESCE(TO_CHAR(BENE_PTD_END_DT , 'YYYYMMDD'),''),8,' ') AS BENE_PTD_END_DT,
                RPAD(COALESCE(BENE_PTD_PYMT_SW,''),1,' ') AS BENE_PTD_PYMT_SW,
                RPAD(COALESCE(CNTRCT_PTD_NUM,''),5,' ') AS CNTRCT_PTD_NUM,
                RPAD(COALESCE(F12.BENE_RACE_CD, B.BENE_RACE_CD),2,' ') AS BENE_RACE_CD,
                RPAD(COALESCE(BENE_RTI_RACE_CD,''),1,' ') AS BENE_RTI_RACE_CD,
                RPAD(COALESCE(F12.BENE_SEX_CD, B.BENE_SEX_CD),1,' ') AS BENE_RACE_CD,
                RPAD(COALESCE(CNTRCT_PBP_PTC_NUM,''),3,' ') AS CNTRCT_PBP_PTC_NUM,
                RPAD(COALESCE(CNTRCT_PBP_PTD_NUM,''),3,' ') AS CNTRCT_PBP_PTD_NUM,
                RPAD(COALESCE(CNTRCT_PTAPTB_NUM,''),5,' ') AS CNTRCT_PTAPTB_NUM,
                RPAD(COALESCE(CNTRCT_PBP_PTAPTB_NUM,''),3,' ') AS CNTRCT_PBP_PTAPTB_NUM,
                RPAD(COALESCE(CNTRCT_PTC_NUM,''),5,' ') AS CNTRCT_PTC_NUM,
                RPAD(COALESCE(F12.GEO_SK, B.GEO_SK),5,' ') AS GEO_SK,
                RPAD(COALESCE(F12.GEO_ZIP4_CD, B.GEO_ZIP4_CD),4,' ') AS GEO_ZIP4_CD,
                RPAD(COALESCE(BENE_CVRG_TYPE_CD,''),2,' ') AS BENE_CVRG_TYPE_CD,
                RPAD(COALESCE(BENE_ENRLMT_TYPE_CD,''),1,' ') AS BENE_ENRLMT_TYPE_CD,
                RPAD(COALESCE(TO_CHAR(BENE_FCT_EFCTV_DT , 'YYYYMMDD'),''),8,' ') AS BENE_FCT_EFCTV_DT,
                RPAD(COALESCE(TO_CHAR(BENE_FCT_OBSLT_DT , 'YYYYMMDD'),''),8,' ') AS BENE_FCT_OBSLT_DT,
                'END' AS END_OF_FILE
            FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."OPMHI_SSN" FNDR

            INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE" B
                ON FNDR.SSN_NUM = B.BENE_SSN_NUM

            LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_VDM_VIEW_MDCR_{ENVNAME}"."V2_MDCR_BENE_FCT_TRANS_CRNT" F12
                ON B.BENE_SSN_NUM = F12.BENE_SSN_NUM

            WHERE ((BENE_FCT_EFCTV_DT <= '{END_DATE}' AND BENE_FCT_OBSLT_DT >= '{START_DATE}')
               OR F12.BENE_DEATH_DT BETWEEN '2018-01-01' AND '{END_DATE}')
            ) FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER='|' ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
              max_file_size=5368709120 """, con, exit_on_error=True)
                      
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


    
