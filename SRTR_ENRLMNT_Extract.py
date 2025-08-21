#!/usr/bin/env python
########################################################################################################
# Name:  SRTR_ENRLMNT_Extract.py
# DESC:  This python script extracts data from IDRC for the SRTR Enrollment extract
#
# Modified: 
# 02/27/2023 -- Sumathi Gayam -- Created the initial version
# 06/06/23   -- Sumathi Gayam -- Updated the file name to match with the EFT naming standards
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

EXT_YEAR=os.getenv('EXT_YEAR')
# Get the last two digits of the year for the extract filename
YY = EXT_YEAR[2:]
ENVNAME = os.getenv('ENVNAME')
TMSTMP = os.getenv('TMSTMP')
#SRTR_ENRLMNT_BUCKET = os.getenv('SRTR_ENRLMNT_BUCKET')
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
    # Extract SRTR PDE data and write to S3 as a flat file
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_SRTRENRLMNT_STG/SRTR_ENRLMNT_Y{YY}_{TMSTMP}.txt.gz
            FROM (
              
        SELECT DISTINCT
        'START' AS ST_OF_FILE
        ,RPAD(COALESCE(BENE_CAN_NUM,' '),9,' ') AS BENE_CAN_NUM
        ,RPAD(COALESCE(BENE_BIC_CD,' '),2,' ') AS BENE_BIC_CD
        ,RPAD(COALESCE(SRTR.SSN,' '),9,' ') AS SRTR_SSN_NUM
        ,RPAD(COALESCE(TO_CHAR(BENE_BIRTH_DT, 'YYYYMMDD'),''),8,' ') AS BENE_BIRTH_DT
        ,TO_CHAR(BENE_AGE_CNT,'FM000') AS BENE_AGE_CNT
        ,RPAD(COALESCE(TO_CHAR(F12.BENE_DEATH_DT, 'YYYYMMDD'),''),8,' ') AS BENE_DEATH_DT
        ,RPAD(COALESCE(BENE_MA_ELGBL_CD,' '),1,' ') AS BENE_MA_ELGBL_CD
        ,RPAD(COALESCE(F12.BENE_MBI_ID,' '),11,' ') AS BENE_MBI_ID
        ,RPAD(COALESCE(BENE_MDCR_STUS_CD,' '),2,' ') AS BENE_MDCR_STUS_CD
        ,RPAD(COALESCE(BENE_PTA_STUS_CD,' '),1,' ') AS BENE_PTA_STUS_CD
        ,RPAD(COALESCE(BENE_PTB_STUS_CD,' '),1,' ') AS BENE_PTB_STUS_CD
        ,RPAD(COALESCE(TO_CHAR(F12.BENE_PTAPTB_STRT_DT, 'YYYYMMDD'),''),8,' ') AS BENE_PTAPTB_STRT_DT
        ,RPAD(COALESCE(TO_CHAR(F12.BENE_PTAPTB_END_DT, 'YYYYMMDD'),''),8,' ') AS BENE_PTAPTB_END_DT
        ,RPAD(COALESCE(BENE_PTD_STUS_CD,' '),1,' ') AS BENE_PTD_STUS_CD
        ,RPAD(COALESCE(TO_CHAR(F12.BENE_PTD_STRT_DT, 'YYYYMMDD'),''),8,' ') AS BENE_PTD_STRT_DT
        ,RPAD(COALESCE(TO_CHAR(F12.BENE_PTD_END_DT, 'YYYYMMDD'),''),8,' ') AS BENE_PTD_END_DT
        ,RPAD(COALESCE(BENE_PTD_PYMT_SW,' '),1,' ') AS BENE_PTD_PYMT_SW
        ,RPAD(COALESCE(CNTRCT_PTD_NUM,' '),5,' ') AS CNTRCT_PTD_NUM
        ,RPAD(COALESCE(BENE_RACE_CD,' '),2,' ') AS BENE_RACE_CD
        ,RPAD(COALESCE(BENE_RTI_RACE_CD,' '),1,' ') AS BENE_RTI_RACE_CD
        ,RPAD(COALESCE(BENE_SEX_CD,' '),1,' ') AS BENE_SEX_CD
        ,RPAD(REPLACE(COALESCE(CNTRCT_PBP_PTC_NUM,' '),'~',' '),3,' ') AS CNTRCT_PBP_PTC_NUM
        ,RPAD(REPLACE(COALESCE(CNTRCT_PBP_PTD_NUM,' '),'~',' '),3,' ') AS CNTRCT_PBP_PTD_NUM
        ,RPAD(REPLACE(COALESCE(CNTRCT_PTAPTB_NUM,' '),'~',' '),5,' ') AS CNTRCT_PTAPTB_NUM
        ,RPAD(REPLACE(COALESCE(CNTRCT_PBP_PTAPTB_NUM,' '),'~',' '),3,' ') AS CNTRCT_PBP_PTAPTB_NUM
        ,RPAD(REPLACE(COALESCE(CNTRCT_PTC_NUM,' '),'~',' '),5,' ') AS CNTRCT_PTC_NUM
        ,RPAD(REPLACE(F12.GEO_SK,'-1',TO_CHAR(F12.GEO_SK,'FM0000')),5,TO_CHAR(F12.GEO_SK,'FM00000'))  AS GEO_SK    
        ,RPAD(REPLACE(COALESCE(GEO_ZIP4_CD,' '),'~',' '),4,' ') AS GEO_ZIP4_CD
        ,RPAD(COALESCE(BENE_CVRG_TYPE_CD,' '),2,' ') AS BENE_CVRG_TYPE_CD
        ,RPAD(COALESCE(BENE_ENRLMT_TYPE_CD,' '),1,' ') AS BENE_ENRLMT_TYPE_CD
        ,'END' AS END_OF_FILE

        FROM

        IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_BENE_FCT_TRANS_CRNT F12
        INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.SRTR_SSN SRTR
           ON F12.BENE_SSN_NUM = LTRIM(RTRIM(SRTR.SSN))

        WHERE
        (TO_DATE('{EXT_YEAR}-01-01','YYYY-MM-DD') Between BENE_FCT_EFCTV_DT and BENE_FCT_OBSLT_DT
        OR BENE_DEATH_DT BETWEEN TO_DATE('{EXT_YEAR}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_YEAR}-12-31','YYYY-MM-DD'))

         ) FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER='|' ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
           SINGLE=TRUE max_file_size=5368709120 """, con, exit_on_error=True)
				  
    snowconvert_helpers.quit_application()
           
   #**************************************
   # End Application
   #**************************************    
   
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
