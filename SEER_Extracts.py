#!/usr/bin/env python
########################################################################################################
# Name:  SEER_Extracts.py
#
# Desc: Seer extract for IMS and registries python script.
#
# Created: Paul Baranoski  09/17/2024  
# Modified: 
#
# Paul Baranoski 2024-09-17 Created script. 
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
script_name = os.path.basename(__file__)

import snowconvert_helpers
from snowconvert_helpers import Export

########################################################################################################
# VARIABLE ASSIGNMENT
########################################################################################################
con = None 
now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')
FF_ID_NODE=os.getenv('FF_ID_NODE')
REGISTRY_NAME=os.getenv('REGISTRY_NAME')

# boolean - Python Exception status
bPythonExceptionOccurred=False

########################################################################################################
# RUN
########################################################################################################
print('')
print("Run date and time: " + date_time  )
print('')

########################################################################################################
# Method to execute the extract SQL using Timestamp 
########################################################################################################
try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()   
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

   snowconvert_helpers.execute_sql_statement("""USE DATABASE IDRC_${ENVNAME}""", con,exit_on_error = True)

   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_SEER_STG/SEER_EXT_{FF_ID_NODE}_{TMSTMP}.txt.gz
                                                FROM (
   
            WITH CLM_INFO  AS (

                SELECT B.BENE_SK, MAX(C.CLM_FROM_DT) AS LAST_CONTACT_DT, MAX(C.CLM_PD_DT) AS LAST_PROCESS_DT 
                
                FROM IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE B
                
                INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.SEER_FF FINDER
                ON FINDER.BENE_SSN_NUM = B.BENE_SSN_NUM
                
                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM  C
                ON  B.BENE_SK           =  C.BENE_SK
                AND C.CLM_FINL_ACTN_IND = 'Y'

                WHERE TO_CHAR(B.IDR_TRANS_OBSLT_TS,'YYYY-MM-DD') = '9999-12-31'
                  AND B.IDR_LTST_TRANS_FLG = 'Y'

                GROUP BY B.BENE_SK  

            )

                SELECT 
                         RPAD(COALESCE(B.BENE_SSN_NUM,' '),9,' ')              AS BENE_SSN_NUM
                        ,RPAD(COALESCE(B.BENE_LAST_NAME,'UNKNOWN '),30,' ')    AS BENE_LAST_NAME
                        ,RPAD(COALESCE(B.BENE_1ST_NAME,' '),20,' ')            AS BENE_1ST_NAME
                        ,SUBSTR(COALESCE(B.BENE_MIDL_NAME,' '),1,1)            AS BENE_MIDL_INIT
                        
                        ,RPAD(COALESCE(TO_CHAR(B.BENE_BRTH_DT,'YYYYMMDD'),' '),8,' ')  AS BENE_BRTH_DT
                        ,RPAD(COALESCE(TO_CHAR(B.BENE_DEATH_DT,'YYYYMMDD'),' '),8,' ') AS BENE_DEATH_DT	

                        --,DATEDIFF(Year,B.BENE_BRTH_DT,B.BENE_DEATH_DT ) AS AGE
                        
                        --,ZIP5.GEO_ZIP5_CD 
                        --,B.GEO_ZIP4_CD
                        ,RPAD(REPLACE(COALESCE(B.GEO_ZIP5_CD,' '),'~',' '),5,' ') || RPAD(REPLACE(COALESCE(B.GEO_ZIP4_CD,' '),'~',' '),4,' ') AS ZIP9

                        ,B.GEO_USPS_STATE_CD AS BENE_USPS_ST_CD
                        --,ZIP5.GEO_FIPS_STATE_CD

                        ,ZIP5.GEO_FIPS_CNTY_CD

                        ,COALESCE(B.BENE_SEX_CD,' ')   AS BENE_SEX_CD
                        ,COALESCE(B.BENE_RACE_CD,' ')  AS BENE_RACE_CD
                        
                        ,CASE WHEN B.BENE_DEATH_DT IS NULL
                              THEN 'A'
                              WHEN DATEDIFF(Year,B.BENE_BRTH_DT,B.BENE_DEATH_DT ) > 120
                              THEN 'U'
                              WHEN B.BENE_DEATH_DT IS NOT NULL
                              THEN 'D'
                          END AS VITAL_STATUS    

                       -- ,BENE_DOD_PROOF_CD
                       -- ,BENE_VRFY_DEATH_DAY_SW

                       ,RPAD(COALESCE(TO_CHAR(C.LAST_CONTACT_DT,'YYYYMMDD'),' '),8,' ')  AS LAST_CONTACT_DT
                       ,RPAD(COALESCE(TO_CHAR(C.LAST_PROCESS_DT,'YYYYMMDD'),' '),8,' ')  AS LAST_PROCESS_DT

                       ,RPAD(COALESCE(B.BENE_MBI_ID,' '),11,' ')  AS BENE_MBI_ID
                       ,RPAD(COALESCE(B.BENE_BIC_CD,' '),2,' ')   AS BENE_BIC_CD

                      ,FINDER.REGISTRY_ID
                      ,FINDER.PATIENT_ID
                  

                FROM IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE B

                INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.SEER_FF FINDER
                ON FINDER.BENE_SSN_NUM = B.BENE_SSN_NUM
                
                INNER JOIN CLM_INFO C
                ON C.BENE_SK = B.BENE_SK
                
                INNER JOIN  IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP5_CD ZIP5
                ON ZIP5.GEO_SK   = B.GEO_SK
                
                WHERE TO_CHAR(B.IDR_TRANS_OBSLT_TS,'YYYY-MM-DD') = '9999-12-31'
                  AND B.IDR_LTST_TRANS_FLG = 'Y'

                      
  ) 
                        FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY=NONE )
                        SINGLE=TRUE max_file_size=5368709120  """, con, exit_on_error=True)

   
   #**************************************
   # End Application
   #**************************************    
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


   


