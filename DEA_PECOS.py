#!/usr/bin/env python
########################################################################################################
# Name:   DEA_PECOS.py
# DESC:   This script creates pecos EXTRACT file
# Created: Sumathi Gayam  
# Modified: 06/14/2022 - SG
#
# Paul Baranoski 2022-07-11 Add Pipe delimiters after each field for Detail records. Delimiters are
#                           not needed for header and trailer, AND delimiters for COPYINTO must be for end
#                           results set which consists of a single column. 
#                           Removed bogus WHERE Clause (not present in production code).
#                           Added extract filename to email.
# Paul Baranoski 2023-07-26 Modify extract file extension from .csv to .txt 
# Paul Baranoski 2025-06-05 Re-work query for clarity. Inspired by Karen Brown.
########################################################################################################
# IMPORTS
########################################################################################################
import os
import sys
import datetime
from datetime import datetime
import sendEmail

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
#var1 = sys.argv[1]

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')


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

def Extract_SQL(ENVNAME,TMSTMP):

 
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PECOS_STG/DEA_PECOS_{TMSTMP}.txt.gz	
  
			FROM (


!!!!! THIS SQL IS NOT READY TO PROMOTE -- DO NOT USE AT THIS TIME!!!!!!!

                    WITH PRVDR_HCIDEA_NPI_INFO AS  (

                     	   -- Get Most recent record
                          SELECT PRVDR_HCIDEA_HC_ID, PRVDR_HCIDEA_NPI_NUM 
                            FROM IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR_HCIDEA_NPI HCINPI
						   WHERE TO_CHAR(IDR_TRANS_OBSLT_TS, 'YYYY-MM-DD') = '9999-12-31'       

                    )
                    
                    ,PRVDR_HCIDEA_STATE_LCNS_INFO AS  (

                           -- Get Most recent record                    
                           -- This table has 3-part unique key 1) HCILCNS.PRVDR_HCIDEA_HC_ID 2) HCILCNS.PRVDR_HCIDEA_LCNS_STATE_CD, 3) PRVDR_HCIDEA_LCNS_NUM (not mentioned or used 
                           -- HCILCNS.PRVDR_HCIDEA_HC_ID = 'HDL065GR38' and HCILCNS.PRVDR_HCIDEA_LCNS_STATE_CD = 'CA'
                          SELECT HCILCNS.PRVDR_HCIDEA_HC_ID, HCILCNS.PRVDR_HCIDEA_LCNS_STATE_CD, MAX(substr(HCILCNS.PRVDR_HCIDEA_EFCTV_PRD, 39, 10) )  AS PRVDR_HCIDEA_EFCTV_PRD
                            FROM IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR_HCIDEA_STATE_LCNS HCILCNS
						   WHERE TO_CHAR(IDR_TRANS_OBSLT_TS, 'YYYY-MM-DD') = '9999-12-31' 
                                                   
                    )                    

                   ,PECOS_DTL_INFO as (
                      
                        SELECT
                            RPAD(HCIDEA.PRVDR_HCIDEA_DEA_NUM,20,' ')                        AS DEA_NUM

                            ,RPAD(COALESCE(HCINPI.PRVDR_HCIDEA_NPI_NUM,' '),10,' ')         AS DEA_NPI
                     
                            ,CASE WHEN HCILCNS.PRVDR_HCIDEA_LCNS_ISS_DT IS NULL
                                  THEN Repeat(' ',8)
                                  ELSE TO_CHAR(HCILCNS.PRVDR_HCIDEA_LCNS_ISS_DT::DATE, 'YYYYMMDD')
                             END  AS LCNS_ISS_DT
                     
                            ,CASE WHEN HCILCNS.PRVDR_HCIDEA_LCNS_EXPRTN_DT IS NULL
                                  THEN repeat(' ',8)
                                  ELSE TO_CHAR(HCILCNS.PRVDR_HCIDEA_LCNS_EXPRTN_DT::DATE, 'YYYYMMDD') 
                             END  AS LCNS_EXPRTN_DT
                     
                            ,RPAD(COALESCE(HCILCNS.PRVDR_HCIDEA_LCNS_STATE_CD,' '),2,' ')   AS STATE_CD
                            ,RPAD(HCIDEA.PRVDR_HCIDEA_HC_ID,10,' ')                         AS DEA_HC_ID
                            ,RPAD(HCIDEA.PRVDR_HCIDEA_DEA_STUS_CD,1,' ')                    AS DEA_STUS_CD
                            ,RPAD(to_timestamp(substr(HCIDEA.PRVDR_HCIDEA_EFCTV_PRD, 3, 19),'YYYY-MM-DD HH:MI:SS'),19,' ')  AS TRANS_STRT_DT
                            ,RPAD(to_timestamp(substr(HCIDEA.PRVDR_HCIDEA_EFCTV_PRD, 39, 19),'YYYY-MM-DD HH:MI:SS'),19,' ') AS TRANS_END_DT

                        FROM IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR_HCIDEA_DEA HCIDEA

                        LEFT OUTER JOIN PRVDR_HCIDEA_NPI_INFO HCINPI
                        ON HCIDEA.PRVDR_HCIDEA_HC_ID = HCINPI.PRVDR_HCIDEA_HC_ID

                        LEFT OUTER JOIN IPRVDR_HCIDEA_STATE_LCNS HCILCNS
                        ON HCIDEA.PRVDR_HCIDEA_HC_ID = HCILCNS.PRVDR_HCIDEA_HC_ID
                        
                        ORDER BY DEA_NUM, LCNS_ISS_DT, STATE_CD, TRANS_STRT_DT
                      
                    )

                    ,PECOS_DTL_SINGLE_COL as (

                          -- If any column is null, the entire string will be NULL --> must fix main query	
                          SELECT '1' as SEQ_NUM
                                ,DEA_NUM           || '|'  
                                || DEA_NPI         || '|' 
                                || LCNS_ISS_DT     || '|' 
                                ||LCNS_EXPRTN_DT   || '|'
                                || STATE_CD        || '|'
                                || DEA_HC_ID       || '|' 
                                || DEA_STUS_CD     || '|'
                                || TRANS_STRT_DT   || '|'
                                || TRANS_END_DT    || '|' 
                              AS DTL_ROW
                            FROM PECOS_DTL_INFO

                    )

                    ,HEADER_ROW as (

                        SELECT '0' as SEQ_NUM 
                              ,'H'|| to_char(CURRENT_DATE,'YYYYMMDD') || repeat(' ',97) as DTL_ROW
                          FROM DUAL

                    )

                    ,NOF_DTL_ROWS as (
                        
                        SELECT COUNT(*) as TOT_RECS
                        FROM PECOS_DTL_INFO
                    )

                    ,TRAILER_ROW as (

                          SELECT '2' as SEQ_NUM 
                                ,'T' || to_char(TOT_RECS,'FM0000000000') || repeat(' ',95)
                                as DTL_ROW
                          FROM NOF_DTL_ROWS

                    )

                    SELECT DTL_ROW
                    FROM (
                        SELECT *
                        FROM HEADER_ROW
                        UNION ALL
                      
                        SELECT *
                        FROM PECOS_DTL_SINGLE_COL
                        UNION ALL
                      
                        SELECT *
                        FROM TRAILER_ROW

                    )  
                    ORDER BY SEQ_NUM 

									
			)
            
FILE_FORMAT = (TYPE = CSV field_delimiter = none FIELD_OPTIONALLY_ENCLOSED_BY = none )
                      SINGLE = TRUE  OVERWRITE = TRUE  max_file_size=5368709120 """, con, exit_on_error=True)



try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()   
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)
   
   #**************************************
   #   Extract PECOS data  
   #**************************************   
   print("before executeSQL")
   Extract_SQL(ENVNAME,TMSTMP)
    
   
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
