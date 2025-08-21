#!/usr/bin/env python
########################################################################################################
# Name:   STS_SNF_RPT.py
#
# DESC:   This script extracts data for STS HHA table report - HHA Rev Cntr UNITS/CHRGS	  
#         by period expense (legacy AA5 report)
#
# Created: Viren Khanna 8/27/2024
# Modified: 
#
# Viren Khanna 2024-08-27 Created programs.
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
TMSTMP=os.getenv('TMSTMP')

RUN_PRD=os.getenv('RUN_PRD')
EXT_TO_DATE=os.getenv('EXT_TO_DATE')
EXT_TO_YYYY=EXT_TO_DATE[:4]
EXT_FROM_YYYY=os.getenv('EXT_FROM_YYYY')



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

   ## INSERT DATA INTO UTIL_EXT_RUNS TABLE ##
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STS_SNF_STG/STS_SNF_RPT_AA7_{EXT_TO_YYYY}_{RUN_PRD}_{TMSTMP}.csv.gz
                    FROM (

                        WITH SNF_CLM AS (
    SELECT C.GEO_BENE_SK
          ,C.CLM_DT_SGNTR_SK
          ,C.CLM_TYPE_CD
          ,C.CLM_NUM_SK
    

      
                             ,TO_CHAR(C.CLM_THRU_DT,'YYYY') AS CAL_YEAR
        
                             --********************************************* 
                             -- Count claim lines as a BILL
                             -- 0 = Credit Adj; C = Credit ?
                             --*********************************************
                             ,CASE WHEN C.CLM_QUERY_CD = '0' THEN -1 ELSE 1 END AS NOF_BILLS 

          --*********************************************************** 
          -- If Cancel claim --> set amt to negative (back-out) ELSE use amt
          --***********************************************************
                             
                  ,CASE WHEN C.CLM_QUERY_CD = '0'  
                                             THEN CI.CLM_INSTNL_CVRD_DAY_CNT * -1
                                             ELSE CI.CLM_INSTNL_CVRD_DAY_CNT          
                             END AS  COVERED_DAYS   
                                  

          --*********************************************************** 
          -- If Cancel claim --> set amt to negative (back-out) ELSE use amt
          --***********************************************************
                             
                  ,CASE WHEN C.CLM_QUERY_CD = '0'  
                                             THEN (C.CLM_SBMT_CHRG_AMT - C.CLM_NCVRD_CHRG_AMT) * -1
                                             ELSE (C.CLM_SBMT_CHRG_AMT - C.CLM_NCVRD_CHRG_AMT)   
                             END AS  TOTAL_CHARGES       
          --*********************************************************** 
          -- If Cancel claim --> set amt to negative (back-out) ELSE use amt
          --***********************************************************
                             ,C.CLM_QUERY_CD
                             ,CASE WHEN C.CLM_QUERY_CD = '0'  
                                             THEN C.CLM_PMT_AMT * -1
                                             ELSE C.CLM_PMT_AMT            
                             END AS  REIMBURSEMENT_AMT


        ,C.CLM_BLG_PRVDR_OSCAR_NUM
                             ,C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD as BILL_TYPE
                             
    
    FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C
    
    
   /* INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
    ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK*/

    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN  CDN
    ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
    AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
    AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
    AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK

    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_INSTNL CI
    ON  C.GEO_BENE_SK     = CI.GEO_BENE_SK
    AND C.CLM_DT_SGNTR_SK = CI.CLM_DT_SGNTR_SK
    AND C.CLM_TYPE_CD     = CI.CLM_TYPE_CD
    AND C.CLM_NUM_SK      = CI.CLM_NUM_SK
              
    WHERE C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_FROM_YYYY}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DATE}','YYYY-MM-DD')
    AND CDN.CLM_CWF_BENE_MDCR_STUS_CD IN ('10','11','20','21','31','40')
AND C.CLM_TYPE_CD BETWEEN 20 AND 30
   AND C.CLM_QUERY_CD <> 'C'
   )
    
,RPT_DATA_SNF_SWING_BEDS  AS  (     
SELECT 
                              
                              
                 'SNF SWING'           AS SORT_ORD_IND  
             , CAL_YEAR
          ,SUM(NOF_BILLS )        AS NUMBER_OF_BILLS
         ,ROUND(SUM(COVERED_DAYS ),0)    AS DAYS_OF_CARE
                             ,ROUND(SUM(TOTAL_CHARGES),0)           AS TOT_AMT
        ,CASE WHEN SUM(COVERED_DAYS) = 0
                                   THEN 0
                                             ELSE round(SUM(TOTAL_CHARGES) / SUM(COVERED_DAYS),0)
                             END  AS AVERAGE_PER_DAY 
                             ,ROUND(SUM(REIMBURSEMENT_AMT ),0)      AS AMOUNT_REIMBURSHED
                             
        ,CASE WHEN SUM(TOTAL_CHARGES) = 0
                                   THEN 0
                                             ELSE SUM(REIMBURSEMENT_AMT) / SUM(TOTAL_CHARGES)*100
                             END  AS PERCENTAGE_OF_COVERED_CHARGES 


                             
              FROM SNF_CLM 
   
WHERE CLM_TYPE_CD IN  ('30')
              
              GROUP BY SORT_ORD_IND, CAL_YEAR 
    )
    
,RPT_DATA_TOTAL_SWING  AS  (     
SELECT 
                                 'SNF TOTAL'           AS SORT_ORD_IND 
                              ,CAL_YEAR
         ,SUM(NOF_BILLS )        AS NUMBER_OF_BILLS
         ,ROUND(SUM(COVERED_DAYS ),0)    AS DAYS_OF_CARE
                             ,ROUND(SUM(TOTAL_CHARGES),000)           AS TOT_AMT
        ,CASE WHEN SUM(COVERED_DAYS) = 0
                                   THEN 0
                                             ELSE round(SUM(TOTAL_CHARGES) / SUM(COVERED_DAYS),0)
                             END  AS AVERAGE_PER_DAY 
                             ,ROUND(SUM(REIMBURSEMENT_AMT ),000)      AS AMOUNT_REIMBURSHED
                             
        ,CASE WHEN SUM(TOTAL_CHARGES) = 0
                                   THEN 0
                                             ELSE SUM(REIMBURSEMENT_AMT) / SUM(TOTAL_CHARGES)*100
                             END  AS PERCENTAGE_OF_COVERED_CHARGES 

                             
              FROM SNF_CLM
    
              
              GROUP BY SORT_ORD_IND,CAL_YEAR
              
)
SELECT * FROM (

              SELECT *
              FROM RPT_DATA_TOTAL_SWING

              UNION 

              SELECT *
              FROM RPT_DATA_SNF_SWING_BEDS
    )
    ORDER BY SORT_ORD_IND DESC, CAL_YEAR    
 )
            FILE_FORMAT = (TYPE = CSV field_delimiter=','  ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY = none )
            SINGLE=TRUE  HEADER=TRUE  max_file_size=5368709120  """, con, exit_on_error=True)

    
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