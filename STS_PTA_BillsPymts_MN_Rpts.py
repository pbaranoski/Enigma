#!/usr/bin/env python
########################################################################################################
# Name:   STS_HHA_RevCtr_MN_Rpts.py
#
# DESC:   This script extracts data for STS PTA Bills Payments by Type of Service report 
#         (legacy A-1 report) for MN
#
# Created: Paul Baranoski 12/31/2024
# Modified: 
#
# Paul Baranoski 2024-12-31 Created program.
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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STS_PTA_BPYMTS_MN_STG/STS_PTA_BPYMTS_MN_RPT_A-1AB_{EXT_TO_YYYY}_{RUN_PRD}_{TMSTMP}.csv.gz
                    FROM (

                WITH RPT_CLM_TYPE_CATEGORIES AS (
                    
                    SELECT '1' AS RPT_CLM_TYPE_CD, 'Inp Hosp Short Stay '     AS RPT_CLM_TYPE_DESC  FROM DUAL
                    UNION
                    SELECT '2' AS RPT_CLM_TYPE_CD, 'Inp Hosp non-Short Stay'  AS RPT_CLM_TYPE_DESC  FROM DUAL
                    UNION
                    SELECT '3' AS RPT_CLM_TYPE_CD, 'SNF'                      AS RPT_CLM_TYPE_DESC  FROM DUAL
                    UNION
                    SELECT '4' AS RPT_CLM_TYPE_CD, 'HHA'                      AS RPT_CLM_TYPE_DESC  FROM DUAL
                    UNION
                    SELECT '6' AS RPT_CLM_TYPE_CD, 'Hospice'                  AS RPT_CLM_TYPE_DESC  FROM DUAL
                        
                )

                ,RPT_STATES    AS (


                    SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME AS GEO_STATE_NAME, '1' AS ST_SORT_ORD
                    FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                    --WHERE GEO_SSA_STATE_CD BETWEEN '01' AND '53'
                    WHERE GEO_SSA_STATE_NAME = 'MINNESOTA'
                                
                            
                    
                )

                ,RPT_YEARS AS (

                    SELECT {EXT_TO_YYYY} - 8 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 7 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 6 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 5 AS CAL_YEAR FROM DUAL
                    UNION	
                    SELECT {EXT_TO_YYYY} - 4 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 3 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 2 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 1 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY}     AS CAL_YEAR FROM DUAL
                    
                )

                ,ALL_RPT_ROWS AS (

                    SELECT DISTINCT CAL_YEAR, GEO_STATE_NAME, RPT_CLM_TYPE_CD, RPT_CLM_TYPE_DESC, ST_SORT_ORD
                    FROM RPT_CLM_TYPE_CATEGORIES
                          ,RPT_YEARS 
                          ,RPT_STATES

                )


                ,PARTA_DTL_CLMS  AS (

                            SELECT C.GEO_BENE_SK
                                  ,C.CLM_DT_SGNTR_SK
                                  ,C.CLM_TYPE_CD
                                  ,C.CLM_NUM_SK

                                 ,COALESCE(C.GEO_BENE_SSA_STATE_CD,'~ ') AS BENE_SSA_STATE_CD
                                 ,COALESCE(ST.GEO_STATE_NAME,'RESIDENCE UNKNOWN') AS GEO_STATE_NAME
                                 ,TO_CHAR(C.CLM_THRU_DT,'YYYY') AS CAL_YEAR
                                 --,C.CLM_THRU_DT
                                
                                 --********************************************* 
                                 -- Count claim lines as a BILL
                                 -- 0 = Credit Adj; C = Credit ?
                                 --*********************************************
                                 ,CASE WHEN C.CLM_QUERY_CD = '0' THEN -1 ELSE 1 END AS NOF_BILLS                        
                                                        
                                  --*********************************************************** 
                                  -- If Cancel claim --> set amt to negative (back-out) ELSE use amt
                                  --***********************************************************
                                 ,C.CLM_QUERY_CD
                                 
                                 ,CASE WHEN C.CLM_QUERY_CD = '0'  
                                     THEN C.CLM_PMT_AMT * -1
                                     ELSE C.CLM_PMT_AMT            
                                 END AS  PMT_AMT

                                 ,C.CLM_SBMT_CHRG_AMT
                                 ,CLM_NCVRD_CHRG_AMT

                                 ,CASE WHEN C.CLM_QUERY_CD = '0' 
                                       THEN
                                           CASE WHEN (C.CLM_TYPE_CD IN ('20','30','60','61', '62','63','64') AND COALESCE(C.CLM_SBMT_CHRG_AMT,0) > 0)
                                                THEN ( (COALESCE(C.CLM_SBMT_CHRG_AMT,0) - COALESCE(C.CLM_NCVRD_CHRG_AMT,0)) * -1 ) 
                                                ELSE (COALESCE(C.CLM_SBMT_CHRG_AMT,0) * -1)
                                           END     
                                       ELSE 
                                           CASE WHEN (C.CLM_TYPE_CD IN ('20','30','60','61', '62','63','64') AND COALESCE(C.CLM_SBMT_CHRG_AMT,0) > 0)
                                                THEN (COALESCE(C.CLM_SBMT_CHRG_AMT,0) - COALESCE(C.CLM_NCVRD_CHRG_AMT,0) ) 
                                                ELSE  COALESCE(C.CLM_SBMT_CHRG_AMT,0)
                                           END     
                                  END AS CHRG_AMT

                                --\/******** For calculation passthru amt
                                -- ,C.CLM_ALOWD_CHRG_AMT  --> NULL
                                ,CASE WHEN C.CLM_QUERY_CD = '0'  
                                      THEN (COALESCE(CI.CLM_INSTNL_PER_DIEM_AMT,0) * -1 ) 
                                      ELSE COALESCE(CI.CLM_INSTNL_PER_DIEM_AMT,0)                 
                                      END  AS CLM_INSTNL_PER_DIEM_AMT
                       
                                 ,COALESCE(CI.CLM_INSTNL_CVRD_DAY_CNT,0) AS CLM_INSTNL_CVRD_DAY_CNT  
                                 
                                 --,CI.CLM_INSTNL_DAY_CNT  -- This is always null
                                --/\******** For calculation passthru amt
                            
                                 ,CDN.CLM_NRLN_RIC_CD

                                 ,RPAD(CI.CLM_PPS_IND_CD,1,' ') AS CI_CLM_PPS_IND_CD

                                 -- if INP, SNF 
                                 ,CASE WHEN C.CLM_TYPE_CD IN ('20','30','60','61', '62','63','64')
                                       THEN  
                                           CASE WHEN RPAD(CI.CLM_PPS_IND_CD,1,' ') = '2'
                                                THEN '1'
                                                ELSE '0'
                                           END	
                                       ELSE ' ' 
                                  END  AS RPT_PPS_CD                    

                                  ,C.CLM_BLG_PRVDR_OSCAR_NUM
                                
                                 ,CASE WHEN (regexp_like(SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1), '[0-9]+') = 'TRUE')
                                       THEN 
                                        -- WHEN 3rd position is NUMERIC
                                       CASE WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '0001' AND '0899'
                                                   THEN '1'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '0900' AND '0999'
                                                   THEN '1'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '1200' AND '1299'
                                                   THEN '2'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '1300' AND '1399'
                                                   THEN '2'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '1500' AND '1799'
                                                   THEN '6'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '1990' AND '1999'
                                                   THEN '2'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '2000' AND '2299'
                                                   THEN '2'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '3100' AND '3199'
                                                   THEN '4'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '3300' AND '3399'
                                                   THEN '2'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '4000' AND '4499'
                                                   THEN '2'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '5000' AND '6499'
                                                   THEN '3'                           
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '7000' AND '8499'
                                                   THEN '4'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '9000' AND '9799'
                                                   THEN '4'
                                               END     
                                         ELSE
                                              -- When 3rd position is NOT Numeric  
                                              CASE WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'S' 
                                                   THEN '1'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'T'
                                                   THEN '1'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'U'
                                                   THEN '3'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'V'
                                                   THEN '3'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'W'
                                                   THEN '3'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'Y'
                                                   THEN '3'
                                                   WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = 'Z'
                                                   THEN '3'
                                                   ELSE '5' 
                                              END     

                                         END AS RPT_CLM_TYPE_CD 
                                             

                            FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM  C
                            
                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR  CDS
                            ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_INSTNL CI
                            ON  C.GEO_BENE_SK     = CI.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CI.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CI.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CI.CLM_NUM_SK
                          
                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN  CDN
                            ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK

                            INNER JOIN RPT_STATES ST
                            ON COALESCE(C.GEO_BENE_SSA_STATE_CD,'~ ') = ST.GEO_SSA_STATE_CD

                            WHERE C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_FROM_YYYY}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DATE}','YYYY-MM-DD')
                            AND (   C.CLM_TYPE_CD IN ('20','30','50','60','61','62','63','64')
                                 OR          
                                   (C.CLM_TYPE_CD = '10' AND CDN.CLM_NRLN_RIC_CD = 'V')  -- HHA
                                )
                            --   V = Part A institutional claim record: (inpatient (IP), skilled nursing facility (SNF), 
                            --       christian science (CS), home health agency (HHA), or hospice)
                            
                            AND CDN.CLM_CWF_BENE_MDCR_STUS_CD IN ('10','11','20','21','31','40' )

                            AND NOT C.CLM_QUERY_CD = 'C'
                            AND RPT_CLM_TYPE_CD IN ('1','2','3','4','6') 
                            
                            AND (  COALESCE(ST.GEO_STATE_NAME,'RESIDENCE UNKNOWN') <> 'RESIDENCE UNKNOWN'
                                 or  
                                   (     COALESCE(ST.GEO_STATE_NAME,'RESIDENCE UNKNOWN') = 'RESIDENCE UNKNOWN'
                                     AND C.GEO_BENE_SK <> 0 )
                                )	 



                )

                 
                ,PARTA_SUM_DATA_BY_CLM_TYPE AS (

                    SELECT C.CAL_YEAR, C.GEO_STATE_NAME, RPT_CLM_TYPE_CD, 
                          SUM(NOF_BILLS) AS NOF_BILLS, SUM(PMT_AMT + PASSTHRU_AMT) AS PMT_AMT, SUM(CHRG_AMT) AS CHRG_AMT
                    FROM (	  
                            SELECT C.CAL_YEAR, C.GEO_STATE_NAME, RPT_CLM_TYPE_CD 
                                  ,NOF_BILLS ,PMT_AMT ,CHRG_AMT
                              
                                  ,CASE WHEN RPT_CLM_TYPE_CD = 1 AND RPT_PPS_CD = 1
                                        THEN (CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT) 
                                        ELSE 0
                                    END AS PASSTHRU_AMT
                            FROM PARTA_DTL_CLMS C
                    ) C		
                    GROUP BY C.CAL_YEAR, C.GEO_STATE_NAME, RPT_CLM_TYPE_CD
                 
                )

                ,ALL_RPT_DATA AS (

                    SELECT RPT.*, COALESCE(C.NOF_BILLS,0) AS NOF_BILLS, COALESCE(C.PMT_AMT,0) AS PMT_AMT, COALESCE(C.CHRG_AMT,0) AS CHRG_AMT
                    FROM PARTA_SUM_DATA_BY_CLM_TYPE C
                    
                    RIGHT OUTER JOIN ALL_RPT_ROWS RPT		
                    ON  RPT.CAL_YEAR        = C.CAL_YEAR
                    AND RPT.GEO_STATE_NAME  = C.GEO_STATE_NAME
                    AND RPT.RPT_CLM_TYPE_CD = C.RPT_CLM_TYPE_CD

                )
                            
                ,RPT_DATA_FORMATTED  AS  (

                    SELECT GEO_STATE_NAME, CAL_YEAR 
                    
                         ,SUM(NOF_BILLS) AS TOT_NOF_BILLS
                         ,SUM(CHRG_AMT)  AS TOT_CHRG_AMT
                         ,ROUND(SUM(PMT_AMT),2)   AS TOT_PMT_AMT
                    
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD IN ('1','2') THEN NOF_BILLS ELSE 0 END) AS INP_NOF_BILLS
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD IN ('1','2') THEN CHRG_AMT  ELSE 0 END) AS INP_CHRG_AMT
                         ,ROUND(SUM(CASE WHEN RPT_CLM_TYPE_CD IN ('1','2') THEN PMT_AMT   ELSE 0 END),2) AS INP_PMT_AMT
                     
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '3' THEN NOF_BILLS ELSE 0 END) AS SNF_NOF_BILLS
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '3' THEN CHRG_AMT  ELSE 0 END) AS SNF_CHRG_AMT
                         ,ROUND(SUM(CASE WHEN RPT_CLM_TYPE_CD = '3' THEN PMT_AMT   ELSE 0 END),2) AS SNF_PMT_AMT
                    
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '4' THEN NOF_BILLS ELSE 0 END) AS HHA_NOF_BILLS
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '4' THEN CHRG_AMT  ELSE 0 END) AS HHA_CHRG_AMT
                         ,ROUND(SUM(CASE WHEN RPT_CLM_TYPE_CD = '4' THEN PMT_AMT   ELSE 0 END),2) AS HHA_PMT_AMT
                    
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '6' THEN NOF_BILLS ELSE 0 END) AS HSP_NOF_BILLS
                         ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '6' THEN CHRG_AMT  ELSE 0 END) AS HSP_CHRG_AMT
                         ,ROUND(SUM(CASE WHEN RPT_CLM_TYPE_CD = '6' THEN PMT_AMT   ELSE 0 END),2) AS HSP_PMT_AMT
                         
                    
                    FROM ALL_RPT_DATA
                    
                    GROUP BY  GEO_STATE_NAME, CAL_YEAR

                )


                SELECT *
                FROM RPT_DATA_FORMATTED

                ORDER BY GEO_STATE_NAME, CAL_YEAR


    
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