#!/usr/bin/env python
########################################################################################################
# Name:   STS_MED_INS_Tbl_Rpts.py
# DESC:   This script extracts data for STS Medical Insureance table report - NOF Bills; Amt Reimbursed 
#         by period expense (legacy BB2A report)
#
# Created: Paul Baranoski 8/09/2024
# Modified: 
#
# Paul Baranoski 2024-08-09 Created program.
# Paul Baranoski 2024-08-29 Modify SQL for better performance.
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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STS_MED_INS_STG/STS_MED_INS_RPT_BB2A_{EXT_TO_YYYY}_{RUN_PRD}_{TMSTMP}.csv.gz
                    FROM (

            WITH SERVICE_CATEGORIES AS (

                SELECT '0' AS SERVICE_CD,'Total'                  AS SERVICE_DESC FROM DUAL
                UNION
                SELECT '1' AS SERVICE_CD,'Physician Services'     AS SERVICE_DESC FROM DUAL
                UNION
                SELECT '2' AS SERVICE_CD,'Outpatient Hospital'    AS SERVICE_DESC FROM DUAL
                UNION  
                SELECT '3' AS SERVICE_CD,'Independent Laboratory' AS SERVICE_DESC FROM DUAL
                UNION  
                SELECT '4' AS SERVICE_CD,'Home Health'            AS SERVICE_DESC FROM DUAL
                UNION  
                SELECT '5' AS SERVICE_CD,'Other Services'         AS SERVICE_DESC FROM DUAL

            )

            ,RPT_YEARS AS (
 
                SELECT {EXT_TO_YYYY}     AS CAL_YEAR FROM DUAL 
                UNION 
                SELECT {EXT_TO_YYYY} - 1 AS CAL_YEAR FROM DUAL 
                UNION 
                SELECT {EXT_TO_YYYY} - 2 AS CAL_YEAR FROM DUAL 
                UNION 
                SELECT {EXT_TO_YYYY} - 3 AS CAL_YEAR FROM DUAL 
                UNION 
                SELECT {EXT_TO_YYYY} - 4 AS CAL_YEAR FROM DUAL
                UNION 
                SELECT {EXT_TO_YYYY} - 5 AS CAL_YEAR FROM DUAL
                UNION 
                SELECT {EXT_TO_YYYY} - 6 AS CAL_YEAR FROM DUAL
                          
            )

            ,RPT_STATES    AS (

                SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME AS GEO_STATE_NAME, '1' AS SORT_ORD_IND
                FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                WHERE GEO_SSA_STATE_CD BETWEEN '01' AND '53'
                         
                UNION
                          
                SELECT DISTINCT COALESCE(GEO_SSA_STATE_CD,'  '), 'FOREIGN COUNTRIES', '2' AS SORT_ORD_IND
                FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                WHERE GEO_SSA_STATE_CD BETWEEN '54' AND '62' 
                          
                UNION
                          
                -- US Possessions, America Samoa, NORTHERN MARIANA, SAIPAN
                SELECT DISTINCT COALESCE(GEO_SSA_STATE_CD,'  '), 'OTHER OUTLYING AREAS', '3' AS SORT_ORD_IND
                FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                WHERE GEO_SSA_STATE_CD IN ('63','64','66','97') 

                UNION
                          
                -- Guam             
                SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME, '4' AS SORT_ORD_IND
                FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                WHERE GEO_SSA_STATE_CD IN ('65','98')
                          
                UNION
                          
                -- Unknown (67 thru 96 missing from GEO_SSA_STATE_CD 
                SELECT COALESCE(GEO_SSA_STATE_CD,'99'), 'RESIDENCE UNKNOWN', '5' AS SORT_ORD_IND
                FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                WHERE GEO_SSA_STATE_CD BETWEEN '67' AND '96'
                OR GEO_SSA_STATE_CD IN ('99','~ ', 'UK')
                          
            )
                
            ,ALL_RPT_ROWS  AS (

                -- All Rpt combinations
                SELECT DISTINCT CAL_YEAR, SERVICE_CD, SERVICE_DESC, GEO_STATE_NAME, SORT_ORD_IND 
                  FROM SERVICE_CATEGORIES
                      ,RPT_YEARS 
                      ,RPT_STATES

            )

            ,PARTB_DTL_CLMS AS (

                 SELECT CL.GEO_BENE_SK
                      ,CL.CLM_DT_SGNTR_SK
                      ,CL.CLM_TYPE_CD
                      ,CL.CLM_NUM_SK
                      ,CL.CLM_LINE_NUM

                      ,COALESCE(C.GEO_BENE_SSA_STATE_CD,'~ ') AS BENE_SSA_STATE_CD
                  
                      ,TO_CHAR(CL.CLM_LINE_THRU_DT,'YYYY') AS CAL_YEAR

                       --********************************************* 
                       -- Count claim lines as a BILL, '03' is a Cancel claim.
                       --********************************************* 
                      ,CASE WHEN C.CLM_DISP_CD = '03' THEN -1 ELSE 1 END AS NOF_BILLS
                                            
                      --*********************************************************** 
                      -- If Cancel claim --> set amt to negative (back-out) ELSE use amt
                      --***********************************************************
                      ,CASE WHEN C.CLM_DISP_CD = '03' THEN CL.CLM_LINE_PRVDR_PMT_AMT * -1
                                                      ELSE CL.CLM_LINE_PRVDR_PMT_AMT 
                       END AS REIMBURSEMENT_AMT
                                            
                       -- SERVICE CATEGORY
                       ,CASE WHEN C.CLM_CNTRCTR_NUM BETWEEN '99990' AND '99992' 
                             THEN '5' 
                             WHEN CL.CLM_POS_CD = '81'
                             THEN '3'
                             WHEN CLP.CLM_PRVDR_SPCLTY_CD = '69'
                             THEN '3'
                             WHEN CLP.CLM_TYPE_SRVC_CD = '9' 
                              AND ( (CLP.CLM_PRVDR_SPCLTY_CD BETWEEN '51' AND '62') OR CLP.CLM_PRVDR_SPCLTY_CD IN ('A8','88') )
                             THEN '5' 
                             WHEN (   CLP.CLM_TYPE_SRVC_CD BETWEEN '0' AND '9') 
                                   OR (CLP.CLM_TYPE_SRVC_CD IN ('H','I','P','R','T','U','W') )
                             THEN '1'
                             ELSE '5'
                        END AS SERVICE_CD     


                FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM  C  

                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN  CDN
                ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
                AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
                AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
                AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK

                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE  CL
                ON  C.GEO_BENE_SK     = CL.GEO_BENE_SK
                AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                AND C.CLM_FROM_DT     = CL.CLM_FROM_DT
                
                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_PRFNL  CLP
                ON  CL.GEO_BENE_SK     = CLP.GEO_BENE_SK
                AND CL.CLM_DT_SGNTR_SK = CLP.CLM_DT_SGNTR_SK
                AND CL.CLM_TYPE_CD     = CLP.CLM_TYPE_CD
                AND CL.CLM_NUM_SK      = CLP.CLM_NUM_SK
                AND CL.CLM_LINE_NUM    = CLP.CLM_LINE_NUM 

                INNER JOIN IDRC_{ENVNAME}.CMS_DIM_CLM_CD_{ENVNAME}.CLM_CNTRCTR_NUM  CARR
                ON CARR.CLM_CNTRCTR_NUM = C.CLM_CNTRCTR_NUM

                WHERE C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_FROM_YYYY}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DATE}','YYYY-MM-DD')
                  AND C.CLM_TYPE_CD BETWEEN 71 and 82
                  AND CDN.CLM_NRLN_RIC_CD IN ('O','M')
                  -- O = Part B physician/supplier claim record (processed by local carriers; can include DMEPOS services)
                  -- M = Part B DMEPOS claim record (processed by DME Regional Carrier)
                  AND CLP.CLM_PRCSG_IND_CD IN ('A','R','S')

                  AND CL.CLM_LINE_ALOWD_CHRG_AMT <> 0
                
                
            )

            ,PARTA_DTL_CLMS AS (

                SELECT C.GEO_BENE_SK
                      ,C.CLM_DT_SGNTR_SK
                      ,C.CLM_TYPE_CD
                      ,C.CLM_NUM_SK

                     ,COALESCE(C.GEO_BENE_SSA_STATE_CD,'~ ') AS BENE_SSA_STATE_CD
                     ,TO_CHAR(C.CLM_THRU_DT,'YYYY') AS CAL_YEAR

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
                     END AS  REIMBURSEMENT_AMT


                     ,C.CLM_BLG_PRVDR_OSCAR_NUM
                     ,C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD as BILL_TYPE
                     ,CDN.CLM_NRLN_RIC_CD
                     
                     -- SOMETIMES SERVICE_CD is NULL
                     ,CASE  WHEN C.CLM_TYPE_CD = '10'
                            THEN '4'
                            
                            --WHEN CDN.CLM_NRLN_RIC_CD = 'U'
                            --THEN '4'
                            
                            WHEN (C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD) IN ('12','22')
                            AND SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) IN ('1','2','3','4')
                            THEN '5'
                            
                            WHEN (C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD) IN ('12','22')
				             AND SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '7000' AND '9799'
				            THEN '5' 
                
                            WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,1) = '5'
                            THEN '5'
                            
                            WHEN (C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD)  IN ( '13', '23', '34', '72', '73', '74', '75', '76','83', '85')
                            AND SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '6500' AND '6989'
                            THEN '5'
                            
                            WHEN (C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD)  IN ( '13', '23', '34', '72', '73', '74', '75', '76', '83', '85')
                            AND NOT SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '6500' AND '6989'
                            THEN '2'
                            
                            WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '3100' AND '3199' 
                            THEN '4'
                            WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '7000' AND '8499' 
                            THEN '4'
                            WHEN SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) BETWEEN '9000' AND '9799' 
                            THEN '4'
                       END  AS SERVICE_CD
                
                FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM  C
                
                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR  CDS
                ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN  CDN
                ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
                AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
                AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
                AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK
                          
                WHERE C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_FROM_YYYY}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DATE}','YYYY-MM-DD')
                AND CDN.CLM_NRLN_RIC_CD IN ('W','U')
                -- W = Part B institutional claim record (outpatient (OP), HHA)
	            -- U = Both Part A and B institutional home health agency (HHA) claim records -- due to HHPPS and HHA A/B split.
                
                AND NOT (C.CLM_BILL_FAC_TYPE_CD || C.CLM_BILL_CLSFCTN_CD || C.CLM_BILL_FREQ_CD) IN ('322','332')
                AND CDN.CLM_CWF_BENE_MDCR_STUS_CD IN ('10','11','20','21','31','40' )

                AND NOT C.CLM_QUERY_CD = 'C'

                          
            )

            ,CLM_VAL_DTL_DATA AS (

                  SELECT C.GEO_BENE_SK
                        ,C.CLM_DT_SGNTR_SK
                        ,C.CLM_TYPE_CD
                        ,C.CLM_NUM_SK

                         ,CASE WHEN C.CLM_NRLN_RIC_CD = 'U' AND VAL.CLM_VAL_CD IN ('64','65','17')
                               THEN VAL.CLM_VAL_AMT
                               ELSE 0
                          END AS VAL_REIMBURSEMENT_AMT
                         
                         ,CASE WHEN VAL.CLM_VAL_CD = '63' THEN VAL.CLM_VAL_AMT ELSE 0 END  AS PTB_VISITS 

                         ,CASE WHEN VAL.CLM_VAL_CD IN ('62','64') THEN VAL.CLM_VAL_AMT ELSE 0 END  AS PTA_PTB_VISITS 
                                               
                  FROM PARTA_DTL_CLMS C

                  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_VAL VAL
                  ON  C.GEO_BENE_SK     = VAL.GEO_BENE_SK
                  AND C.CLM_DT_SGNTR_SK = VAL.CLM_DT_SGNTR_SK
                  AND C.CLM_TYPE_CD     = VAL.CLM_TYPE_CD
                  AND C.CLM_NUM_SK      = VAL.CLM_NUM_SK

                  WHERE C.CLM_NRLN_RIC_CD = 'U'
                    AND VAL.CLM_VAL_CD IN ('17','62','63','64','65')
              

            )

            --!!!!! Need to join this data into the main claim data and sum.
            ,CLM_VAL_SUM_DATA  AS  (


                SELECT GEO_BENE_SK
                       ,CLM_DT_SGNTR_SK
                       ,CLM_TYPE_CD
                       ,CLM_NUM_SK
                       ,ROUND((VAL_REIMBURSEMENT_AMT * PTB_PERC),2) AS VAL_REIMBURSEMENT_AMT
                FROM (

                     SELECT GEO_BENE_SK
                         ,CLM_DT_SGNTR_SK
                         ,CLM_TYPE_CD
                         ,CLM_NUM_SK
                         ,SUM(VAL_REIMBURSEMENT_AMT) AS VAL_REIMBURSEMENT_AMT
                         ,SUM(PTB_VISITS)            AS PTB_VISITS 
                         ,SUM(PTA_PTB_VISITS)        AS PTA_PTB_VISITS
                         ,CASE WHEN SUM(PTA_PTB_VISITS) = 0 
                               THEN 0
                               ELSE SUM(PTB_VISITS) / SUM(PTA_PTB_VISITS) 
                          END AS PTB_PERC     
                     FROM CLM_VAL_DTL_DATA       

                     GROUP BY GEO_BENE_SK, CLM_DT_SGNTR_SK, CLM_TYPE_CD, CLM_NUM_SK

                )
                WHERE VAL_REIMBURSEMENT_AMT <> 0 
                AND PTB_PERC <> 0

            )

            -- Combine Claim and Value data
            ,PARTA_CLM_VAL_DTL_DATA AS (

                SELECT * 
                FROM (
                     SELECT C.*, COALESCE(VAL.VAL_REIMBURSEMENT_AMT,0) AS  VAL_REIMBURSEMENT_AMT
                     FROM PARTA_DTL_CLMS C

                     LEFT OUTER JOIN CLM_VAL_SUM_DATA VAL
                     ON  C.GEO_BENE_SK     = VAL.GEO_BENE_SK
                     AND C.CLM_DT_SGNTR_SK = VAL.CLM_DT_SGNTR_SK
                     AND C.CLM_TYPE_CD     = VAL.CLM_TYPE_CD
                     AND C.CLM_NUM_SK      = VAL.CLM_NUM_SK
                )

            )

            ,PARTA_SUM_DATA_BY_CAT AS (


                SELECT 
                         CAL_YEAR
                        ,GEO_STATE_NAME 
                        ,SERVICE_CD 
                        ,SUM(NOF_BILLS)    AS NOF_BILLS
                        ,SUM(REIMBURSEMENT_AMT) + SUM(VAL_REIMBURSEMENT_AMT)  AS REIMBURSEMENT_AMT
                  FROM (

                         SELECT  DTL.CAL_YEAR
                                 ,COALESCE(RPT.GEO_STATE_NAME,'RESIDENCE UNKNOWN') AS GEO_STATE_NAME 
                                 ,DTL.SERVICE_CD 
                                 ,DTL.NOF_BILLS
                                 ,DTL.REIMBURSEMENT_AMT
                                 ,DTL.VAL_REIMBURSEMENT_AMT
                                                      
                         FROM PARTA_CLM_VAL_DTL_DATA DTL
                         
                         -- Don't filter out claims with SSA_STATE_CD not in dimension table
                         LEFT OUTER JOIN RPT_STATES RPT
                         ON DTL.BENE_SSA_STATE_CD = RPT.GEO_SSA_STATE_CD

                  )
                          
                GROUP BY CAL_YEAR
                        ,GEO_STATE_NAME 
                        ,SERVICE_CD
                ORDER BY CAL_YEAR, GEO_STATE_NAME, SERVICE_CD 

            )

            ,PARTB_SUM_DATA_BY_CAT AS (


                SELECT 
                         CAL_YEAR
                        ,GEO_STATE_NAME 
                        ,SERVICE_CD 
                        ,SUM(NOF_BILLS)          AS NOF_BILLS
                        ,SUM(REIMBURSEMENT_AMT)  AS REIMBURSEMENT_AMT

                FROM (

                    SELECT  DTL.CAL_YEAR
                      ,COALESCE(RPT.GEO_STATE_NAME,'RESIDENCE UNKNOWN') AS GEO_STATE_NAME 
                      ,DTL.SERVICE_CD 
                      ,DTL.NOF_BILLS
                      ,DTL.REIMBURSEMENT_AMT
                                              
                    FROM PARTB_DTL_CLMS DTL

                    -- Don't filter out claims with SSA_STATE_CD not in dimension table
                    LEFT OUTER JOIN RPT_STATES RPT
                    ON DTL.BENE_SSA_STATE_CD = RPT.GEO_SSA_STATE_CD

                )
                          
                GROUP BY CAL_YEAR
                        ,GEO_STATE_NAME 
                        ,SERVICE_CD
                ORDER BY CAL_YEAR, GEO_STATE_NAME, SERVICE_CD 

            )

            ,PARTAB_SUM_DATA_BY_CAT AS (

                SELECT CAL_YEAR, GEO_STATE_NAME, SERVICE_CD, SUM(NOF_BILLS) AS NOF_BILLS, SUM(REIMBURSEMENT_AMT) AS REIMBURSEMENT_AMT,
                FROM (
                       SELECT CAL_YEAR, GEO_STATE_NAME, SERVICE_CD, NOF_BILLS, REIMBURSEMENT_AMT 
                       FROM PARTA_SUM_DATA_BY_CAT
                       UNION 
                       SELECT CAL_YEAR, GEO_STATE_NAME, SERVICE_CD, NOF_BILLS, REIMBURSEMENT_AMT  
                       FROM PARTB_SUM_DATA_BY_CAT
                     ) CLM_DATA                    

                          
                GROUP BY CAL_YEAR
                        ,GEO_STATE_NAME 
                        ,SERVICE_CD
                                                       
            )

            ,PARTAB_SUM_DATA_TOTAL AS (

                SELECT 
                     CAL_YEAR
                    ,GEO_STATE_NAME 
                    ,'0'    AS SERVICE_CD
                    ,NOF_BILLS
                    ,REIMBURSEMENT_AMT

                FROM ( 

                   SELECT 
                         CAL_YEAR
                        ,GEO_STATE_NAME 
                        ,SUM(NOF_BILLS)         AS NOF_BILLS
                        ,SUM(REIMBURSEMENT_AMT) AS REIMBURSEMENT_AMT
                                                              
                   FROM PARTAB_SUM_DATA_BY_CAT

                   GROUP BY CAL_YEAR
                           ,GEO_STATE_NAME 
                                                
                   )
                        
            )

            ,PARTAB_ALL_SUM_DATA AS (

                SELECT   
                      CAL_YEAR
                     ,GEO_STATE_NAME
                     ,SERVICE_CD
                     ,NOF_BILLS
                     ,REIMBURSEMENT_AMT
                     ,CASE WHEN NOF_BILLS = 0    THEN 0
                           WHEN NOF_BILLS < 0    THEN (ROUND(REIMBURSEMENT_AMT / NOF_BILLS,0))  * -1
                           ELSE ROUND(REIMBURSEMENT_AMT / NOF_BILLS,0)
                     END  AS PER_BILLS        
                                            
                FROM ( 
                     SELECT *
                     FROM PARTAB_SUM_DATA_BY_CAT    
                     UNION
                     SELECT * 
                     FROM PARTAB_SUM_DATA_TOTAL
                )

            )

            ,ALL_RPT_DATA  AS  (

                SELECT   
                      RPT.CAL_YEAR
                     ,RPT.GEO_STATE_NAME
                     ,RPT.SORT_ORD_IND
                     ,RPT.SERVICE_CD
                     ,RPT.SERVICE_DESC
                     ,COALESCE(NOF_BILLS,0)         AS NOF_BILLS
                     ,COALESCE(REIMBURSEMENT_AMT,0) AS REIMBURSEMENT_AMT
                     ,COALESCE(PER_BILLS,0)         AS PER_BILLS


                FROM PARTAB_ALL_SUM_DATA   DATA

                RIGHT OUTER JOIN ALL_RPT_ROWS RPT
                ON  DATA.CAL_YEAR          = RPT.CAL_YEAR
                AND DATA.GEO_STATE_NAME    = RPT.GEO_STATE_NAME
                AND DATA.SERVICE_CD        = RPT.SERVICE_CD 

                ORDER BY RPT.CAL_YEAR, RPT.SORT_ORD_IND, RPT.GEO_STATE_NAME, RPT.SERVICE_CD

            )

            ,RPT_DATA_FORMATTED  AS  (

                SELECT 
                      GEO_STATE_NAME
                     ,SORT_ORD_IND 
                     ,CAL_YEAR
                     ,MAX(CASE WHEN SERVICE_CD = 0 THEN REIMBURSEMENT_AMT END) AS TOT_AMT
                     ,MAX(CASE WHEN SERVICE_CD = 1 THEN NOF_BILLS         END) AS PHYS_NOF_BILLS 
                     ,MAX(CASE WHEN SERVICE_CD = 1 THEN REIMBURSEMENT_AMT END) AS PHYS_AMT
                     ,MAX(CASE WHEN SERVICE_CD = 1 THEN PER_BILLS         END) AS PHYS_PER_BILLS  
                     ,MAX(CASE WHEN SERVICE_CD = 2 THEN NOF_BILLS         END) AS OPT_NOF_BILLS 
                     ,MAX(CASE WHEN SERVICE_CD = 2 THEN REIMBURSEMENT_AMT END) AS OPT_AMT
                     ,MAX(CASE WHEN SERVICE_CD = 2 THEN PER_BILLS         END) AS OPT_PER_BILLS 
                     ,MAX(CASE WHEN SERVICE_CD = 3 THEN NOF_BILLS         END) AS IND_LAB_NOF_BILLS 
                     ,MAX(CASE WHEN SERVICE_CD = 3 THEN REIMBURSEMENT_AMT END) AS IND_LAB_AMT
                     ,MAX(CASE WHEN SERVICE_CD = 3 THEN PER_BILLS         END) AS IND_LAB_PER_BILLS  
                     ,MAX(CASE WHEN SERVICE_CD = 4 THEN NOF_BILLS         END) AS HHA_NOF_BILLS 
                     ,MAX(CASE WHEN SERVICE_CD = 4 THEN REIMBURSEMENT_AMT END) AS HHA_AMT
                     ,MAX(CASE WHEN SERVICE_CD = 4 THEN PER_BILLS         END) AS HHA_PER_BILLS 
                     ,MAX(CASE WHEN SERVICE_CD = 5 THEN REIMBURSEMENT_AMT END) AS OTH_AMT
                     
                FROM ALL_RPT_DATA

                GROUP BY SORT_ORD_IND, GEO_STATE_NAME, CAL_YEAR
            --ORDER BY SORT_ORD_IND, GEO_STATE_NAME, CAL_YEAR

            )


            ,RPT_DATA_ALL_AREAS  AS  (

                SELECT 
                      'ALL AREAS'   AS GEO_STATE_NAME
                     ,'6'           AS SORT_ORD_IND   
                     ,CAL_YEAR
                     
                    ,SUM(TOT_AMT)           AS TOT_AMT
                    ,SUM(PHYS_NOF_BILLS)    AS PHYS_NOF_BILLS 
                    ,SUM(PHYS_AMT)          AS PHYS_AMT
                    ,CASE WHEN SUM(PHYS_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(PHYS_NOF_BILLS) < 0 THEN  (ROUND(SUM(PHYS_AMT) / SUM(PHYS_NOF_BILLS),0)) * -1
                          ELSE ROUND(SUM(PHYS_AMT) / SUM(PHYS_NOF_BILLS),0) 
                     END  AS PHYS_PER_BILLS

                    ,SUM(OPT_NOF_BILLS)     AS OPT_NOF_BILLS 
                    ,SUM(OPT_AMT)           AS OPT_AMT
                    ,CASE WHEN SUM(OPT_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(OPT_NOF_BILLS) < 0 THEN (ROUND(SUM(OPT_AMT) / SUM(OPT_NOF_BILLS),0)) * -1
                          ELSE ROUND(SUM(OPT_AMT) / SUM(OPT_NOF_BILLS),0) 
                     END  AS OPT_PER_BILLS

                    ,SUM(IND_LAB_NOF_BILLS) AS IND_LAB_NOF_BILLS 
                    ,SUM(IND_LAB_AMT)       AS IND_LAB_AMT
                    ,CASE WHEN SUM(IND_LAB_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(IND_LAB_NOF_BILLS) < 0 THEN (ROUND(SUM(IND_LAB_AMT) / SUM(IND_LAB_NOF_BILLS),0)) * -1
                          ELSE ROUND(SUM(IND_LAB_AMT) / SUM(IND_LAB_NOF_BILLS),0) 
                     END  AS IND_LAB_PER_BILLS

                    ,SUM(HHA_NOF_BILLS) AS HHA_NOF_BILLS 
                    ,SUM(HHA_AMT)       AS HHA_AMT
                    ,CASE WHEN SUM(HHA_NOF_BILLS) = 0  THEN 0
                          WHEN SUM(HHA_NOF_BILLS) < 0 THEN (ROUND(SUM(HHA_AMT) / SUM(HHA_NOF_BILLS),0)) * -1 
                          ELSE ROUND(SUM(HHA_AMT) / SUM(HHA_NOF_BILLS),0) 
                     END  AS HHA_PER_BILLS

                    ,SUM(OTH_AMT)           AS OTH_AMT
                             
                FROM RPT_DATA_FORMATTED
                GROUP BY CAL_YEAR
                          
            )

            ,RPT_DATA_USA  AS  (

                SELECT 
                      'UNITED STATES'   AS GEO_STATE_NAME
                     ,'7'               AS SORT_ORD_IND   
                     ,CAL_YEAR
                     ,SUM(TOT_AMT)           AS TOT_AMT
                     ,SUM(PHYS_NOF_BILLS)    AS PHYS_NOF_BILLS 
                     ,SUM(PHYS_AMT)          AS PHYS_AMT
                    ,CASE WHEN SUM(PHYS_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(PHYS_NOF_BILLS) < 0 THEN (ROUND(SUM(PHYS_AMT) / SUM(PHYS_NOF_BILLS),0)) * -1
                                                       ELSE ROUND(SUM(PHYS_AMT) / SUM(PHYS_NOF_BILLS),0) 
                     END  AS PHYS_PER_BILLS

                    ,SUM(OPT_NOF_BILLS)     AS OPT_NOF_BILLS 
                    ,SUM(OPT_AMT)           AS OPT_AMT
                    ,CASE WHEN SUM(OPT_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(OPT_NOF_BILLS) < 0 THEN (ROUND(SUM(OPT_AMT) / SUM(OPT_NOF_BILLS),0)) * -1
                                                      ELSE ROUND(SUM(OPT_AMT) / SUM(OPT_NOF_BILLS),0) 
                     END  AS OPT_PER_BILLS
                     
                    ,SUM(IND_LAB_NOF_BILLS) AS IND_LAB_NOF_BILLS 
                    ,SUM(IND_LAB_AMT)       AS IND_LAB_AMT
                    ,CASE WHEN SUM(IND_LAB_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(IND_LAB_NOF_BILLS) < 0 THEN (ROUND(SUM(IND_LAB_AMT) / SUM(IND_LAB_NOF_BILLS),0)) * -1
                                                          ELSE ROUND(SUM(IND_LAB_AMT) / SUM(IND_LAB_NOF_BILLS),0) 
                     END  AS IND_LAB_PER_BILLS

                    ,SUM(HHA_NOF_BILLS) AS HHA_NOF_BILLS 
                    ,SUM(HHA_AMT)       AS HHA_AMT
                    ,CASE WHEN SUM(HHA_NOF_BILLS) = 0 THEN 0
                          WHEN SUM(HHA_NOF_BILLS) < 0 THEN ((ROUND(SUM(HHA_AMT) / SUM(HHA_NOF_BILLS),0))) * -1
                                                      ELSE ROUND(SUM(HHA_AMT) / SUM(HHA_NOF_BILLS),0) 
                     END  AS HHA_PER_BILLS

                    ,SUM(OTH_AMT)           AS OTH_AMT
                                         
                FROM RPT_DATA_FORMATTED
                WHERE NOT GEO_STATE_NAME IN ('FOREIGN COUNTRIES','RESIDENCE UNKNOWN') 
                GROUP BY CAL_YEAR
                          
            )

            SELECT *
            FROM (

                SELECT *
                FROM RPT_DATA_FORMATTED

                UNION 

                SELECT *
                FROM RPT_DATA_ALL_AREAS

                UNION 

                SELECT *
                FROM RPT_DATA_USA
            )

            ORDER BY SORT_ORD_IND, GEO_STATE_NAME, CAL_YEAR
    
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