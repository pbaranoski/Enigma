#!/usr/bin/env python
########################################################################################################
# Name:   STS_HHA_RevCtr_Rpts.py
#
# DESC:   This script extracts data for STS HHA table report - HHA Rev Cntr UNITS/CHRGS	  
#         by period expense (legacy AA5 report)
#
# Created: Paul Baranoski 8/27/2024
# Modified: 
#
# Paul Baranoski 2024-08-27 Created program.
# Paul Baranoski 2024-09-30 Add '40' to CLM_CWF_BENE_MDCR_STUS_CD filter.
# Paul Baranoski 2024-10-01 Remove 'FOREIGN COUNTRIES','VIRGIN ISLANDS', 'PUERTO RICO', 'GUAM', 'OTHER OUTLYING AREAS'
#                           from USA totals.
# Paul Baranoski 2025-02-19 Modify to use new BIA_{ENVNAME}_XTR_STS_HHA_REV_CTR_STG stage.
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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STS_HHA_REV_CTR_STG/STS_HHA_RPT_AA5_{EXT_TO_YYYY}_{RUN_PRD}_{TMSTMP}.csv.gz
                    FROM (

                        WITH REV_CTR_CATEGORIES AS (

                            SELECT '055' AS REV_CTR_CAT,'SKILLED NURSING (55X)     '    AS REV_CTR_CAT_DESC, '1' AS RC_SORT_ORD FROM DUAL
                            UNION
                            SELECT '042' AS REV_CTR_CAT,'PHYSICAL THERAPY (42X)    '    AS REV_CTR_CAT_DESC, '2' AS RC_SORT_ORD FROM DUAL
                            UNION
                            SELECT '044' AS REV_CTR_CAT,'SPEECH THERAPY (44X)      '    AS REV_CTR_CAT_DESC, '3' AS RC_SORT_ORD FROM DUAL
                            UNION
                            SELECT '043' AS REV_CTR_CAT,'OCCUPATIONAL THERAPY (43X)'    AS REV_CTR_CAT_DESC, '4' AS RC_SORT_ORD FROM DUAL
                            UNION
                            SELECT '056' AS REV_CTR_CAT,'MEDICAL SOCIAL SERVICES (56X)' AS REV_CTR_CAT_DESC, '5' AS RC_SORT_ORD FROM DUAL
                            UNION
                            SELECT '057' AS REV_CTR_CAT,'HOME HEALTH AIDE (57X)'        AS REV_CTR_CAT_DESC, '6' AS RC_SORT_ORD FROM DUAL
                            UNION
                            SELECT 'ZZZ' AS REV_CTR_CAT,'OTHER                 '        AS REV_CTR_CAT_DESC, '7' AS RC_SORT_ORD FROM DUAL
                            
                        )

                        ,RPT_STATES    AS (

                            SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME AS GEO_STATE_NAME, '1' AS ST_SORT_ORD
                            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                            WHERE GEO_SSA_STATE_CD BETWEEN '01' AND '53'
                                     
                            UNION
                                      
                            SELECT DISTINCT COALESCE(GEO_SSA_STATE_CD,'  '), 'FOREIGN COUNTRIES', '2' AS ST_SORT_ORD
                            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                            WHERE GEO_SSA_STATE_CD BETWEEN '54' AND '62' 
                                      
                            UNION
                                      
                            -- US Possessions, America Samoa, NORTHERN MARIANA, SAIPAN
                            SELECT DISTINCT COALESCE(GEO_SSA_STATE_CD,'  '), 'OTHER OUTLYING AREAS', '3' AS ST_SORT_ORD
                            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                            WHERE GEO_SSA_STATE_CD IN ('63','64','66','97') 

                            UNION
                                      
                            -- Guam             
                            SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME, '4' AS ST_SORT_ORD
                            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                            WHERE GEO_SSA_STATE_CD IN ('65','98')
                                      
                            UNION
                                      
                            -- Unknown (67 thru 96 missing from GEO_SSA_STATE_CD 
                            SELECT COALESCE(GEO_SSA_STATE_CD,'99'), 'RESIDENCE UNKNOWN', '5' AS ST_SORT_ORD
                            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
                            WHERE GEO_SSA_STATE_CD BETWEEN '67' AND '96'
                            OR GEO_SSA_STATE_CD IN ('99','~ ', 'UK')
                            
                        )

                        ,RPT_STATE_NAMES    AS (
						
							SELECT DISTINCT GEO_STATE_NAME, ST_SORT_ORD
							FROM RPT_STATES
							
						)
                        
                        ,RPT_YEARS AS (

                            SELECT {EXT_TO_YYYY}     AS CAL_YEAR FROM DUAL 
                            UNION 
                            SELECT {EXT_TO_YYYY} - 1 AS CAL_YEAR FROM DUAL 
                            UNION 
                            SELECT {EXT_TO_YYYY} - 2 AS CAL_YEAR FROM DUAL 	
                                      
                        )

                        ,ALL_ST_YR_ROWS  AS (

                            -- All Rpt combinations
                            SELECT DISTINCT CAL_YEAR, GEO_STATE_NAME, ST_SORT_ORD 
                              FROM RPT_YEARS 
                                  ,RPT_STATES

                        )

                        ,ALL_RPT_ROWS  AS (

                            -- All Rpt combinations
                            SELECT DISTINCT CAL_YEAR, REV_CTR_CAT, REV_CTR_CAT_DESC, RC_SORT_ORD, GEO_STATE_NAME, ST_SORT_ORD 
                              FROM REV_CTR_CATEGORIES
                                  ,RPT_YEARS 
                                  ,RPT_STATES

                        )

                        ,CLM_HDR_DTL AS (

                            -- Payments are at a header level. Very rarely at the line level.
                            SELECT C.GEO_BENE_SK
                                  ,C.CLM_DT_SGNTR_SK
                                  ,C.CLM_TYPE_CD
                                  ,C.CLM_NUM_SK
                                  ,C.CLM_FROM_DT

                                  ,TO_CHAR(C.CLM_THRU_DT,'YYYY') AS CAL_YEAR

                                  ,COALESCE(C.GEO_BENE_SSA_STATE_CD,'~  ') AS BENE_SSA_STATE_CD
                                  ,COALESCE(ST.GEO_STATE_NAME, 'RESIDENCE UNKNOWN') AS GEO_STATE_NAME

                                  ,C.CLM_QUERY_CD
                                  
                                  ,CASE WHEN C.CLM_QUERY_CD = '0' THEN -1 ELSE 1 END AS NOF_BILLS  
                                  
                                  ,CASE WHEN C.CLM_QUERY_CD = '0' 
                                        THEN (COALESCE(C.CLM_PMT_AMT,0) * -1)  
                                        ELSE  COALESCE(C.CLM_PMT_AMT,0)  END  AS PMT_AMT


                                FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C  
                                
                                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN CDN
                                ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
                                AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
                                AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
                                AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK
                                
                                LEFT OUTER JOIN RPT_STATES ST
                                ON C.GEO_BENE_SSA_STATE_CD = ST.GEO_SSA_STATE_CD
                                
                                 -- Claim approved
                                WHERE C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_FROM_YYYY}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DATE}','YYYY-MM-DD')
                                  AND C.CLM_TYPE_CD = '10'
                                  --AND C.GEO_BENE_SSA_STATE_CD = '05'

                                  AND CDN.CLM_CWF_BENE_MDCR_STUS_CD IN ('10','11','20','21','31','40' )

                                  -- Do not select claims that are for Health Insurance Prospective Payment System (HIPPS)		
                                  AND EXISTS (SELECT '1' 
                                                FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                                                WHERE C.GEO_BENE_SK     = CL.GEO_BENE_SK
                                                AND C.CLM_DT_SGNTR_SK   = CL.CLM_DT_SGNTR_SK
                                                AND C.CLM_TYPE_CD       = CL.CLM_TYPE_CD
                                                AND C.CLM_NUM_SK        = CL.CLM_NUM_SK
                                                AND C.CLM_FROM_DT       = CL.CLM_FROM_DT
                                                AND NOT SUBSTR(CL.CLM_LINE_REV_CTR_CD,1,3) IN ('002','003', '004', '005', '006', '007','008','009')
                                                AND NOT CL.CLM_LINE_REV_CTR_CD = '0001' )
                                
                                  --AND C.CLM_FINL_ACTN_IND = 'Y'

                        )


                        ,CLM_LINE_ITEM_DTL AS (

                            SELECT C.GEO_BENE_SK
                                  ,C.CLM_DT_SGNTR_SK
                                  ,C.CLM_TYPE_CD
                                  ,C.CLM_NUM_SK

                                  ,C.CLM_QUERY_CD

                                  ,C.CAL_YEAR
                                  ,C.BENE_SSA_STATE_CD
                                  ,COALESCE(ST.GEO_STATE_NAME, 'RESIDENCE UNKNOWN') AS GEO_STATE_NAME
                                  
                                  --,SUBSTR(CL.CLM_LINE_REV_CTR_CD,1,3) AS REV_CTR_CAT
                                  ,COALESCE(RC.REV_CTR_CAT,'ZZZ') AS REV_CTR_CAT
                                  ,CL.CLM_LINE_REV_CTR_CD
                                    
                                  ,CASE WHEN C.CLM_QUERY_CD = '0' THEN (CL.CLM_LINE_SRVC_UNIT_QTY * -1)  ELSE CL.CLM_LINE_SRVC_UNIT_QTY  END  AS RC_UNITS
                                  ,CASE WHEN C.CLM_QUERY_CD = '0' THEN (CL.CLM_LINE_SBMT_CHRG_AMT * -1)  ELSE CL.CLM_LINE_SBMT_CHRG_AMT  END  AS RC_CHRG_AMT	
                                  ,CASE WHEN C.CLM_QUERY_CD = '0' THEN (CL.CLM_LINE_NCVRD_CHRG_AMT * -1) ELSE CL.CLM_LINE_NCVRD_CHRG_AMT END  AS RC_CLM_LINE_NCVRD_CHRG_AMT
                                       

                                /* These fields are always zero or NULL.  CLM_LINE_PRVDR_PMT_AMT is mostly Zeroes.
                                ,CL.CLM_LINE_PRVDR_PMT_AMT
                                ,CL.CLM_LINE_ALOWD_CHRG_AMT
                                ,CL.CLM_LINE_BENE_PD_AMT		
                                ,CL.CLM_LINE_BENE_PMT_AMT
                                ,CL.CLM_LINE_NCVRD_PD_AMT  */		

                                FROM CLM_HDR_DTL C  
                                
                                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                                ON  C.GEO_BENE_SK     = CL.GEO_BENE_SK
                                AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                                AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                                AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                                AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

                                LEFT OUTER JOIN REV_CTR_CATEGORIES RC
                                ON RC.REV_CTR_CAT  = SUBSTR(CL.CLM_LINE_REV_CTR_CD,1,3)

                                LEFT OUTER JOIN RPT_STATES ST
                                ON C.BENE_SSA_STATE_CD = ST.GEO_SSA_STATE_CD
                                
                                -- Revenue Center Code 0001 represents the total of all revenue centers included on the claim.
                                WHERE NOT CL.CLM_LINE_REV_CTR_CD = '0001' 

                                -- RCs for Health Insurance Prospective Payment System (HIPPS)
                                AND NOT SUBSTR(CL.CLM_LINE_REV_CTR_CD,1,3) IN ('002','003', '004', '005', '006', '007','008','009')

                                -- AND CL.CLM_LINE_FINL_ACTN_IND = 'Y'
                                  

                        )


                        ,CLM_SUM_TOTALS AS (

                            SELECT ST.GEO_STATE_NAME, ST.ST_SORT_ORD, CAL_YEAR
                                  ,SUM(PMT_AMT)       AS TOT_PMT_AMT
                                  ,SUM(NOF_BILLS)     AS TOT_BILLS 

                            FROM CLM_HDR_DTL C

                            INNER JOIN RPT_STATE_NAMES ST
                            ON C.GEO_STATE_NAME = ST.GEO_STATE_NAME

                            GROUP BY ST.GEO_STATE_NAME, ST.ST_SORT_ORD, CAL_YEAR 

                        )

                        ,CLM_LINE_ITEM_SUM_TOTALS AS (

                            SELECT ST.GEO_STATE_NAME, ST.ST_SORT_ORD, CAL_YEAR
                                  ,SUM(RC_UNITS)      AS TOT_UNITS
                                  ,SUM(RC_CHRG_AMT)   AS TOT_CHRGS

                            FROM CLM_LINE_ITEM_DTL C

                            INNER JOIN RPT_STATE_NAMES ST
                            ON C.GEO_STATE_NAME = ST.GEO_STATE_NAME

                            GROUP BY ST.GEO_STATE_NAME, ST.ST_SORT_ORD, CAL_YEAR 
                                
                        )

                        ,ST_YR_TOTALS AS (

                            -- Get all possible combinations                            
                            SELECT RPT.GEO_STATE_NAME, RPT.ST_SORT_ORD, RPT.CAL_YEAR
                                  ,COALESCE(TOT_BILLS,0)    AS TOT_BILLS 
                                  ,COALESCE(TOT_PMT_AMT,0)  AS TOT_PMT_AMT 
                                  ,COALESCE(TOT_UNITS,0)    AS TOT_UNITS 
                                  ,COALESCE(TOT_CHRGS,0)    AS TOT_CHRGS 
                            FROM (	
                            
                                    SELECT C.GEO_STATE_NAME, C.ST_SORT_ORD, C.CAL_YEAR
                                          ,TOT_BILLS
                                          ,TOT_PMT_AMT
                                          ,TOT_UNITS
                                          ,TOT_CHRGS

                                    FROM CLM_SUM_TOTALS C
                                    
                                    INNER JOIN CLM_LINE_ITEM_SUM_TOTALS CL
                                    ON  C.GEO_STATE_NAME = CL.GEO_STATE_NAME
                                    AND C.CAL_YEAR       = CL.CAL_YEAR  
                                
                            ) DATA
                            
                            RIGHT OUTER JOIN ALL_ST_YR_ROWS RPT
                            ON  DATA.CAL_YEAR          = RPT.CAL_YEAR
                            AND DATA.GEO_STATE_NAME    = RPT.GEO_STATE_NAME  

                        )

                         ,RC_ST_YR_TOTALS AS (
                        
                            -- Get all possible combinations
                            SELECT RPT.GEO_STATE_NAME, RPT.ST_SORT_ORD, RPT.CAL_YEAR, RPT.RC_SORT_ORD, RPT.REV_CTR_CAT
                                ,COALESCE(RC_UNITS,0)     AS RC_UNITS
                                ,COALESCE(RC_CHRGS,0)     AS RC_CHRGS	
                            FROM (
                            
                                SELECT C.GEO_STATE_NAME, ST_SORT_ORD, CAL_YEAR, C.REV_CTR_CAT
                                    ,SUM(RC_UNITS)     AS RC_UNITS
                                    ,SUM(RC_CHRG_AMT)  AS RC_CHRGS	

                                FROM CLM_LINE_ITEM_DTL C
                                
                                INNER JOIN RPT_STATE_NAMES ST
                                ON C.GEO_STATE_NAME = ST.GEO_STATE_NAME

                                GROUP BY C.GEO_STATE_NAME, ST.ST_SORT_ORD, CAL_YEAR, C.REV_CTR_CAT
                                
                            ) DATA
                            
                            RIGHT OUTER JOIN ALL_RPT_ROWS RPT
                            ON  DATA.CAL_YEAR          = RPT.CAL_YEAR
                            AND DATA.GEO_STATE_NAME    = RPT.GEO_STATE_NAME
                            AND DATA.REV_CTR_CAT       = RPT.REV_CTR_CAT

                        )

                        -- MAX (THEN NEGATIVE VALUE ELSE 0) will result in 0 value returned.
                        ,RC_ST_YR_TOTALS_1_ROW  AS  (

                            -- pivot table
                            SELECT 
                                  GEO_STATE_NAME
                                 ,ST_SORT_ORD 
                                 ,CAL_YEAR
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 1 THEN RC_UNITS ELSE NULL END),0) AS RC1_UNITS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 2 THEN RC_UNITS ELSE NULL END),0) AS RC2_UNITS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 3 THEN RC_UNITS ELSE NULL END),0) AS RC3_UNITS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 4 THEN RC_UNITS ELSE NULL END),0) AS RC4_UNITS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 5 THEN RC_UNITS ELSE NULL END),0) AS RC5_UNITS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 6 THEN RC_UNITS ELSE NULL END),0) AS RC6_UNITS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 7 THEN RC_UNITS ELSE NULL END),0) AS RC7_UNITS
                                 
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 1 THEN RC_CHRGS ELSE NULL END),0) AS RC1_CHRGS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 2 THEN RC_CHRGS ELSE NULL END),0) AS RC2_CHRGS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 3 THEN RC_CHRGS ELSE NULL END),0) AS RC3_CHRGS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 4 THEN RC_CHRGS ELSE NULL END),0) AS RC4_CHRGS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 5 THEN RC_CHRGS ELSE NULL END),0) AS RC5_CHRGS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 6 THEN RC_CHRGS ELSE NULL END),0) AS RC6_CHRGS
                                 ,COALESCE(MAX(CASE WHEN RC_SORT_ORD = 7 THEN RC_CHRGS ELSE NULL END),0) AS RC7_CHRGS
                                 
                            FROM RC_ST_YR_TOTALS DATA
                                        
                            GROUP BY ST_SORT_ORD, GEO_STATE_NAME, CAL_YEAR

                        )

                        ,RPT_DATA_FORMATTED  AS  (

                            SELECT ST.GEO_STATE_NAME, ST.ST_SORT_ORD, ST.CAL_YEAR
                                   ,ST.TOT_BILLS     AS TOT_BILLS
                                  
                                  ,ST.TOT_UNITS     AS TOT_UNITS
                                  ,RC1_UNITS        AS UNITS_055X
                                  ,RC2_UNITS        AS UNITS_042X
                                  ,RC3_UNITS        AS UNITS_044X
                                  ,RC4_UNITS        AS UNITS_043X
                                  ,RC5_UNITS        AS UNITS_056X
                                  ,RC6_UNITS        AS UNITS_057X
                                  ,RC7_UNITS        AS UNITS_OTHER
                                  
                                  ,ST.TOT_CHRGS     AS TOT_CHRGS
                                  ,RC1_CHRGS        AS CHRGS_055X
                                  ,RC2_CHRGS        AS CHRGS_042X
                                  ,RC3_CHRGS        AS CHRGS_044X
                                  ,RC4_CHRGS        AS CHRGS_043X
                                  ,RC5_CHRGS        AS CHRGS_056X
                                  ,RC6_CHRGS        AS CHRGS_057X
                                  ,RC7_CHRGS        AS CHRGS_OTHER	    

                                  ,COALESCE(ST.TOT_PMT_AMT,0)  AS TOT_PMT_AMT
                                 
                                  ,CASE WHEN COALESCE(ST.TOT_CHRGS,0) = 0
                                        THEN '0.00'
                                        ELSE TO_CHAR((COALESCE(ST.TOT_PMT_AMT,0) / ST.TOT_CHRGS) * 100,'999.00')
                                   END   AS PERC_TOT_CHRGS
                                   
                            FROM ST_YR_TOTALS ST

                            INNER JOIN RC_ST_YR_TOTALS_1_ROW RC
                            ON  ST.CAL_YEAR       = RC.CAL_YEAR
                            AND ST.GEO_STATE_NAME = RC.GEO_STATE_NAME

                        )

                        ,RPT_DATA_ALL_AREAS  AS  (

                            SELECT SUM_DATA.*
                                  ,TO_CHAR((TOT_PMT_AMT / TOT_CHRGS) * 100,'999.00') AS PERC_TOT_CHRGS
                            FROM (

                                SELECT 
                                      'ALL AREAS'    AS GEO_STATE_NAME
                                     ,'6'            AS ST_SORT_ORD   
                                     ,CAL_YEAR

                                     ,SUM(TOT_BILLS)   AS TOT_BILLS
                                       
                                     ,SUM(TOT_UNITS)   AS TOT_UNITS
                                     ,SUM(UNITS_055X)  AS UNITS_055X
                                     ,SUM(UNITS_042X)  AS UNITS_042X
                                     ,SUM(UNITS_044X)  AS UNITS_044X
                                     ,SUM(UNITS_043X)  AS UNITS_043X
                                     ,SUM(UNITS_056X)  AS UNITS_056X
                                     ,SUM(UNITS_057X)  AS UNITS_057X
                                     ,SUM(UNITS_OTHER) AS UNITS_OTHER
                                      
                                     ,SUM(TOT_CHRGS)   AS TOT_CHRGS
                                     ,SUM(CHRGS_055X)  AS CHRGS_055X
                                     ,SUM(CHRGS_042X)  AS CHRGS_042X
                                     ,SUM(CHRGS_044X)  AS CHRGS_044X
                                     ,SUM(CHRGS_043X)  AS CHRGS_043X
                                     ,SUM(CHRGS_056X)  AS CHRGS_056X
                                     ,SUM(CHRGS_057X)  AS CHRGS_057X
                                     ,SUM(CHRGS_OTHER) AS CHRGS_OTHER	  

                                     ,SUM(TOT_PMT_AMT) AS TOT_PMT_AMT
                                     
                                      
                                FROM RPT_DATA_FORMATTED RPT
                                GROUP BY CAL_YEAR
                            ) SUM_DATA
                            
                        )

                        ,RPT_DATA_USA  AS  (

                            SELECT SUM_DATA.*
                                  ,TO_CHAR((TOT_PMT_AMT / TOT_CHRGS) * 100,'999.00') AS PERC_TOT_CHRGS
                            FROM (
                            
                                SELECT 
                                      'UNITED STATES'   AS GEO_STATE_NAME
                                     ,'7'               AS ST_SORT_ORD  
                                     ,CAL_YEAR

                                     ,SUM(TOT_BILLS)   AS TOT_BILLS
                                       
                                     ,SUM(TOT_UNITS)   AS TOT_UNITS
                                     ,SUM(UNITS_055X)  AS UNITS_055X
                                     ,SUM(UNITS_042X)  AS UNITS_042X
                                     ,SUM(UNITS_044X)  AS UNITS_044X
                                     ,SUM(UNITS_043X)  AS UNITS_043X
                                     ,SUM(UNITS_056X)  AS UNITS_056X
                                     ,SUM(UNITS_057X)  AS UNITS_057X
                                     ,SUM(UNITS_OTHER) AS UNITS_OTHER
                                      
                                     ,SUM(TOT_CHRGS)   AS TOT_CHRGS
                                     ,SUM(CHRGS_055X)  AS CHRGS_055X
                                     ,SUM(CHRGS_042X)  AS CHRGS_042X
                                     ,SUM(CHRGS_044X)  AS CHRGS_044X
                                     ,SUM(CHRGS_043X)  AS CHRGS_043X
                                     ,SUM(CHRGS_056X)  AS CHRGS_056X
                                     ,SUM(CHRGS_057X)  AS CHRGS_057X
                                     ,SUM(CHRGS_OTHER) AS CHRGS_OTHER		  

                                     ,SUM(TOT_PMT_AMT) AS TOT_PMT_AMT
                                      
                                FROM RPT_DATA_FORMATTED RPT
                                WHERE NOT GEO_STATE_NAME IN ('FOREIGN COUNTRIES','VIRGIN ISLANDS', 'PUERTO RICO', 'GUAM', 'OTHER OUTLYING AREAS') 
                                GROUP BY CAL_YEAR
                            ) SUM_DATA	
                            
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

                        ORDER BY ST_SORT_ORD, GEO_STATE_NAME, CAL_YEAR
    
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