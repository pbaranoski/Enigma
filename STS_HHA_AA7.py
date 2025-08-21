#!/usr/bin/env python
########################################################################################################
# Name:   STS_HHA_1007.py
#
# DESC:   This script extracts data for STS HHA table report - HHA Facilities payments, charges	  
#         by period expense (legacy AA4 report)
#
# Created: copied from Paul Baranoski STS HHA 
# Modified: 
#
# Nat.Tinovsky 2025-02-04 Created program.
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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STS_HHA_STG/STS_HHA_RPT_AA7_{EXT_TO_YYYY}_{RUN_PRD}_{TMSTMP}.csv.gz
                    FROM (

        WITH RPT_CLM_TYPE_CATEGORIES AS ( 
        
        SELECT  DISTINCT COALESCE(HHA_TYPE_CD,7) AS RPT_CLM_TYPE_CD
            ,COALESCE(HHA_TYPE_DESC,'OTHER FACILITIES') AS RPT_CLM_TYPE_DESC
            ,FF.PRVDR_NUM
        FROM  BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.STS_HOS_HHA_FF FF
        WHERE (SUBSTR(FF.PRVDR_NUM,3,4) BETWEEN '3100' AND '3199'
            OR SUBSTR(FF.PRVDR_NUM,3,4) BETWEEN '7000' AND '8499'
            OR SUBSTR(FF.PRVDR_NUM,3,4) BETWEEN '9000' AND '9799'
            )
        )
                
        -- Report requires 3 years of data
        ,RPT_YEARS AS (

            SELECT TO_CHAR( CAST( '{EXT_TO_DATE}' AS DATE )  ,'YYYY') AS CAL_YEAR FROM DUAL
            UNION
            SELECT TO_CHAR( DATEADD(YEAR,-1,  CAST( '{EXT_TO_DATE}' AS DATE )) ,'YYYY') AS CAL_YEAR FROM DUAL
            UNION
            SELECT TO_CHAR( DATEADD(YEAR,-2,  CAST( '{EXT_TO_DATE}' AS DATE )) ,'YYYY') AS CAL_YEAR FROM DUAL
                       
        )
        ,RPT_STATES    AS (

            SELECT DISTINCT GEO_SSA_STATE_CD
                , GEO_SSA_STATE_NAME AS GEO_STATE_NAME
                , '1'  AS ST_SORT_ORD, GEO_SSA_STATE_CD  AS ST_GROUP_CD
            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
            WHERE GEO_SSA_STATE_CD BETWEEN '01' AND '53'
                     
            UNION
                      
            SELECT DISTINCT GEO_SSA_STATE_CD
                , 'FOREIGN COUNTRIES' AS GEO_SSA_STATE_NAME
                , '2' AS ST_SORT_ORD, 200 AS ST_GROUP_CD
            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
            WHERE GEO_SSA_STATE_CD BETWEEN '54' AND '62' 
                      
            UNION
                      
            -- US Possessions, America Samoa, NORTHERN MARIANA, SAIPAN
            SELECT DISTINCT GEO_SSA_STATE_CD
                , 'OTHER OUTLYING AREAS' AS GEO_SSA_STATE_NAME
                , '3' AS ST_SORT_ORD, 300 AS ST_GROUP_CD
            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
            WHERE GEO_SSA_STATE_CD IN ('63','64','66','97') 

            UNION
                      
            -- Guam             
            SELECT GEO_SSA_STATE_CD
                , 'GUAM' AS GEO_SSA_STATE_NAME
                ,'4' AS ST_SORT_ORD, 400 AS ST_GROUP_CD
            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
            WHERE GEO_SSA_STATE_CD IN ('65','98')
                      
            UNION
                      
            -- Unknown (67 thru 96 missing from GEO_SSA_STATE_CD 
            -- Use logic to create only numeric values for ST_SORT_ORD,
            -- because it will be used in the sorting of report lines
            SELECT DISTINCT 
                CASE WHEN GEO_SSA_STATE_CD BETWEEN '67' AND '96' 
                    THEN GEO_SSA_STATE_CD ELSE '99' END AS GEO_SSA_STATE_CD
                , 'RESIDENCE UNKNOWN' AS GEO_SSA_STATE_NAME
                , '5' AS ST_SORT_ORD  , 99 AS ST_GROUP_CD 
            FROM IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD
            WHERE GEO_SSA_STATE_CD BETWEEN '67' AND '96'
                OR GEO_SSA_STATE_CD IN ('99','~ ', 'UK')
           
        )
        , RPT_CLM_TYPE_ALL AS (
            SELECT  DISTINCT  '0' AS RPT_CLM_TYPE_CD, 'TOTAL ALL FACILITIES' AS RPT_CLM_TYPE_DESC  
            FROM    RPT_CLM_TYPE_CATEGORIES
            UNION
            SELECT  DISTINCT  RPT_CLM_TYPE_CD, RPT_CLM_TYPE_DESC  
            FROM    RPT_CLM_TYPE_CATEGORIES
        )

        , RPT_STATE_ALL AS (
        
        SELECT  DISTINCT GEO_STATE_NAME, GEO_SSA_STATE_CD, ST_SORT_ORD, ST_GROUP_CD
        FROM    RPT_STATES
        UNION
        SELECT  DISTINCT 'ALL AREAS' AS GEO_STATE_NAME, 600 GEO_SSA_STATE_CD,  '6' AS ST_SORT_ORD, 600 AS ST_GROUP_ORD
        FROM    DUAL
        UNION
        SELECT  DISTINCT 'UNITED STATES' AS GEO_STATE_NAME, 700 GEO_SSA_STATE_CD, '7' AS ST_SORT_ORD,  700 AS  ST_GROUP_ORD
        FROM    DUAL
        )
        ,ALL_RPT_ROWS AS (

        SELECT  DISTINCT GEO_STATE_NAME,  ST_SORT_ORD, ST_GROUP_CD
                , CAL_YEAR  , RPT_CLM_TYPE_CD, RPT_CLM_TYPE_DESC 
        FROM    RPT_CLM_TYPE_ALL ,RPT_YEARS,RPT_STATE_ALL

        )

        ,PARTA_DTL_CLMS  AS (
        SELECT 
        DISTINCT  FF.PRVDR_NUM,
            C.GEO_BENE_SK ,C.CLM_DT_SGNTR_SK,C.CLM_TYPE_CD, C.CLM_NUM_SK
            ,COALESCE(ST.GEO_STATE_NAME, 'RESIDENCE UNKNOWN') AS GEO_STATE_NAME
            ,COALESCE(ST.ST_GROUP_CD,99) AS ST_GROUP_CD
            ,COALESCE(ST.ST_SORT_ORD ,'5')   AS ST_SORT_ORD 

            --Value for facilities that cannot be matched
            ,COALESCE(RPT_CLM_TYPE_DESC,'OTHER FACILITIES') AS RPT_CLM_TYPE_DESC
            ,RPT_CLM_TYPE_CD 
            ,RY.CAL_YEAR 
            ,C.CLM_QUERY_CD 
            ,CDN.CLM_NRLN_RIC_CD
                   --CLM_MDCR_INSTNL_TOT_CHRG_AMT
            ,CASE WHEN C.CLM_QUERY_CD = '0' THEN (COALESCE(CLM_MDCR_HHA_TOT_VISIT_CNT,0)  * -1)  
                      ELSE COALESCE(CLM_MDCR_HHA_TOT_VISIT_CNT,0) END  AS VISIT
            ,CASE WHEN C.CLM_QUERY_CD = '0' THEN -1 ELSE 1 END AS NOF_BILLS 
            ,CASE WHEN C.CLM_QUERY_CD = '0' THEN (COALESCE(C.CLM_SBMT_CHRG_AMT,0)  * -1)  
                      ELSE COALESCE(C.CLM_SBMT_CHRG_AMT,0) END  AS CHRG_AMT
            ,CASE WHEN C.CLM_QUERY_CD = '0' THEN (COALESCE(C.CLM_PMT_AMT,0) * -1)
                      ELSE COALESCE(C.CLM_PMT_AMT,0) END AS  PMT_AMT
            ,SUBSTR(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4) AS PROV4
            ,CASE WHEN C.GEO_BENE_SK = 0 then 0 ELSE 1 END GEO_BENE_SK_IND
            
        FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM  C
                                    
        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR  CDS
        ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_INSTNL  CI
        ON  C.GEO_BENE_SK     = CI.GEO_BENE_SK
        AND C.CLM_DT_SGNTR_SK = CI.CLM_DT_SGNTR_SK
        AND C.CLM_TYPE_CD     = CI.CLM_TYPE_CD
        AND C.CLM_NUM_SK      = CI.CLM_NUM_SK

        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN  CDN
            ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
            AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
            AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
            AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK
        INNER JOIN RPT_YEARS RY
            ON YEAR(C.CLM_THRU_DT)=RY.CAL_YEAR
            
        INNER JOIN RPT_CLM_TYPE_CATEGORIES FF
            ON C.CLM_BLG_PRVDR_OSCAR_NUM= FF.PRVDR_NUM

        LEFT OUTER JOIN RPT_STATES ST
            ON COALESCE(C.GEO_BENE_SSA_STATE_CD,'~ ') = ST.GEO_SSA_STATE_CD

        WHERE 
            -----Medicare status criteria
            CDN.CLM_CWF_BENE_MDCR_STUS_CD IN ('10','11','20','21','31','40' )
            AND CDN.CLM_NRLN_RIC_CD  in ( 'W', 'V','U')
            
            ---HHA claims criteria 
            AND C.CLM_TYPE_CD=10
            AND  C.CLM_QUERY_CD <> 'C'
            AND RPT_CLM_TYPE_CD IN ('1','2','3','4', '5', '6', '7')     

            --To exclude GEO_BENE_SK = 0 for RESIDENCE UNKNOWN,no impact on the other states or other categories
            AND (  COALESCE(ST.GEO_STATE_NAME,'RESIDENCE UNKNOWN') <> 'RESIDENCE UNKNOWN'
                    OR 
                   (    COALESCE(ST.GEO_STATE_NAME,'RESIDENCE UNKNOWN') = 'RESIDENCE UNKNOWN'
                        AND C.GEO_BENE_SK <> 0 
                     )
                )
        )

        ,W_STATE AS (

        --Counts for State , Facility
        SELECT  GEO_STATE_NAME, ST_GROUP_CD
            ,RPT_CLM_TYPE_DESC
            ,CAL_YEAR 
            ,SUM(NOF_BILLS) AS NOF_BILLS 
            ,SUM(VISIT ) VISIT
            ,SUM(CHRG_AMT)/1000  AS CHRG_AMT
            ,SUM(PMT_AMT)/1000  AS  PMT_AMT
            ,ST_SORT_ORD
            ,RPT_CLM_TYPE_CD
        FROM PARTA_DTL_CLMS C

        GROUP BY  GEO_STATE_NAME, ST_GROUP_CD
           ,RPT_CLM_TYPE_DESC
           ,RPT_CLM_TYPE_CD
            ,CAL_YEAR,  ST_SORT_ORD

        UNION
        --Total Counts for State  
        SELECT  GEO_STATE_NAME, ST_GROUP_CD
            ,'TOTAL ALL FACILITIES' AS RPT_CLM_TYPE_DESC
            ,CAL_YEAR 
            ,SUM(NOF_BILLS) AS NOF_BILLS 
            ,SUM(VISIT ) VISIT
            ,SUM(CHRG_AMT)/1000  AS CHRG_AMT
            ,SUM(PMT_AMT)/1000  AS  PMT_AMT
            ,ST_SORT_ORD
            ,0 RPT_CLM_TYPE_CD
        FROM PARTA_DTL_CLMS C

        GROUP BY  GEO_STATE_NAME, ST_GROUP_CD
         --  ,RPT_CLM_TYPE_DESC
         --  ,RPT_CLM_TYPE_CD
            ,CAL_YEAR,  ST_SORT_ORD
        )

        ,W_ALL_AREA as (

        SELECT  'ALL AREAS' AS GEO_STATE_NAME,600 AS ST_GROUP_CD
            ,RPT_CLM_TYPE_DESC
            ,CAL_YEAR 
            ,SUM(NOF_BILLS) AS NOF_BILLS 
            ,SUM(VISIT ) VISIT
            ,SUM(CHRG_AMT)  AS CHRG_AMT
            ,SUM(PMT_AMT)  AS  PMT_AMT
            ,'6' ST_SORT_ORD
            ,RPT_CLM_TYPE_CD
        FROM W_STATE C
        GROUP BY   CAL_YEAR
            ,RPT_CLM_TYPE_DESC
           ,RPT_CLM_TYPE_CD
        )
        
         ---Total counts for USA 
        ,W_UNITED as ( 

        SELECT  'UNITED STATES' AS GEO_STATE_NAME, 700 AS ST_GROUP_CD
            ,RPT_CLM_TYPE_DESC
            ,CAL_YEAR 
            ,SUM(NOF_BILLS) AS NOF_BILLS 
            ,SUM(VISIT ) VISIT
            ,SUM(CHRG_AMT)  AS CHRG_AMT
            ,SUM(PMT_AMT)  AS  PMT_AMT
            ,'7' AS ST_SORT_ORD
            ,RPT_CLM_TYPE_CD
        FROM W_STATE C
        WHERE ST_SORT_ORD =  '1'
        GROUP BY  
            RPT_CLM_TYPE_DESC
            ,RPT_CLM_TYPE_CD
            ,CAL_YEAR,  ST_SORT_ORD
            
         ) 
        ,ALL_RPT_DATA AS (
        SELECT * FROM W_STATE       
        UNION
        SELECT * FROM W_ALL_AREA
        UNION
        SELECT * FROM W_UNITED
         )
        ,W_RES as (
        SELECT  DISTINCT
             RPT.GEO_STATE_NAME
            , RPT.RPT_CLM_TYPE_DESC
            , RPT.CAL_YEAR 
            , COALESCE(C.NOF_BILLS,0) AS TOT_NOF_BILLS
            , COALESCE(C.VISIT,0) AS TOT_NOF_VISITS
            , ROUND(COALESCE(C.CHRG_AMT ,0),2) AS TOT_CHRG_AMT
            , ROUND( COALESCE(C.PMT_AMT,0),2) AS TOT_PMT_AMT
           ,row_number() over (order by RPT.ST_SORT_ORD, RPT.ST_GROUP_CD, RPT.RPT_CLM_TYPE_CD, SUBSTR(RPT.CAL_YEAR,4,1)) ord
        FROM ALL_RPT_ROWS  RPT
        LEFT OUTER JOIN ALL_RPT_DATA C
            ON  RPT.GEO_STATE_NAME  = C.GEO_STATE_NAME
            AND RPT.RPT_CLM_TYPE_CD = C.RPT_CLM_TYPE_CD
            AND RPT.CAL_YEAR        = C.CAL_YEAR
            AND RPT.ST_SORT_ORD     = C.ST_SORT_ORD    
            AND RPT.ST_GROUP_CD    = c.ST_GROUP_CD
        )
        select * exclude (ord)
        from W_RES 
        order by ord  
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
