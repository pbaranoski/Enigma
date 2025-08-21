#!/usr/bin/env python
########################################################################################################
# Name:  STS_HOS_Facility_Rpt.py
#        
# DESC:   This script extracts data for STS HOS Facility table report - STS HOS FACILITY PMT/CHRGS	  
#         by period expense (legacy AA6 report)
#
# Created: Viren Khanna  02/10/2025
# Modified: 
#
# Viren Khanna 2025-02-10 Create scripts.
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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_STS_HOS_FACILITY_STG/STS_HOS_FACILITY_RPT_AA6_{EXT_TO_YYYY}_{RUN_PRD}_{TMSTMP}.csv.gz
                    FROM (

                   WITH RPT_CLM_TYPE_CATEGORIES AS (
    
    SELECT '1' AS RPT_CLM_TYPE_CD, 'VISITING NURSE ASSOCIATION' AS RPT_CLM_TYPE_DESC, '1' AS SORT_ORD_IND FROM DUAL
    UNION
    SELECT '2' AS RPT_CLM_TYPE_CD, 'COMBINATION GOVT & VOL AGENCY'  AS RPT_CLM_TYPE_DESC, '2' AS SORT_ORD_IND FROM DUAL
    UNION
    SELECT '3' AS RPT_CLM_TYPE_CD, 'OFFICIAL HEALTH'      AS RPT_CLM_TYPE_DESC, '3' AS SORT_ORD_IND FROM DUAL
    UNION
    SELECT '4' AS RPT_CLM_TYPE_CD, 'REHAB FACILITY BASED'  AS RPT_CLM_TYPE_DESC, '4' AS SORT_ORD_IND FROM DUAL
    UNION
    SELECT '5' AS RPT_CLM_TYPE_CD, 'HOSPITAL BASED'      AS RPT_CLM_TYPE_DESC, '5' AS SORT_ORD_IND FROM DUAL
    UNION
    SELECT '6' AS RPT_CLM_TYPE_CD, 'SNF BASED'  AS RPT_CLM_TYPE_DESC, '6' AS SORT_ORD_IND FROM DUAL
    UNION
    SELECT '7' AS RPT_CLM_TYPE_CD, 'OTHER FACILITIES'      AS RPT_CLM_TYPE_DESC, '7' AS SORT_ORD_IND FROM DUAL
        
)


,RPT_YEARS AS (

    -- Need 4 years
                    SELECT {EXT_TO_YYYY} - 3 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 2 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY} - 1 AS CAL_YEAR FROM DUAL
                    UNION
                    SELECT {EXT_TO_YYYY}     AS CAL_YEAR FROM DUAL

              
)

,ALL_RPT_ROWS AS (

	SELECT DISTINCT CAL_YEAR,  RPT_CLM_TYPE_CD, RPT_CLM_TYPE_DESC, SORT_ORD_IND
	  FROM RPT_CLM_TYPE_CATEGORIES
		  ,RPT_YEARS 
		 

)


,PARTA_DTL_CLMS  AS (

            SELECT C.GEO_BENE_SK
                  ,C.CLM_DT_SGNTR_SK
                  ,C.CLM_TYPE_CD
                  ,C.CLM_NUM_SK
                  ,TO_CHAR(C.CLM_THRU_DT,'YYYY') AS CAL_YEAR                      
                  --*********************************************************** 
                  -- If Cancel claim --> set amt to negative (back-out) ELSE use amt
                  --***********************************************************
                 ,C.CLM_QUERY_CD
                 
                 ,CASE WHEN C.CLM_QUERY_CD = '0'  
                     THEN C.CLM_PMT_AMT * -1
                     ELSE C.CLM_PMT_AMT            
                 END AS  PMT_AMT

                ,CASE WHEN C.CLM_QUERY_CD = '0' 
                      THEN (COALESCE(C.CLM_SBMT_CHRG_AMT,0)  * -1)  
                      ELSE COALESCE(C.CLM_SBMT_CHRG_AMT,0)   
                  END  AS CHRG_AMT

		                  

                  ,C.CLM_BLG_PRVDR_OSCAR_NUM
                  , CASE WHEN FF.HHA_TYPE_CD = '1' THEN '1'
                  WHEN FF.HHA_TYPE_CD = '2' THEN '2'
                  WHEN FF.HHA_TYPE_CD = '3' THEN '3' 
                  WHEN FF.HHA_TYPE_CD = '4' THEN '4'
                  WHEN FF.HHA_TYPE_CD = '5' THEN '5'
                  WHEN FF.HHA_TYPE_CD = '6' THEN '6' 
                  WHEN FF.HHA_TYPE_CD = '7' THEN '7' 
                ELSE '8' END AS RPT_CLM_TYPE_CD 
                
                             

            FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM    C

            INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.STS_HOS_HHA_FF FF
            ON C.CLM_BLG_PRVDR_OSCAR_NUM= FF.PRVDR_NUM
            
        

          
            
            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN  CDN
            ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
            AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
            AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
            AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK

          
                      
            WHERE C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_FROM_YYYY}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DATE}','YYYY-MM-DD')
          
            AND CDN.CLM_CWF_BENE_MDCR_STUS_CD IN ('10','11','20','21','31','40' )

            AND  C.CLM_QUERY_CD <> 'C'
            AND C.CLM_TYPE_CD = '10'
            
           AND RPT_CLM_TYPE_CD IN ('1','2','3','4','5','6','7') 
           
and (substr(FF.PRVDR_NUM,3,4) between '9000' and '9800' 
or substr(FF.PRVDR_NUM,3,4) between '7000' and '8499'
or substr(FF.PRVDR_NUM,3,4) between '3100' and '3199')
       

)
  
,PARTA_SUM_DATA_BY_CLM_TYPE AS (

    SELECT CAL_YEAR, RPT_CLM_TYPE_CD, 
	       ROUND(SUM(PMT_AMT),0) AS PMT_AMT, ROUND(SUM(CHRG_AMT),0) AS CHRG_AMT
	FROM PARTA_DTL_CLMS
    GROUP BY CAL_YEAR, RPT_CLM_TYPE_CD
 
)



,ALL_RPT_DATA AS (

	SELECT RPT.*, COALESCE(C.PMT_AMT,0) AS PMT_AMT, COALESCE(C.CHRG_AMT,0) AS CHRG_AMT
	FROM PARTA_SUM_DATA_BY_CLM_TYPE C
	
	RIGHT OUTER JOIN ALL_RPT_ROWS RPT		
	ON  RPT.CAL_YEAR        = C.CAL_YEAR
	AND RPT.RPT_CLM_TYPE_CD = C.RPT_CLM_TYPE_CD
	

)



SELECT CAL_YEAR

     
     ,SUM(CHRG_AMT)  AS TOT_CHRG_AMT
     ,SUM(PMT_AMT)   AS TOT_PMT_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '1' THEN CHRG_AMT  ELSE 0 END) AS VISITING_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '1' THEN PMT_AMT   ELSE 0 END) AS VISITING_PMT_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '2' THEN CHRG_AMT  ELSE 0 END) AS COMBINATION_GOVT_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '2' THEN PMT_AMT   ELSE 0 END) AS COMBINATION_GOVT_PMT_AMT
    
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '3' THEN CHRG_AMT  ELSE 0 END) AS OFFICIAL_HEALTH_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '3' THEN PMT_AMT   ELSE 0 END) AS OFFICIAL_HEALTH_PMT_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '4' THEN CHRG_AMT  ELSE 0 END) AS REHAB_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '4' THEN PMT_AMT   ELSE 0 END) AS REHAB_PMT_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '5' THEN CHRG_AMT  ELSE 0 END) AS HOSPITAL_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '5' THEN PMT_AMT   ELSE 0 END) AS HOSPITAL_PMT_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '6' THEN CHRG_AMT  ELSE 0 END) AS SNF_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '6' THEN PMT_AMT   ELSE 0 END) AS SNF_PMT_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '7' THEN CHRG_AMT  ELSE 0 END) AS OTHER_CHRG_AMT
     ,SUM(CASE WHEN RPT_CLM_TYPE_CD = '7' THEN PMT_AMT   ELSE 0 END) AS OTHER_PMT_AMT
  

FROM ALL_RPT_DATA

GROUP BY CAL_YEAR
ORDER BY CAL_YEAR

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