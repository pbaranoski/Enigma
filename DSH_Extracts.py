#!/usr/bin/env python
########################################################################################################
# Name:   DSH_Extracts.py
# DESC:   This script extracts into DSH_EDX_STAY table.
#
# Created: Paul Baranoski 4/25/2024
# Modified: 
#
# Paul Baranoski 2024-04-25 Created program.
#
# Paul Baranoski 2024-07-19 Added code to SQL to format DDD field to have leading zeroes.
# Paul Baranoski 2024-08-05 Modified SQL. When getting days for DSCHRG_DT_DD for January, the ADMIT_DT_DD
#                           was coded instead.
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
FF_TMSTMP=os.getenv('FF_TMSTMP')
PRVDR_ID=os.getenv('PRVDR_ID')
FROM_FY=os.getenv('FROM_FY')
TO_FY=os.getenv('TO_FY')
FF_ID_NODE=os.getenv('FF_ID_NODE')

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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DSH_STG/DSH_EXTRACT_{FF_ID_NODE}_{PRVDR_ID}_{FROM_FY}_{TO_FY}_{FF_TMSTMP}.csv.gz
                    FROM (

SELECT HICN
    ,ADMIT_DT_YYYY||TO_CHAR(ADMIT_DT_DDD,'FM000')   AS Admit_Date
    ,DSCHRG_DT_YYYY||TO_CHAR(DSCHRG_DT_DDD,'FM000') AS Discharge_Date
    ,PRVDR_ID       AS Provider_ID
    ,SPCL_UNIT_CD   AS Special_Unit_CD
    ,LENGTH_OF_STAY AS Length_Of_Stay
    ,PRE_RLNG_SSI_DAYS AS Pre_Ruling_SSI_Days
    
    ,CVRD_SSI_UNDER_RULING_MATCH AS Covered_SSI_Under_Ruling_Match
    
    ,MA_STUS             AS MA_Status
    ,POST_RLNG_SSI_DAYS  AS Post_Ruling_SSI_Days 
    ,FED_FY              AS Fiscal_Year
    ,MEDPAR_VSN          AS Medpar_Source_Version
    ,UTLZTN_DAYS         AS Utilization_Days
    ,MBI_ID              AS MBI_ID

FROM (
   SELECT HICN
               ,TO_CHAR(ADM_DT,'YYYY-MM-DD') 
               ,ADMIT_DT_YYYY
               ,CASE WHEN ADMIT_DT_MM = 1  THEN ADMIT_DT_DD
                     WHEN ADMIT_DT_MM = 2  THEN (31  + ADMIT_DT_DD)
                     WHEN ADMIT_DT_MM = 3  THEN (59  + ADMIT_DT_DD + ADMIT_FEB_29_DAY) 
                     WHEN ADMIT_DT_MM = 4  THEN (90  + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 5  THEN (120 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 6  THEN (151 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 7  THEN (181 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 8  THEN (212 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 9  THEN (243 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 10 THEN (273 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 11 THEN (304 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                     WHEN ADMIT_DT_MM = 12 THEN (334 + ADMIT_DT_DD + ADMIT_FEB_29_DAY)
                      END AS ADMIT_DT_DDD  
                
                ,TO_CHAR(DSCHRG_DT,'YYYY-MM-DD') 
                ,DSCHRG_DT_YYYY
               ,CASE WHEN DSCHRG_DT_MM = 1  THEN DSCHRG_DT_DD
                     WHEN DSCHRG_DT_MM = 2  THEN (31  + DSCHRG_DT_DD)
                     WHEN DSCHRG_DT_MM = 3  THEN (59  + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY) 
                     WHEN DSCHRG_DT_MM = 4  THEN (90  + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 5  THEN (120 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 6  THEN (151 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 7  THEN (181 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 8  THEN (212 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 9  THEN (243 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 10 THEN (273 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 11 THEN (304 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                     WHEN DSCHRG_DT_MM = 12 THEN (334 + DSCHRG_DT_DD + DSCHRG_FEB_29_DAY)
                      END AS DSCHRG_DT_DDD        

               ,PRVDR_ID
               ,SPCL_UNIT_CD
               ,LENGTH_OF_STAY
               ,PRE_RLNG_SSI_DAYS

               ,CVRD_SSI_UNDER_RULING_MATCH

               ,MA_STUS
               ,POST_RLNG_SSI_DAYS 
               ,FED_FY
               ,MEDPAR_VSN
               ,UTLZTN_DAYS
               ,MBI_ID
                
        FROM (

             
              SELECT HICN 
                    ,ADM_DT
                    ,TO_CHAR(ADM_DT,'YYYY')    AS ADMIT_DT_YYYY
                    ,DATE_PART(month,ADM_DT)   AS ADMIT_DT_MM
                    ,DATE_PART(day,ADM_DT)     AS ADMIT_DT_DD
                    ,CASE WHEN (DATE_PART(year,ADM_DT) % 4) = 0 THEN 1 ELSE 0 END AS ADMIT_FEB_29_DAY
                    
                    ,DSCHRG_DT
                    ,TO_CHAR(DSCHRG_DT,'YYYY')    AS DSCHRG_DT_YYYY
                    ,DATE_PART(month,DSCHRG_DT)   AS DSCHRG_DT_MM
                    ,DATE_PART(day,DSCHRG_DT)     AS DSCHRG_DT_DD
                    ,CASE WHEN (DATE_PART(year,DSCHRG_DT) % 4) = 0 THEN 1 ELSE 0 END AS DSCHRG_FEB_29_DAY
                    
                    ,S.PRVDR_ID
                    ,SPCL_UNIT_CD
                    ,LENGTH_OF_STAY
                    ,PRE_RLNG_SSI_DAYS

                    -- Covered SSI under Ruling Match
                    ,(CASE WHEN S.FED_FY BETWEEN 1988 AND 2004
                           THEN
                               CASE WHEN S.POST_RLNG_SSI_DAYS < S.UTLZTN_DAYS
                                    THEN S.POST_RLNG_SSI_DAYS
                                    ELSE S.UTLZTN_DAYS
                               END
                            ELSE 0
                            END) AS CVRD_SSI_UNDER_RULING_MATCH

                    ,MA_STUS
                    ,POST_RLNG_SSI_DAYS 
                    ,FED_FY
                    ,MEDPAR_VSN
                    ,UTLZTN_DAYS
                    ,MBI_ID


              FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DSH_EDX_STAY  S
              
              --INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DSH_EDX_PRVDR P 
              --ON S.PRVDR_ID = P.PRVDR_ID
              
              WHERE S.PRVDR_ID = '{PRVDR_ID}' 
                AND S.FED_FY BETWEEN '{FROM_FY}' AND '{TO_FY}' 
                AND S.MEDPAR_VSN = CASE WHEN S.FED_FY IN ('2004','2005','2006') AND S.PRVDR_ID IS NOT NULL THEN 'O'
                                        ELSE 'R' END 

            )                                
     )                           
    
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