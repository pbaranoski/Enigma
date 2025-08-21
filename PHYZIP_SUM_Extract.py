#!/usr/bin/env python
########################################################################################################
# Name:   PHYZIP_SUM_Extracts.py
#
# DESC:   This script extracts PHYZIP data to replace legacy Mainframe data extract.
#         Sumathi requested to remove FINAL ACTION IND filters.
#
#   32 s --> 7,729,275 rows - No Excel spreadsheet --> All Zips	
#   12 s --> 285 rows       - One Zip Code '29202'	
#
# Created: Paul Baranoski 4/04/2025
# Modified: 
#
# Paul Baranoski 2025-04-04 Created program.
# Paul Baranoski 2025-04-18 Modified S3 Extract Filename. Added FILE_LIT.
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

FILE_LIT=os.getenv('FILE_LIT')
EXT_FROM_DT=os.getenv('EXT_FROM_DT')
EXT_TO_DT=os.getenv('EXT_TO_DT')


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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PHYZIP_STG/PHYZIP_SUM{FILE_LIT}_{TMSTMP}.txt.gz
                    FROM (


                WITH CLM_DTL_INFO AS (

                    SELECT   C.GEO_BENE_SK
                            ,C.CLM_DT_SGNTR_SK
                            ,C.CLM_TYPE_CD
                            ,C.CLM_NUM_SK

                            ,EXTRACT(YEAR FROM CL.CLM_LINE_THRU_DT) AS LINE_LAST_EXPENSE_DT_YYYY
                            ,CL.CLM_LINE_FROM_DT
                            ,CL.CLM_LINE_THRU_DT
                            ,CDS.CLM_NCH_WKLY_PROC_DT
                            
                            ,RPAD(REPLACE(COALESCE(CP.CLM_CARR_PMT_DNL_CD,''),'~',''),2,' ') AS CLM_CARR_PMT_DNL_CD
                            ,CLP.CLM_PRCSG_IND_CD

                            ,CL.CLM_RNDRG_PRVDR_ZIP5_CD
                            ,CL.CLM_CNTRCTR_NUM
                            ,CLP.CLM_PRCNG_LCLTY_CD
                            ,CL.CLM_LINE_HCPCS_CD
                            ,CL.CLM_POS_CD
                            ,CL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD

                            ,CASE WHEN CLP.CLM_FED_TYPE_SRVC_CD = '8'   
                                THEN '80'
                                WHEN CL.HCPCS_1_MDFR_CD IN ('81','82','AS') 
                                THEN '80'
                                WHEN CL.HCPCS_2_MDFR_CD IN ('81','82','AS') 
                                THEN '80'
                                WHEN CL.HCPCS_1_MDFR_CD = '26' and CL.HCPCS_2_MDFR_CD = 'TC'
                                THEN '  '		  
                                WHEN CL.HCPCS_1_MDFR_CD = 'TC' and CL.HCPCS_2_MDFR_CD = '26'
                                THEN '  '	
                                WHEN CL.HCPCS_1_MDFR_CD IN ('22', '26', '50', '51', '52', '53', '54', '55', '56', '62',  '66', '80', 'TC' ) 
                                THEN CL.HCPCS_1_MDFR_CD
                                WHEN CL.HCPCS_2_MDFR_CD IN ('22', '26', '50', '51', '52', '53', '54', '55', '56', '62',  '66', '80', 'TC' ) 
                                THEN CL.HCPCS_2_MDFR_CD
                                ELSE '  '
                            END AS INIT_MOD 	
                           
                            ,CL.CLM_LINE_SRVC_UNIT_QTY
                            ,CL.CLM_LINE_ALOWD_CHRG_AMT 
                            ,CL.CLM_LINE_CVRD_PD_AMT 
                    
                         
                    FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
                    ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK
                          
                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PRFNL CP
                    ON  C.GEO_BENE_SK     = CP.GEO_BENE_SK
                    AND C.CLM_DT_SGNTR_SK = CP.CLM_DT_SGNTR_SK
                    AND C.CLM_TYPE_CD     = CP.CLM_TYPE_CD
                    AND C.CLM_NUM_SK      = CP.CLM_NUM_SK
                    
                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL 
                    ON C.GEO_BENE_SK = CL.GEO_BENE_SK
                    AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                    AND C.CLM_TYPE_CD = CL.CLM_TYPE_CD
                    AND C.CLM_NUM_SK = CL.CLM_NUM_SK
                    AND C.CLM_FROM_DT = CL.CLM_FROM_DT
                    
                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_PRFNL CLP 
                    ON CL.GEO_BENE_SK = CLP.GEO_BENE_SK
                    AND CL.CLM_DT_SGNTR_SK = CLP.CLM_DT_SGNTR_SK
                    AND CL.CLM_TYPE_CD = CLP.CLM_TYPE_CD
                    AND CL.CLM_NUM_SK = CLP.CLM_NUM_SK
                    AND CL.CLM_LINE_NUM = CLP.CLM_LINE_NUM
                    
                    WHERE C.CLM_TYPE_CD IN (71,72) 
                      --AND CLM_NCH_WKLY_PROC_DT Between '2025-02-01' AND '2025-02-28'   
                      AND CLM_NCH_WKLY_PROC_DT Between TO_DATE('{EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('{EXT_TO_DT}','YYYY-MM-DD')
                      

                      -- Remove FINAL ACTION per Sumathi
                      --AND C.CLM_FINL_ACTN_IND = 'Y' 
                      --AND CL.CLM_LINE_FINL_ACTN_IND = 'Y' 

                      AND CLP.CLM_PRCSG_IND_CD IN ('A','R','S')
                      
                      AND CL.CLM_LINE_ALOWD_CHRG_AMT > 0
                      AND CLP.CLM_FED_TYPE_SRVC_CD NOT IN ('F','7')
                      
                      AND (UPPER(SUBSTR(CL.CLM_LINE_HCPCS_CD,1,5)) NOT IN ('B','E','L','W','X','Y','Z'))
                      AND NOT CL.CLM_LINE_HCPCS_CD BETWEEN '00099' AND '02000'

                  
                )

                  SELECT DTL.LINE_LAST_EXPENSE_DT_YYYY
                        ,DTL.CLM_RNDRG_PRVDR_ZIP5_CD
                        ,DTL.CLM_CNTRCTR_NUM
                        ,DTL.CLM_PRCNG_LCLTY_CD
                        ,DTL.CLM_LINE_HCPCS_CD
                        ,DTL.INIT_MOD
                        ,DTL.CLM_POS_CD
                        ,DTL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD
                    
                        ,TO_CHAR(SUM(CLM_LINE_SRVC_UNIT_QTY),'S0000000.000')      AS TOTAL_SERVICE_UNITS
                        ,TO_CHAR(SUM(CLM_LINE_ALOWD_CHRG_AMT),'S00000000000.00')  AS TOTAL_ALLOWED_CHARGES
                        ,TO_CHAR(SUM(CLM_LINE_CVRD_PD_AMT),'S00000000000.00')     AS TOTAL_COVERED_PAID_AMT

                    FROM CLM_DTL_INFO DTL

                    GROUP BY  DTL.LINE_LAST_EXPENSE_DT_YYYY
                             ,CLM_RNDRG_PRVDR_ZIP5_CD
                             ,CLM_CNTRCTR_NUM
                             ,CLM_PRCNG_LCLTY_CD
                             ,CLM_LINE_HCPCS_CD
                             ,INIT_MOD
                             ,CLM_POS_CD
                             ,CLM_RNDRG_FED_PRVDR_SPCLTY_CD
    
 )
            FILE_FORMAT = (TYPE = CSV field_delimiter=NONE  ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY = none )
            SINGLE=TRUE   max_file_size=5368709120  """, con, exit_on_error=True)

    
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