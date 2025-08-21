#!/usr/bin/env python
########################################################################################################
# Name:  blbtn_drug_ext.py
#
# Desc: Script to Extract drug data (IDR#BLB3)
#
# Created: Paul Baranoski  06/09/2022
# Modified: 
#
# Paul Baranoski 11/03/2022 Removed call to send Success email with Extract filename. Will
#                           do this from script instead.
# Paul Baranoski 07/19/2023 Change extract filename extension from .csv to .txt
# Paul Baranoski 07/20/2023 Modified table name. Prior table name was incorrect.
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

# boolean - Python Exception status
bPythonExceptionOccurred=False

########################################################################################################
# RUN
########################################################################################################
print('')
print("Run date and time: " + date_time  )
print('')

try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()   
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

   snowconvert_helpers.execute_sql_statement("""USE DATABASE IDRC_${ENVNAME}""", con,exit_on_error = True)
   
   #**************************************
   #   Extract Drug Provider data  
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_BLBTN_STG/blbtn_drug_ext_{TMSTMP}.txt.gz
                                                FROM (

                                WITH BLB_BTN_DRUG_DTL_INFO as (
                                  
                                    SELECT
                                         RPAD(PN.PROD_NDC_CD,11,' ')          AS PROD_NDC_CD
                                        ,RPAD(COALESCE(PN.PROD_NDC_PROD_NAME,' '),100,' ')  AS PROD_NDC_PROD_NAME
                                        ,RPAD(COALESCE(PN.PROD_NDC_BRAND_NAME,' '),30,' ')  AS PROD_NDC_BRAND_NAME
                                        ,to_char(PN.PROD_NDC_DRUG_OBSLT_DT,'MM/DD/YYYY')
                                                                                AS PROD_NDC_DRUG_OBSLT_DT
                                        ,RPAD(COALESCE(PN.PROD_NDC_AHFS_DESC,' '),40,' ')     AS PROD_NDC_AHFS_DESC
                                        ,RPAD(PN.PROD_NDC_DRUG_FORM_CD,1,' ')   AS PROD_NDC_DRUG_FORM_CD
                                        ,RPAD(COALESCE(PN.NDC_BRND_NAME_CD,' '),1,' ')        AS NDC_BRND_NAME_CD
                                        ,replace(to_char(COALESCE(PN.PROD_NDC_PKG_SIZE_AMT,0),'FM000000000.000'),'.','')
                                                                                AS PROD_NDC_PKG_SIZE_AMT
                                        ,repeat(' ',195)                        AS FILLER_FLD
                                    FROM BIA_{ENVNAME}.CMS_DIM_BEPSD_{ENVNAME}.PROD_NDC_EFCTV PN
                                  
                                )

                                ,DRUG_DTL_SINGLE_COL as (

                                      /* If any column is null, the entire string will be NULL --> must fix main query	*/
                                      SELECT '1' as SEQ_NUM
                                            ,PROD_NDC_CD 
                                            || PROD_NDC_PROD_NAME
                                            || PROD_NDC_BRAND_NAME
                                            || PROD_NDC_DRUG_OBSLT_DT
                                            || PROD_NDC_AHFS_DESC
                                            || PROD_NDC_DRUG_FORM_CD
                                            || NDC_BRND_NAME_CD
                                            || PROD_NDC_PKG_SIZE_AMT
                                            || FILLER_FLD 
                                          AS DTL_ROW
                                        FROM BLB_BTN_DRUG_DTL_INFO
                                )

                                ,HEADER_ROW as (

                                    SELECT '0' as SEQ_NUM 
                                          ,'HNDC'|| to_char(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS') || 'M' || to_char(CURRENT_DATE,'YYYYMM') ||'01000001' 
                                          || to_char(last_day(CURRENT_DATE),'YYYYMMDD') || '235959'
                                          || repeat(' ',353) as DTL_ROW
                                      FROM DUAL

                                )

                                ,NOF_DTL_ROWS as (
                                    
                                    SELECT COUNT(*) as TOT_RECS
                                    FROM BLB_BTN_DRUG_DTL_INFO 
                                )

                                ,TRAILER_ROW as (

                                      SELECT '2' as SEQ_NUM 
                                            ,'TNDC' || to_char(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS') 
                                                    || to_char(TOT_RECS,'FM0000000000')
                                                    || repeat(' ',372)
                                            as DTL_ROW
                                      FROM NOF_DTL_ROWS

                                )


                                SELECT DTL_ROW
                                FROM (
                                    SELECT *
                                    FROM HEADER_ROW
                                    UNION ALL
                                  
                                    SELECT *
                                    FROM DRUG_DTL_SINGLE_COL
                                    UNION ALL
                                  
                                    SELECT *
                                    FROM TRAILER_ROW

                                )  
                                ORDER BY SEQ_NUM  


                        ) 
                        FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=none )
                        SINGLE=TRUE  max_file_size=5368709120  """, con, exit_on_error=True)


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
