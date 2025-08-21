#!/usr/bin/env python
########################################################################################################
# Name:  PAC_SNF_DGNS.py
#
# Desc: Script to Extract Skilled Nursing Facility(SNF) Diagnosis Code Data
#
# Created: Viren Khanna  11/22/2022  
#
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
FYQ=os.getenv('FYQ')


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
try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()   
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}  """, con,exit_on_error = True)
   
   #**************************************
   #   Extract Skilled Nursing Facility(SNF) Diagnosis Code Data
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PAC_STG/PAC_SNF_DGNS_Y{FYQ}_{TMSTMP}.csv.gz
                                                FROM (
SELECT DISTINCT    
--C.CLM_THRU_DT (FORMAT 'YYYYMM')(CHAR(8)) AS CLAIM_YEAR_MONTH,
TO_CHAR(C.CLM_THRU_DT::DATE,'YYYYMM  ') AS CLAIM_YEAR_MONTH,
LPAD(C.CLM_UNIQ_ID,16,' ' )AS CLAIM_UID,
RPAD(' ',6) AS SQNC_NUM,
RPAD (CP.CLM_DGNS_CD,7) AS CLM_DGNS_CD,  
RPAD(CP.CLM_PROD_TYPE_CD,1),  
RPAD(CP.CLM_DGNS_PRCDR_ICD_IND,1),
TO_CHAR(CDS.CLM_ACTV_CARE_FROM_DT::DATE, 'YYYYMMDD') AS CLM_ACTV_CARE_FROM_DT,  
RPAD(C.CLM_FINL_ACTN_IND,1),  
LPAD(C.CLM_TYPE_CD,6,' '),  
RPAD(CLM_POA_IND,1), 
RPAD(Trim(C.CLM_BLG_PRVDR_OSCAR_NUM),6)
FROM 
IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C  
LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS  
ON CDS.CLM_DT_SGNTR_SK = C.CLM_DT_SGNTR_SK  
INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PROD CP 
ON C.GEO_BENE_SK=CP.GEO_BENE_SK 
AND C.CLM_DT_SGNTR_SK=CP.CLM_DT_SGNTR_SK 
AND C.CLM_TYPE_CD = CP.CLM_TYPE_CD 
AND C.CLM_NUM_SK = CP.CLM_NUM_SK 
WHERE CP.CLM_DGNS_CD<>'~' AND  
C.CLM_TYPE_CD=20 
AND (C.CLM_FROM_DT between
            dateadd('month',-10, date_trunc('month',CURRENT_DATE()))
             and 
LAST_DAY(ADD_MONTHS(CURRENT_DATE,-8)))
AND (C.CLM_THRU_DT between
            dateadd('month',-10, date_trunc('month',CURRENT_DATE()))
             and 
LAST_DAY(ADD_MONTHS(CURRENT_DATE,-8)))
AND (Substr(C.CLM_BLG_PRVDR_OSCAR_NUM,3,4)) BETWEEN '5000' AND '6499' 
AND C.CLM_PMT_AMT > 0 
AND C.CLM_FINL_ACTN_IND='Y' 
AND C.CLM_BILL_FAC_TYPE_CD='2' 
AND C.CLM_BILL_CLSFCTN_CD='1' 
ORDER BY CLAIM_YEAR_MONTH,CLAIM_UID,CLM_ACTV_CARE_FROM_DT

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
