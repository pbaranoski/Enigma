#!/usr/bin/env python
########################################################################################################
# Name:  ST_TMP_LEO_PTB_TAB.py
#THIS SCRIPT LOADS STAGE TABLE FOR PART B CARRIER FILES.
#THIS EXTRACT RUNS 3 TIMES A YEAR.
# Desc: This script will insert data in to PST_TMP_LEO_PTB_TAB table in snowflake 
# Created: Suamthi Gayam  
# Modified: 06/10/2022
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

# boolean - Python Exception status
bPythonExceptionOccurred=False

########################################################################################################
# RUN
#######################################################################################################
print('')
print("Run date and time: " + date_time  )
print('')

try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)
   
   snowconvert_helpers.execute_sql_statement("""DELETE FROM "BIA_${ENVNAME}"."CMS_STAGE_XTR_${ENVNAME}".ST_TMP_LEO_PTB_TAB""", con, exit_on_error=True)
   
   snowconvert_helpers.execute_sql_statement("""INSERT INTO "BIA_${ENVNAME}"."CMS_STAGE_XTR_${ENVNAME}".ST_TMP_LEO_PTB_TAB (CLM_HIC_NUM,
GEO_BENE_SK ,
CLM_DT_SGNTR_SK ,
CLM_TYPE_CD ,
CLM_NUM_SK ,
CLM_LINE_NUM,
CLM_LINE_HCPCS_CD,
HCPCS_1_MDFR_CD,
HCPCS_2_MDFR_CD,
HCPCS_3_MDFR_CD,
HCPCS_4_MDFR_CD,
HCPCS_5_MDFR_CD,
CLM_LINE_FROM_DT,
CLM_LINE_ALOWD_CHRG_AMT,
CLM_LINE_SRVC_UNIT_QTY,
CLM_CNTL_NUM,
CLM_QUERY_CD,
CLM_DISP_CD,
CLM_FED_TYPE_SRVC_CD,
CLM_POS_CD,
CLM_RNDRG_FED_PRVDR_SPCLTY_CD,
GEO_ZIP5_CD,
CLM_RNDRG_PRVDR_NPI_NUM,
CLM_LINE_CVRD_PD_AMT,
CLM_CNTRCTR_NUM,
CLM_PRCNG_LCLTY_CD,
CLM_MTUS_IND_CD,
CLM_LINE_PRFNL_MTUS_CNT,
CLM_RNDRG_PRVDR_PRTCPTG_CD
)

SELECT DISTINCT
C.CLM_HIC_NUM,
CL.GEO_BENE_SK ,
CL.CLM_DT_SGNTR_SK ,
CL.CLM_TYPE_CD ,
CL.CLM_NUM_SK ,
CL.CLM_LINE_NUM,
CL.CLM_LINE_HCPCS_CD,
CL.HCPCS_1_MDFR_CD,
CL.HCPCS_2_MDFR_CD,
CL.HCPCS_3_MDFR_CD,
CL.HCPCS_4_MDFR_CD,
CL.HCPCS_5_MDFR_CD,
CL.CLM_LINE_FROM_DT,
CL.CLM_LINE_ALOWD_CHRG_AMT,
CL.CLM_LINE_SRVC_UNIT_QTY,
C.CLM_CNTL_NUM,
C.CLM_QUERY_CD,
C.CLM_DISP_CD,
CLP.CLM_FED_TYPE_SRVC_CD,
CL.CLM_POS_CD,
CL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,
ZIP5.GEO_ZIP5_CD,
CL.CLM_RNDRG_PRVDR_NPI_NUM,
CL.CLM_LINE_CVRD_PD_AMT,
CL.CLM_CNTRCTR_NUM,
CLP.CLM_PRCNG_LCLTY_CD,
CLP.CLM_MTUS_IND_CD,
CLP.CLM_LINE_PRFNL_MTUS_CNT,
CL.CLM_RNDRG_PRVDR_PRTCPTG_CD

FROM IDRC_${ENVNAME}.CMS_FCT_CLM_${ENVNAME}.CLM  C

INNER JOIN IDRC_${ENVNAME}.CMS_FCT_CLM_${ENVNAME}.CLM_DT_SGNTR D
    ON D.CLM_DT_SGNTR_SK              = C.CLM_DT_SGNTR_SK

INNER JOIN IDRC_${ENVNAME}.CMS_FCT_CLM_${ENVNAME}.CLM_LINE CL
    ON C.GEO_BENE_SK     = CL.GEO_BENE_SK
    AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
    AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
    AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
    AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

INNER JOIN IDRC_${ENVNAME}.CMS_DIM_GEO_${ENVNAME}.GEO_ZIP5_CD ZIP5
    ON ZIP5.GEO_SK = C.GEO_BENE_SK

INNER JOIN IDRC_${ENVNAME}.CMS_FCT_CLM_${ENVNAME}.CLM_LINE_PRFNL CLP
    ON      CL.GEO_BENE_SK = CLP.GEO_BENE_SK
    AND     CL.CLM_DT_SGNTR_SK = CLP.CLM_DT_SGNTR_SK
    AND     CL.CLM_TYPE_CD = CLP.CLM_TYPE_CD
    AND     CL.CLM_NUM_SK = CLP.CLM_NUM_SK
    AND     CL.CLM_LINE_NUM = CLP.CLM_LINE_NUM

WHERE
/* Part B CARRIER claims */
C.CLM_TYPE_CD IN (71,72) 
/* only select final action claims */
AND C.CLM_FINL_ACTN_IND='Y' 
/* Date of service */
AND CL.CLM_LINE_FROM_DT BETWEEN 
TRIM(EXTRACT(YEAR FROM CURRENT_DATE)-1)||'-01-01' 
AND TRIM(EXTRACT(YEAR FROM CURRENT_DATE)-1)||'-12-31'
 /* claims processing dates*/

/* Added 01-22-2020 Leo Porter Request CR45497*/ 
AND D.CLM_NCH_WKLY_PROC_DT BETWEEN TRIM(EXTRACT (YEAR FROM CURRENT_DATE)-1||'-01'||'-01')
(CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 3  THEN
TRIM(EXTRACT (YEAR FROM CURRENT_DATE)-1||'-12'||'-31')
WHEN (EXTRACT(MONTH FROM CURRENT_DATE) >= 3
and EXTRACT(MONTH FROM CURRENT_DATE) <= 6)  THEN
TRIM(EXTRACT(YEAR FROM CURRENT_DATE))||'-03-31'
ELSE TRIM(EXTRACT(YEAR FROM CURRENT_DATE))||'-06-30' END)
AND CL.CLM_LINE_ALOWD_CHRG_AMT>0 """, con, exit_on_error=True)

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
