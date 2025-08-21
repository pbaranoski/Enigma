#!/usr/bin/env python
########################################################################################################
# Name:  PAC_LTCH_RC.py
#
# Desc: Script to Extract Long Term Care Hospital(LTCH) Revenue Center Data
#
# Created: Viren Khanna  11/29/2022  
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
   #   Extract Long Term Care Hospital(LTCH) Revenue Center Data
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PAC_STG/PAC_LTCH_RC_Y{FYQ}_{TMSTMP}.csv.gz
                                                FROM (
SELECT   
DISTINCT    
TO_CHAR(C.CLM_THRU_DT::DATE,'YYYYMM  ') AS CLAIM_YEAR_MONTH,
LPAD(C.CLM_UNIQ_ID,16,' ' )AS CLAIM_UID,
RPAD(' ',6) AS SQNC_NUM,    
RPAD(CL.CLM_LINE_REV_CTR_CD,4),
TO_CHAR(CLI.CLM_LINE_INSTNL_REV_CTR_DT::DATE, 'YYYYMMDD') AS CLM_LINE_INSTNL_REV_CTR_DT,   
RPAD(CAS.CLM_1_REV_CNTR_ANSI_GRP_CD,2),   
RPAD(CAS.CLM_2_REV_CNTR_ANSI_GRP_CD,2),   
RPAD(CAS.CLM_3_REV_CNTR_ANSI_GRP_CD,2),    
RPAD(CAS.CLM_4_REV_CNTR_ANSI_GRP_CD,2),   
RPAD(CLI.CLM_REV_APC_HIPPS_CD,5),    
RPAD(CL.CLM_LINE_HCPCS_CD,5),   
RPAD(CL.HCPCS_1_MDFR_CD,2),   
RPAD(CL.HCPCS_2_MDFR_CD,2),   
RPAD(CL.HCPCS_3_MDFR_CD,2),   
RPAD(CL.HCPCS_4_MDFR_CD,2),   
RPAD(CL.HCPCS_5_MDFR_CD,2), 
RPAD(CLI.CLM_REV_PMT_MTHD_CD,2),   
RPAD(CLI.CLM_REV_DSCNT_IND_CD,1),   
RPAD(CLI.CLM_REV_PACKG_IND_CD,1),   
RPAD(CLI.CLM_REV_PRICNG_IND_CD,1),   
RPAD(CLI.CLM_OTAF_ONE_IND_CD,1),   
RPAD(CL.CLM_LINE_IDE_NUM,20),   
LPAD(to_char(CL.CLM_LINE_SRVC_UNIT_QTY,'FM9999990.0000'),12,' ') ,   
LPAD(TO_CHAR(CLI.CLM_LINE_INSTNL_RATE_AMT,'FM999999999999990.000'),19,' '),   
LPAD(TO_CHAR(CL.CLM_LINE_BLOOD_DDCTBL_AMT,'FM999999990.00'),12,' '),   
LPAD(TO_CHAR(CL.CLM_LINE_MDCR_DDCTBL_AMT,'FM999999990.00'),12,' '),    
LPAD(TO_CHAR(CLI.CLM_LINE_INSTNL_ADJSTD_AMT,'FM999999990.00'),12,' '),  
LPAD(TO_CHAR(CLI.CLM_LINE_INSTNL_RDCD_AMT,'FM999999990.00'),12,' '),  
LPAD(TO_CHAR(CLI.CLM_LINE_INSTNL_MSP1_PD_AMT,'FM999999990.00'),12,' '),    
LPAD(TO_CHAR(CLI.CLM_LINE_INSTNL_MSP2_PD_AMT,'FM999999990.00'),12,' '),   
LPAD(TO_CHAR(CL.CLM_LINE_PRVDR_PMT_AMT,'FM999999990.00'),12,' '),  
LPAD(TO_CHAR(CL.CLM_LINE_BENE_PD_AMT,'FM9999999999990.00'),16,' '), 
LPAD(TO_CHAR(CL.CLM_LINE_BENE_PMT_AMT,'FM999999990.00'),12,' '),
LPAD(TO_CHAR(CL.CLM_LINE_CVRD_PD_AMT,'FM999999990.00'),12,' '), 
LPAD(TO_CHAR(CL.CLM_LINE_SBMT_CHRG_AMT,'FM9999999999990.00'),16,' '),   
LPAD(TO_CHAR(CL.CLM_LINE_NCVRD_CHRG_AMT,'FM9999999999990.00'),16,' '), 
RPAD(CLI.CLM_DDCTBL_COINSRNC_CD,1),   
TO_CHAR(CDS.CLM_ACTV_CARE_FROM_DT::DATE, 'YYYYMMDD') AS CLM_ACTV_CARE_FROM_DT,   
RPAD(C.CLM_FINL_ACTN_IND,1),   
RPAD(CL.CLM_CNSLDTD_BLG_CD,1),   
LPAD(C.CLM_TYPE_CD,6,' '),   
RPAD(CLI.CLM_REV_CNTR_STUS_CD,2),   
RPAD(CLI.CLM_LINE_INSTNL_DUP_CLM_CHK_CD,2),   
RPAD(CL.CLM_LINE_NDC_QTY_QLFYR_CD,2),   
LPAD(TO_CHAR(CL.CLM_LINE_NDC_QTY,'FM999999999999990.000'),19,' '),   
RPAD(CL.CLM_RNDRG_PRVDR_NPI_NUM,10),   
RPAD(COALESCE(C.CLM_RNDRG_PRVDR_LAST_NAME,' '),60,' ') AS  CLM_RNDRG_PRVDR_LAST_NAME,  
RPAD(CL.CLM_LINE_DCMTN_CD,2),   
RPAD(CL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,2),  
RPAD(CL.CLM_LINE_FPS_MODEL_NUM,2),
RPAD(CLDN.CLM_LINE_FPS_RSN_CD,3),  
RPAD(CLDN.CLM_LINE_FPS_RMRK_CD,5),   
RPAD(CL.CLM_LINE_FPS_MSN_1_CD,5), 
RPAD(CL.CLM_LINE_FPS_MSN_2_CD,5), 
RPAD(CLDN.CLM_LINE_THRPY_CAP_IND_1_CD,1),  
RPAD(CLDN.CLM_LINE_THRPY_CAP_IND_2_CD,1),
RPAD(CLDN.CLM_LINE_THRPY_CAP_IND_3_CD,1), 
RPAD(CLDN.CLM_LINE_THRPY_CAP_IND_4_CD,1),
RPAD(CLDN.CLM_LINE_THRPY_CAP_IND_5_CD,1),   
RPAD(CDN.CLM_PTNT_TRTMT_AUTHRZTN_NUM,18),
RPAD(COALESCE(CDN.CLM_PA_UTN_NUM,' '),14),
RPAD(BENE.BENE_RP_SW,1,' ')  
FROM   
IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C   
INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL   
ON C.GEO_BENE_SK=CL.GEO_BENE_SK   
AND C.CLM_DT_SGNTR_SK=CL.CLM_DT_SGNTR_SK   
AND C.CLM_TYPE_CD = CL.CLM_TYPE_CD   
AND C.CLM_NUM_SK = CL.CLM_NUM_SK   
AND C.CLM_FROM_DT = CL.CLM_FROM_DT   
LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE BENE   
ON BENE.BENE_SK = C.BENE_SK   
INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS   
ON CDS.CLM_DT_SGNTR_SK = C.CLM_DT_SGNTR_SK   
LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN CDN   
ON C.GEO_BENE_SK=CDN.GEO_BENE_SK   
AND C.CLM_DT_SGNTR_SK=CDN.CLM_DT_SGNTR_SK   
AND C.CLM_TYPE_CD = CDN.CLM_TYPE_CD   
AND C.CLM_NUM_SK = CDN.CLM_NUM_SK   
LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_DCMTN CLDN    
ON CL.GEO_BENE_SK=CLDN.GEO_BENE_SK   
AND CL.CLM_DT_SGNTR_SK=CLDN.CLM_DT_SGNTR_SK   
AND CL.CLM_TYPE_CD = CLDN.CLM_TYPE_CD   
AND CL.CLM_NUM_SK = CLDN.CLM_NUM_SK   
AND CL.CLM_LINE_NUM = CLDN.CLM_LINE_NUM   
LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_INSTNL CLI   
ON CL.GEO_BENE_SK=CLI.GEO_BENE_SK   
AND CL.CLM_DT_SGNTR_SK=CLI.CLM_DT_SGNTR_SK   
AND CL.CLM_TYPE_CD = CLI.CLM_TYPE_CD   
AND CL.CLM_NUM_SK = CLI.CLM_NUM_SK   
AND CL.CLM_LINE_NUM = CLI.CLM_LINE_NUM   
LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_ANSI_SGNTR CAS    
ON CAS.CLM_ANSI_SGNTR_SK=CLI.CLM_ANSI_SGNTR_SK   
WHERE      
C.CLM_TYPE_CD=60      
AND (C.CLM_FROM_DT between
            dateadd('month',-10, date_trunc('month',CURRENT_DATE()))
             and 
LAST_DAY(ADD_MONTHS(CURRENT_DATE,-8)))
AND (C.CLM_THRU_DT between
            dateadd('month',-10, date_trunc('month',CURRENT_DATE()))
             and 
LAST_DAY(ADD_MONTHS(CURRENT_DATE,-8)))
AND (Substr(CL.CLM_FAC_PRVDR_OSCAR_NUM,3,4)) BETWEEN '2000' AND '2299'     
AND C.CLM_PMT_AMT >0      
AND C.CLM_FINL_ACTN_IND='Y'     
AND C.CLM_BILL_FAC_TYPE_CD IN ('1', '4')      
AND C.CLM_BILL_CLSFCTN_CD='1'     
ORDER BY CLAIM_YEAR_MONTH,CLAIM_UID
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
