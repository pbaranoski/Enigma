#!/usr/bin/env python
########################################################################################################
# Name:   PartB_Carrier_v2.py
# DESC:   This script creates Part B Carrier files
# Created: Vijayendra Mandavilli  
# Modified: 4/30/2026
#
# Modified:
#
########################################################################################################
# IMPORTS
########################################################################################################
import os
import sys
import datetime
from datetime import datetime
# import sendEmail

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

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')

QSTART_DATE=os.environ["QSTART_DATE"] 
QEND_DATE=os.environ["QEND_DATE"]
EXT_TYPE=os.environ["EXT_TYPE"]
XTR_FILE_NAME=os.environ["XTR_FILE_NAME"]

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


   ########################################################################################################
   # Method to execute the extract SQL using parameters Start Date, End Date, Year, Quarter Number, 
   ########################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PTBCARRIER_STG/{XTR_FILE_NAME}	
      
			FROM (
				SELECT

                -- CLM_HIC_NUM
                RPAD(CLM_HIC_NUM,20,' ')  AS  HIC_NUM,
                -- HCPCS
                RPAD(CLM_LINE_HCPCS_CD,5,' ') AS HCPCS,
                -- MODIFIERS
                RPAD(HCPCS_1_MDFR_CD,2,' ') AS MODIFIER_1,
                RPAD(HCPCS_2_MDFR_CD,2,' ') AS MODIFIER_2,
                RPAD(HCPCS_3_MDFR_CD,2,' ') AS MODIFIER_3,
                RPAD(HCPCS_4_MDFR_CD,2,' ') AS MODIFIER_4,
                RPAD(HCPCS_5_MDFR_CD,2,' ') AS MODIFIER_5,

                -- FIRST EXPENSE DATE
                --CLM_LINE_FROM_DT(DATE, FORMAT 'YYYYMMDD')(CHAR(8))AS FIRST_EXPENSE_DATE,
                TO_CHAR(CLM_LINE_FROM_DT::DATE, 'YYYYMMDD') AS FIRST_EXPENSE_DATE,
                --to_char(CL.CLM_LINE_FROM_DT,'YYYYMMDD') (CHAR(8))AS FIRST_EXPENSE_DATE1,
                -- ALLOWED CHARGE
                --CLM_LINE_ALOWD_CHRG_AMT (FORMAT '9(15).9(2)')(CHAR(18))AS ALLOWED_CHARG,
                to_char(CLM_LINE_ALOWD_CHRG_AMT,'s00000000000000.00') AS  ALLOWED_CHARG,
                -- ALLOWED SERVICE
                to_char(CLM_LINE_SRVC_UNIT_QTY,'s000000000000000000.0000') AS  ALLOWED_SERVIC,
                --CLM_LINE_SRVC_UNIT_QTY(FORMAT '-9(18).9(4)')(CHAR(24))AS ALLOWED_SERVIC,
                -- HIC CLAIM CONTROL NUMBER
                RPAD(CLM_CNTL_NUM,40,' ') AS HIC_CLAIM_CONTROL_NUMBER,
                -- CLAIM ENTRY CODE
                RPAD(CLM_QUERY_CD,1,' ') AS CLAIM_ENTRY_CODE,
                -- CLAIM DISPOSITION CODE
                RPAD(CLM_DISP_CD,2,' ') AS CLAIM_DISPOSITION_CODE,
                -- TYPE OF SERVICE
                RPAD(CLM_FED_TYPE_SRVC_CD,1,' ') AS TYPE_OF_SERVICE,
                -- PLACE OF SERVICE
                RPAD(CLM_POS_CD,2,' ') AS PLACE_OF_SERVICE,
                -- SPECIALTY
                RPAD(CLM_RNDRG_FED_PRVDR_SPCLTY_CD,2,' ')AS SPECIALTY,
                -- PROVIDER ZIP CODE
                RPAD(GEO_ZIP5_CD,5,' ') AS PROVIDER_ZIP,
                -- NPI
                RPAD(CLM_RNDRG_PRVDR_NPI_NUM,10,' ') AS NPI,
                -- PAYMENT AMOUNT
                to_char(CLM_LINE_CVRD_PD_AMT,'s0000000000.00') AS  PAYMENT_AMOUNT,
                --CLM_LINE_CVRD_PD_AMT (FORMAT '9(11).9(2)')(CHAR(14))AS PAYMENT_AMOUNT,
                RPAD(CLM_CNTRCTR_NUM,5,' ') AS CLM_CNTRCTR_NUM,
                RPAD(CLM_PRCNG_LCLTY_CD,2,' ') AS CLM_PRCNG_LCLTY_CD,
                RPAD(CLM_MTUS_IND_CD,1,' ') AS CLM_MTUS_IND_CD,
                to_char(CLM_LINE_PRFNL_MTUS_CNT, 's000000000.000') AS CLM_LINE_PRFNL_MTUS_CNT,
                RPAD(CLM_RNDRG_PRVDR_PRTCPTG_CD,1,' ') AS CLM_RNDRG_PRVDR_PRTCPTG_CD

                FROM BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.ST_TMP_LEO_PTB_TAB

                WHERE

                CLM_LINE_FROM_DT BETWEEN
               '{QSTART_DATE}' AND '{QEND_DATE}'
                --TRIM(EXTRACT(YEAR FROM CURRENT_DATE) -1)||'{QSTART_DATE}' AND TRIM(EXTRACT(YEAR FROM CURRENT_DATE) -1)||'{QEND_DATE}'
	   
									
			)							
         FILE_FORMAT = (TYPE = CSV field_delimiter = none ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=none )
            OVERWRITE=TRUE  max_file_size=5368709120 """, con, exit_on_error=True)


   snowconvert_helpers.configure_log()
   


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

