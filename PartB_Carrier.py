#!/usr/bin/env python
########################################################################################################
# Name:   PartB_Carrier.py
# DESC:   This script creates Part B Carrier files
# Created: Sumathi Gayam  
# Modified: 06/13/2022
#
# Paul Baranoski 9/19/2022  Modified python code to remove the "SINGLE=TRUE" option to write to a single 
#                           file since we exceeded the 5GB size limit when ran job in production.
# Paul Baranoski 2023-07-26 Change extract file extension from .csv to .txt 
# Paul Barnaoski 2024-02-02 Change EXT_TYPE "EARLYCUT" to "EARLY". "EARLYCUT" would create an EFT filename
#                           that is too long.
########################################################################################################
# IMPORTS
########################################################################################################
import os
import sys
import datetime
from datetime import datetime
import sendEmail

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
iMonth = int(now.strftime("%m"))
#var1 = sys.argv[1]

# Set Extract type
if iMonth < 3:
    EXT_TYPE = "M12"
elif iMonth >= 3 and iMonth <= 6:
    EXT_TYPE = "EARLY"
else:
    EXT_TYPE = "FINAL"

# set variables from environment variables
LAST_YEAR=os.getenv('last_year')
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

########################################################################################################
# Method to execute the extract SQL using parameters Start Date, End Date, Year, Quarter Number, 
########################################################################################################

def Extract_SQL(QSTRT_DT,QEND_DT,QTR):

   
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PTBCARRIER_STG/PartB_Carrier_{EXT_TYPE}_{LAST_YEAR}_QTR{QTR}_{TMSTMP}.txt.gz	
      
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
               '{LAST_YEAR}{QSTRT_DT}' AND '{LAST_YEAR}{QEND_DT}'
                --TRIM(EXTRACT(YEAR FROM CURRENT_DATE) -1)||'{QSTRT_DT}' AND TRIM(EXTRACT(YEAR FROM CURRENT_DATE) -1)||'{QEND_DT}'
	   
									
			)							
FILE_FORMAT = (TYPE = CSV field_delimiter = none ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=none )
                       OVERWRITE=TRUE  max_file_size=5368709120 """, con, exit_on_error=True)



try:
   snowconvert_helpers.configure_log()
   con = snowconvert_helpers.log_on()   
   snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)
   #snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_etl_warehouse}""", con,exit_on_error = True)
   
   #**************************************
   #   Extract PartB Carrier data  
   #**************************************   
   print("before executeSQL")
   Extract_SQL("-01-01","-03-31",'1')
   Extract_SQL("-04-01","-06-30",'2')
   Extract_SQL("-07-01","-09-30",'3')
   Extract_SQL("-10-01","-12-31",'4')
   


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

