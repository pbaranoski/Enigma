#!/usr/bin/env python
########################################################################################################
# Name:  FMR_EXTRACT.py
#
# Desc: Script to Extract FMR Data
# On-Prem Version: 
# Mainframe PARMs: IDRFMRDT,IDRFMRFE
# Date of Implementation: 01/20/2013
# Cloud Conversion scripts
# Created:  Viren Khanna  01/17/2023
# Modified: Joshua Turner 09/01/2023 Updated inputs vars and filename for EFT functionality  
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
#FNAME=os.getenv('FNAME')
FNAME_INT=os.getenv('FNAME_INT')
FNAME_RUN=os.getenv('FNAME_RUN')
CYEAR=os.getenv('CYEAR')
CUR_YR=os.getenv('CUR_YR')
PRIOR_INTRVL=os.getenv('PRIOR_INTRVL')
CURRENT_INTRVL=os.getenv('CURRENT_INTRVL')


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
   #   Extract FMR Half Yearly data  Current Naming: P#EFT.ON.FMRD.Y19I02.APR2020.TMSTMP and P#IDR.XTR.FMRD.Y20I01.OCT2020 
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_FMR_STG/FMRD_{FNAME_INT}_{FNAME_RUN}_{TMSTMP}.txt.gz
                                                FROM (



SELECT                                                                   
LPAD(TO_CHAR(GEO_RGN_CD,'FM00'),2) AS REGION,          
RPAD(CLM_CNTRCTR_NUM,5,' ')  AS CARRIER_NUMBER,                            
RPAD(CLM_RNDRG_FED_PRVDR_SPCLTY_CD,2,' ')                                  
AS PROVIDER_SPECIALTY,                                                         
RPAD(COALESCE(HCPCS_CD,' '),5,' ') AS PROCEDURE_CODE,                         
RPAD(COALESCE(HCPCS_MDFR_CD, ' '),2,' ') AS MODIFIER,                           
RPAD(HCPCS_FMR_CLSFCTN_CD,1,' ') AS DATA_CLASS,                           
RPAD('138',3) AS FILLER,                                                     
TO_CHAR(COALESCE(CARR_RANK,0),'FM00000')  AS CARRIER_RANK,                            
TO_CHAR(COALESCE(NATL_RANK,0),'FM00000')  AS NATIONAL_RANK,                           
TO_CHAR(COALESCE(CARR_ENRLMT,0),'FM00000000000')                           
AS CARRIER_ENROLLMENT,                                                         
TO_CHAR(COALESCE(NATL_ENRLMT, 0),'FM00000000000')                            
AS NATIONAL_ENROLLMENT,                                                        
TO_CHAR(COALESCE(CARR_ALOWD_SRVCS,0),'FM000000000')                                     
AS CARRIER_ALLOWED_SERVICES,                                                   
TO_CHAR(COALESCE(NATL_ALOWD_SRVCS,0),'FM00000000000')                                      
AS NATIONAL_ALLOWES_SERVICES,                                                
TO_CHAR(COALESCE(CARR_ALOWD_CHRGS,0),'FM00000000000')                                   
AS CARRIER_ALLOWED_CHARGES,                                                  
TO_CHAR(COALESCE(NATL_ALOWD_CHRGS,0), 'FM000000000000') 
AS NATIONAL_ALLOWED_CHARGES,                                                 
TO_CHAR(COALESCE(CARR_FREQ,0), 'FM000000000')                                          
AS CARRIER_FREQUENCY,                                                        
TO_CHAR(COALESCE(NATL_FREQ,0) ,'FM00000000000')                                            
AS NATIONAL_FREQUENCY,                                                       
TO_CHAR(COALESCE(CARR_DND_SRVCS,0), 'FM000000000')                                   
AS CARRIER_DENIED_SERVICES,                                                  
TO_CHAR(COALESCE(NATL_DND_SRVCS,0), 'FM00000000000')                                    
AS NATIONAL_DENIED_SERVICES,                                                 
TO_CHAR(COALESCE(CARR_PRR_YR_ALOWD_CHRGS,0),'FM00000000000')           
AS CARRIER_PRIOR_YEAR_CHARGES,                                               
TO_CHAR(COALESCE(NATL_PRR_YR_ALOWD_CHRGS,0), 'FM000000000000')            
AS NATIONAL_PRIOR_YEAR_CHARGES                                                  
FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.CLM_FMRD                                              
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21                  
ORDER BY 1,2,3,8 )

 
                        FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY=none )
                        SINGLE=TRUE max_file_size=5368709120  """, con, exit_on_error=True)

   
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
