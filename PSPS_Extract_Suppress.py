#!/usr/bin/env python
########################################################################################################
# Name:  PSPS_Extract_Suppress.py
#
# Desc: Script to Extract PSPS data and suppress small cell values. (Unit Qty < 11) (IDR#PSQS)
#
# Created: Paul Baranoski 07/20/2022 
# Modified: 
#
# Paul Baranoski 2022-11-09 Added code to send Success emails with filenames from script
#                           instead of python code. 
# Paul Baranoski 2023-09-01 Modified extract options to create CSV file with fields enclosed in double-
#                           quotes. field_delimiter = "," FIELD_OPTIONALLY_ENCLOSED_BY = '"'.
#                           This file will be used for upload to the web.
# Paul Baranoski 2023-09-19 Removed options to create CSV file using Python Extract SQL.
#                           Will add step to script to create CSV file from S3 Extract file.
#                           Rename Extract file for easier EFT processing. 
# Paul Baranoski 2024-07-22 Added COPYINTO File Format option ESCAPE_UNENCLOSED_FIELD=NONE to prevent '\A' from becoming '\\A' in output file.
# Paul Baranoski 2024-07-26 Create new set of date parameters for SERV_CYQ and PROC_CYQ.
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
SERV_CYQ_BEG_DT=os.getenv('SERV_CYQ_BEG_DT')
SERV_CYQ_END_DT=os.getenv('SERV_CYQ_END_DT')
PROC_CYQ_BEG_DT=os.getenv('PROC_CYQ_BEG_DT')
PROC_CYQ_END_DT=os.getenv('PROC_CYQ_END_DT')


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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PSPS_STG/PSPSQ6_SUPPRESS_{TMSTMP}.txt.gz
                                                FROM (

                        WITH PSPS_DTL_INFO as (

                            SELECT        
                                RPAD(CASE                                                                          
                                     WHEN A.CLM_LINE_INVLD_HCPCS_CD = '~' 
                                     THEN COALESCE(H.HCPCS_CD,' ')                       
                                     ELSE A.CLM_LINE_INVLD_HCPCS_CD                                            
                                     END,5,' ')  AS HCPCS_CD 

                               ,RPAD(CASE 
                                     WHEN E.HCPCS_1_MDFR_CD IS NULL 
                                       OR E.HCPCS_1_MDFR_CD = '~' 
                                     THEN ' '  
                                     ELSE E.HCPCS_1_MDFR_CD 
                                     END,2,' ')  AS INTI_MOD    
                               ,RPAD(CASE 
                                     WHEN E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD IS NULL 
                                       OR E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD ='~' 
                                     THEN ' ' 
                                     ELSE E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD 
                                     END,2,' ')  AS SPECIALTY_CD    
                               ,RPAD(CASE 
                                     WHEN A.CLM_CNTRCTR_NUM ='~' 
                                     THEN ' '  
                                     ELSE A.CLM_CNTRCTR_NUM 
                                     END,5,' ') AS CARRIER_NUM   
                                     
                               ,RPAD(CASE 
                                     WHEN E.CLM_PRCNG_LCLTY_CD ='~' 
                                     THEN ' '   
                                     ELSE E.CLM_PRCNG_LCLTY_CD 
                                     END,2,' ')  AS LOCALITY_CD 
                               ,RPAD(CASE 
                                     WHEN E.CLM_FED_TYPE_SRVC_CD ='~' 
                                     THEN ' '   
                                     ELSE E.CLM_FED_TYPE_SRVC_CD 
                                     END,1,' ') AS TOS  
                               ,RPAD(CASE 
                                     WHEN E.CLM_POS_CD ='~' 
                                     THEN ' '   
                                     ELSE E.CLM_POS_CD 
                                     END,2,' ') AS POS
                               ,RPAD(CASE 
                                     WHEN E.HCPCS_2_MDFR_CD is NULL
                                       OR E.HCPCS_2_MDFR_CD ='~' 
                                     THEN ' ' 
                                     ELSE E.HCPCS_2_MDFR_CD 
                                     END,2,' ') AS SECOND_MOD 

                               ,SUM(A.CLM_LINE_SBMT_SRVC_UNIT_QTY)   AS SBMT_SRVC_UNIT_QTY                                                     
                               ,SUM(A.CLM_LINE_SBMT_CHRG_AMT)        AS SBMT_CHRG_AMT                                                     
                               ,SUM(A.CLM_LINE_ALOWD_CHRG_AMT)       AS ALOWD_CHRG_AMT                                                     
                               ,SUM(A.CLM_LINE_DND_SRVC_UNIT_QTY)    AS DND_SRVC_UNIT_QTY 
                               ,SUM(A.CLM_LINE_DND_AMT)              AS DND_AMT 
                               ,SUM(A.CLM_LINE_ASGND_SRVC_UNIT_QTY)  AS ASGND_SRVC_UNIT_QTY
                               ,SUM(A.CLM_LINE_CVRD_PD_AMT)          AS CVRD_PD_AMT
                                                        
                               ,(CASE 
                                 WHEN H.HCPCS_ASC_IND_CD IS NULL
                                   OR H.HCPCS_ASC_IND_CD = '~' 
                                 THEN ' '  
                                 ELSE H.HCPCS_ASC_IND_CD       
                                 END)  AS HCPCS_ASC_IND_CD 
                                 
                               ,to_char(A.CLM_ERR_SGNTR_SK,'FM00') AS CLM_ERR_SGNTR_SK 
                               
                               ,RPAD(CASE 
                                 WHEN H.HCPCS_BETOS_CD IS NULL 
                                   OR H.HCPCS_BETOS_CD = '~' 
                                 THEN ' '   
                                 ELSE H.HCPCS_BETOS_CD 
                                 END,3,' ') AS BETOS                          
                                                        
                            FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_PTB_MO_AGG A  
                                                              
                            INNER JOIN BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_CYQ_SGNTR B                      
                            ON    A.CLM_CYQ_SGNTR_SK = B.CLM_CYQ_SGNTR_SK      
                                                 
                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_CYQ SERV_CYQ              
                            ON    B.CLNDR_SRVC_CYQ_SK = SERV_CYQ.CLNDR_CYQ_SK   
                                                
                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_CYQ PROC_CYQ              
                            ON    B.CLNDR_PROC_CYQ_SK = PROC_CYQ.CLNDR_CYQ_SK 
                                                 
                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_CYQ SP_CYQ                
                            ON    B.CLNDR_SCHLD_PMT_CYQ_SK = SP_CYQ.CLNDR_CYQ_SK  
                                                 
                            INNER JOIN BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_YEAR_SGNTR D                        
                            ON    B.CLM_YEAR_SGNTR_SK = D.CLM_YEAR_SGNTR_SK   
                                                     
                            INNER JOIN BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_CD_SGNTR E                          
                            ON    A.CLM_CD_SGNTR_SK = E.CLM_CD_SGNTR_SK      
                                                    
                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_HCPCS_{ENVNAME}.HCPCS_CD H                         
                            ON    A.CLM_LINE_HCPCS_CD = H.HCPCS_CD                                     
                            AND    A.CLNDR_HCPCS_YR_NUM = H.CLNDR_HCPCS_YR_NUM                         
                                                                                                           
                            WHERE SERV_CYQ.CLNDR_CYQ_NAME BETWEEN '{SERV_CYQ_BEG_DT}' AND '{SERV_CYQ_END_DT}'                                                                    
	                        AND   PROC_CYQ.CLNDR_CYQ_NAME BETWEEN '{PROC_CYQ_BEG_DT}' AND '{PROC_CYQ_END_DT}'  

                            GROUP BY                                                                     
                                     A.CLM_ERR_SGNTR_SK                                                             
                                    ,A.CLM_CNTRCTR_NUM                                                              
                                    ,E.CLM_PRCNG_LCLTY_CD                                                           
                                    ,E.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                                
                                    ,E.CLM_FED_TYPE_SRVC_CD                                                         
                                    ,E.CLM_POS_CD                                                                   
                                    ,E.HCPCS_1_MDFR_CD                                                              
                                    ,E.HCPCS_2_MDFR_CD                                                              
                                    ,H.HCPCS_CD                                                                     
                                    ,H.HCPCS_BETOS_CD                                                               
                                    ,H.HCPCS_ASC_IND_CD                                                             
                                    ,A.CLM_LINE_INVLD_HCPCS_CD

                        )

                        SELECT 
                               HCPCS_CD 
                              ,INTI_MOD    
                              ,SPECIALTY_CD    
                              ,CARRIER_NUM   
                              ,LOCALITY_CD 
                              ,TOS  
                              ,POS
                              ,SECOND_MOD 
                              ,CASE WHEN SBMT_SRVC_UNIT_QTY < 11
                                    THEN RPAD('*',14,' ')
                                    ELSE to_char(SBMT_SRVC_UNIT_QTY,'FM0000000000.000')
                                END  as  SBMT_SRVC_UNIT_QTY		
                              ,CASE WHEN SBMT_SRVC_UNIT_QTY < 11
                                    THEN RPAD('*',15,' ')
                                    ELSE to_char(SBMT_CHRG_AMT,'S00000000000.00')
                                END	as SBMT_CHRG_AMT
                              ,CASE WHEN ASGND_SRVC_UNIT_QTY < 11 
                                    THEN RPAD('*',15,' ')
                                    ELSE to_char(ALOWD_CHRG_AMT,'S00000000000.00')  
                                END as ALOWD_CHRG_AMT 	 
                              ,CASE WHEN DND_SRVC_UNIT_QTY < 11
                                    THEN RPAD('*',14,' ')
                                    ELSE to_char(DND_SRVC_UNIT_QTY,'FM0000000000.000') 
                               END as DND_SRVC_UNIT_QTY 		 
                              ,CASE WHEN DND_SRVC_UNIT_QTY < 11
                                    THEN RPAD('*',15,' ')
                                    ELSE to_char(DND_AMT,'S00000000000.00')
                               END as DND_AMT	
                              ,CASE WHEN ASGND_SRVC_UNIT_QTY < 11
                                    THEN RPAD('*',14,' ')
                                    ELSE to_char(ASGND_SRVC_UNIT_QTY,'FM0000000000.000') 
                               END as ASGND_SRVC_UNIT_QTY			
                              ,to_char(CVRD_PD_AMT,'S00000000000.00')  AS CVRD_PD_AMT
                              ,HCPCS_ASC_IND_CD 
                              ,CLM_ERR_SGNTR_SK 
                              ,BETOS              
                                 
                        FROM PSPS_DTL_INFO	


                        ) 
                        FILE_FORMAT = (TYPE = CSV field_delimiter = none FIELD_OPTIONALLY_ENCLOSED_BY=NONE  ESCAPE_UNENCLOSED_FIELD=NONE )
                        SINGLE = TRUE max_file_size=5368709120  """, con, exit_on_error=True)



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
