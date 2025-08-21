#!/usr/bin/env python
########################################################################################################
# Name:  OFM_PDE_Extract.py
#
# Desc: OFM PDE Extract
#
# Created: Paul Baranoski  03/27/2023
# Modified: 
#
# Paul Baranoski 2023-03-27 Created python pgm/script.
# Paul Baranoski 2024-08-02 Update parameter value names passed to python code.
# Paul Baranoski 2024-08-06 Modified to retrieve Env variable EXTRACT_FILE_TMSTMP and assign to python variable TMSTMP.
# N. Tinovsky	 2024-12-23 Increased length for following fields:
#			CLM_PRSBNG_PRVDR_GNRC_ID_NUM,CLM_LINE_INGRDNT_CST_AMT,CLM_LINE_SRVC_CST_AMT
#			,CLM_LINE_SLS_TAX_AMT,CLM_LINE_GRS_BLW_THRSHLD_AMT,CLM_LINE_GRS_ABOVE_THRSHLD_AMT,CLM_LINE_LIS_AMT,CLM_LINE_PLRO_AMT
#			,CLM_LINE_NCVRD_PD_AMT,CLM_LINE_REBT_PASSTHRU_POS_AMT,CLM_LINE_VCCN_ADMIN_FEE_AMT,CLM_LINE_GRS_CVRD_CST_TOT_AMT
#			,CLM_LINE_TROOP_TOT_AMT,CLM_LINE_RPTD_GAP_DSCNT_AMT,CLM_LTC_DSPNSNG_MTHD_CD,CLM_LINE_CALCD_GAP_DSCNT_AMT
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

TMSTMP=os.getenv('EXTRACT_FILE_TMSTMP')
ENVNAME=os.getenv('ENVNAME')

CLM_PRIOR_YYYY=os.getenv('CLM_PRIOR_YYYY')
CLM_EFCTV_DT=os.getenv('CLM_EFCTV_DT')
CONTRACTOR=os.getenv('CONTRACTOR')
CONTRACT_NUM=os.getenv('CONTRACT_NUM')
PBP_NUM=os.getenv('PBP_NUM')
 

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
   #   Extract SRTR Part A data 
   #**************************************                                                     
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_OFMPDE_STG/OFM_PDE_{CONTRACTOR}_{CONTRACT_NUM}_{PBP_NUM}_ext_{TMSTMP}.txt.gz
                                                FROM (                                                           


        SELECT DISTINCT 
             'DET'       AS RECORD_ID
            ,'*      '  AS SEQ_NO
            ,RPAD(C.CLM_CNTL_NUM,40,' ')          AS  CLM_CNTL_NUM
            ,RPAD(TRIM(C.CLM_HIC_NUM),20,' ')     AS  CLM_HIC_NUM
            ,RPAD(C.CLM_CARDHLDR_ID,20,' ')       AS  CLM_CARDHLDR_ID                              
            ,CASE WHEN C.CLM_PTNT_BIRTH_DT IS NULL  
                  THEN REPEAT(' ',8)
                  ELSE TO_CHAR(C.CLM_PTNT_BIRTH_DT,'YYYYMMDD') 
             END                                  AS  PATIENT_DOB	                                 
            ,RPAD(C.BENE_SEX_CD,1,' ')            AS  CLM_PTNT_SEX_CD
            ,CASE WHEN CL.CLM_LINE_FROM_DT IS NULL  
                  THEN REPEAT(' ',8)
                  ELSE TO_CHAR(CL.CLM_LINE_FROM_DT,'YYYYMMDD') 
             END                                  AS  CLM_LINE_SRVC_DT
            ,CASE WHEN C.CLM_SCHLD_PMT_DT IS NULL  
                  THEN REPEAT(' ',8)
                  ELSE TO_CHAR(C.CLM_SCHLD_PMT_DT,'YYYYMMDD') 
             END                                  AS  CLM_SCHLD_PMT_DT
            ,RPAD(CL.CLM_LINE_RX_NUM,30,' ')      AS  CLM_LINE_RX_NUM
            ,'  '    AS FILLER_1
            ,RPAD(CL.CLM_LINE_NDC_CD,19,' ')                 AS  PROD_NDC_CODE  
            ,RPAD(C.PRVDR_SRVC_ID_QLFYR_CD,2,' ')            AS  SRVC_GNRC_ID_QLFYR
            ,RPAD(C.CLM_SRVC_PRVDR_GNRC_ID_NUM,20,' ')       AS  SRVC_PRVDR_ID
            ,TO_CHAR(CLR.CLM_LINE_RX_FILL_NUM,'FM000000000') AS CLM_LINE_RX_FILL_NUM
            ,RPAD(CLR.CLM_DSPNSNG_STUS_CD,1,' ')             AS  CLM_DSPNSNG_STUS_CD
            ,RPAD(CLR.CLM_CMPND_CD,1,' ')                    AS  CLM_CMPND_CD
            ,RPAD(CLR.CLM_DAW_PROD_SLCTN_CD,1,' ')           AS  CLM_DAW_PROD_SLCTN_CD
            -- 18.4	FORMAT '-9(14).9999'
            ,TO_CHAR(CL.CLM_LINE_SRVC_UNIT_QTY,'MI00000000000000.0000')  AS PTAP_QUANTITY_DISPENSED
            ,'  '   AS FILLER_2
            ,TO_CHAR(CLR.CLM_LINE_DAYS_SUPLY_QTY,'FM000000000')   AS  CLM_LINE_DAYS_SUPLY_QTY
            ,RPAD(C.PRVDR_PRSBNG_ID_QLFYR_CD,2,' ')          AS  PRSCRB_ID_QLFYR

			--****\/****
			--OLD: NUMBER(20) X(20) 		NEW: NUMBER(35) X(35)							
			--,RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,20,' ')     	AS  PRSCRB_ID 
			  ,RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,35,' ')     	AS  PRSCRB_ID
			--****/\****

            ,RPAD(CLR.CLM_DRUG_CVRG_STUS_CD,2,' ')          	AS  CLM_DRUG_CVRG_STUS_CD 
            ,CASE C.CLM_TYPE_CD WHEN 1 THEN ' '  WHEN 2 THEN 'A'  WHEN 3 THEN 'D'  WHEN 4 THEN 'R' ELSE 'X' END   AS ADJ_DEL_CD              
            ,RPAD(C.CLM_SBMT_FRMT_CD,1,' ')                 AS  CLM_SBMT_FRMT_CD
            ,RPAD(CLR.CLM_PRCNG_EXCPTN_CD,1,' ')            AS  CLM_PRCNG_EXCPTN_CD 
            ,RPAD(CLR.CLM_CTSTRPHC_CVRG_IND_CD,1,' ')       AS  CLM_CTSTRPHC_CVRG_IND_CD

			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
			--,TO_CHAR(CLR.CLM_LINE_INGRDNT_CST_AMT,'MI0000000.00')     AS PTAP_INGRDNT_COST_PD
			  ,TO_CHAR(CLR.CLM_LINE_INGRDNT_CST_AMT,'MI000000000.00')   AS PTAP_INGRDNT_COST_PD
			--****/\****
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_SRVC_CST_AMT,'MI0000000.00')      	AS PTAP_DSPNSNG_FEE_PD
			  ,TO_CHAR(CLR.CLM_LINE_SRVC_CST_AMT,'MI000000000.00')  	AS PTAP_DSPNSNG_FEE_PD
			--****/\****
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_SLS_TAX_AMT,'MI0000000.00') 	AS PTAP_AMT_SALES_TAX
			  ,TO_CHAR(CLR.CLM_LINE_SLS_TAX_AMT,'MI000000000.00') 	AS PTAP_AMT_SALES_TAX
			--****/\****
			
			--****\/****
			--OLD: NUMBER(8,2) S9(6)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,'MI000000.00')		AS PTAP_BELOW_OOP_THRHLD
			  ,TO_CHAR(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,'MI000000000.00')	AS PTAP_BELOW_OOP_THRHLD
			--****/\****
			
			--****\/****
			--OLD: NUMBER(8,2) S9(6)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,'MI000000.00')	AS PTAP_ABOVE_OOP_THRHLD
			  ,TO_CHAR(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,'MI000000000.00')	AS PTAP_ABOVE_OOP_THRHLD
			--****/\****
						
			,TO_CHAR(CL.CLM_LINE_BENE_PMT_AMT,'MI000000000.00')          AS PTAP_PATIENT_PAY_AMT
			,TO_CHAR(CL.CLM_LINE_OTHR_TP_PD_AMT,'MI0000000000000.00')    AS PTAP_OTHER_TROOP_AMT

			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_LIS_AMT,'MI0000000.00')		AS PTAP_LICS_AMT
			  ,TO_CHAR(CLR.CLM_LINE_LIS_AMT,'MI000000000.00')	AS PTAP_LICS_AMT
			--****/\****
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_PLRO_AMT,'MI0000000.00')	AS PTAP_PLRO_AMT
			  ,TO_CHAR(CLR.CLM_LINE_PLRO_AMT,'MI000000000.00')	AS PTAP_PLRO_AMT
			--****/\****			
			
            ,TO_CHAR(CL.CLM_LINE_CVRD_PD_AMT,'MI000000000.00')           	AS PTAP_CVRD_D_PLAN_PAID
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99									
			--,TO_CHAR(CL.CLM_LINE_NCVRD_PD_AMT,'MI0000000.00')		AS PTAP_NON_CVRD_PLAN_PAID
			  ,TO_CHAR(CL.CLM_LINE_NCVRD_PD_AMT,'MI000000000.00')	AS PTAP_NON_CVRD_PLAN_PAID
			--****/\****
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_REBT_PASSTHRU_POS_AMT,'MI0000000.00')  	AS CLM_LINE_REBT_PASSTHRU_POS_AMT
			  ,TO_CHAR(CLR.CLM_LINE_REBT_PASSTHRU_POS_AMT,'MI000000000.00')	AS CLM_LINE_REBT_PASSTHRU_POS_AMT
			--****/\****
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--TO_CHAR(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT,'MI0000000.00')		AS CLM_LINE_VCCN_ADMIN_FEE_AMT
			  ,TO_CHAR(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT,'MI000000000.00')	AS CLM_LINE_VCCN_ADMIN_FEE_AMT
			--****/\****
						
            ,RPAD(CLR.CLM_LINE_RX_ORGN_CD,1,' ')                         	AS CLM_LINE_RX_ORGN_CD
            ,CASE WHEN CLR.CLM_LINE_ORGNL_RCVD_DT IS NULL  
                  THEN REPEAT(' ',8)
                  ELSE TO_CHAR(CLR.CLM_LINE_ORGNL_RCVD_DT,'YYYYMMDD') 
             END   AS CLM_LINE_ORGNL_RCVD_DT                               
            ,RPAD(TO_CHAR(CLR.CLM_LINE_ADJDCTN_BGN_TS,'YYYY-MM-DD HH:MI:SS'),26,' ')  AS CLM_LINE_ADJDCTN_BGN_T                              

			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--,TO_CHAR(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT,'MI0000000.00')		AS CLM_LINE_GRS_CVRD_CST_TOT_AMT
			  ,TO_CHAR(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT,'MI000000000.00')	AS CLM_LINE_GRS_CVRD_CST_TOT_AMT
			--****/\****
			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--TO_CHAR(CLR.CLM_LINE_TROOP_TOT_AMT,'MI0000000.00')	AS CLM_LINE_TROOP_TOT_AMT
			 ,TO_CHAR(CLR.CLM_LINE_TROOP_TOT_AMT,'MI000000000.00')	AS CLM_LINE_TROOP_TOT_AMT
			--****/\****
					
            ,RPAD(CLR.CLM_BRND_GNRC_CD,1,' ')                            AS CLM_BRND_GNRC_CD
            ,RPAD(CLR.CLM_BGN_BNFT_PHASE_CD,1,' ')                       AS CLM_BGN_BNFT_PHASE_CD
            ,RPAD(CLR.CLM_END_BNFT_PHASE_CD,1,' ')                       AS CLM_END_BNFT_PHASE_CD

			
			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99										
			--TO_CHAR(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,'MI0000000.00')	AS CLM_LINE_RPTD_GAP_DSCNT_AMT
			 ,TO_CHAR(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,'MI000000000.00')	AS CLM_LINE_RPTD_GAP_DSCNT_AMT
			--****/\****
						
            ,RPAD(CLR.CLM_LINE_FRMLRY_TIER_LVL_ID,1,' ')                 AS CLM_LINE_FRMLRY_TIER_LVL_ID
            ,RPAD(CLR.CLM_FRMLRY_CD,1,' ')                               AS CLM_FRMLRY_CD
            ,' '                                                         AS CLM_GAP_DSCNT_PLAN_OVRRD_CD
            ,RPAD(CLR.CLM_PHRMCY_SRVC_TYPE_CD,2,' ')                     AS CLM_PHRMCY_SRVC_TYPE_CD
            ,RPAD(CLR.CLM_PTNT_RSDNC_CD,2,' ')                           AS CLM_PTNT_RSDNC_CD

			--****\/****
			--OLD:NUMBER(2) 9(2) 			NEW:NUMBER(3) 9(3)
			--,RPAD(CLR.CLM_LTC_DSPNSNG_MTHD_CD,2,' ')                   AS CLM_LTC_DSPNSNG_MTHD_CD	
			  ,RPAD(CLR.CLM_LTC_DSPNSNG_MTHD_CD,3,' ')                   AS CLM_LTC_DSPNSNG_MTHD_CD	
			--****/\****
						
            ,RPAD(CLR.CLM_PTD_ADJSTMT_QLFYR_CD,1,' ')                    AS CLM_PTD_ADJSTMT_QLFYR_CD
            ,RPAD(CLR.CLM_PTD_ADJSTMT_RSN_TEXT,12,' ')                   AS CLM_PTD_ADJSTMT_RSN_TEXT 
            ,REPEAT(' ',11)                                              AS FILLER_3

			--****\/****
			--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99							
			--,TO_CHAR(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT,'MI0000000.00')  	AS CLM_LINE_CALCD_GAP_DSCNT_AMT
			  ,TO_CHAR(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT,'MI000000000.00') 	AS CLM_LINE_CALCD_GAP_DSCNT_AMT
			--****/\****
						
            ,RPAD(C.CLM_SBMTR_CNTRCT_NUM,5,' ')                          AS SBMTR_CNTRCT_NUM
            ,RPAD(C.CLM_SBMTR_CNTRCT_PBP_NUM,3,' ')                      AS SBMTR_CNTRCT_PBP_NUM
            ,RPAD(C.CLM_CNTRCT_OF_REC_CNTRCT_NUM,5,' ')                  AS COR_CNTRCT_NUM
            ,RPAD(C.CLM_CNTRCT_OF_REC_PBP_NUM,3,' ')                     AS COR_CNTRCT_PBP_NUM 
            ,RPAD(C.CLM_BENE_MBI_ID,11,' ')                              AS MBI_ID
        FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C
             
        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
            ON  C.CLM_NUM_SK      = CL.CLM_NUM_SK
            AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
            AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
            AND C.GEO_BENE_SK     = CL.GEO_BENE_SK
            AND C.CLM_FROM_DT     = CL.CLM_FROM_DT
                 
        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX CLR
            ON  CL.CLM_NUM_SK      = CLR.CLM_NUM_SK
            AND CL.CLM_TYPE_CD     = CLR.CLM_TYPE_CD
            AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
            AND CL.GEO_BENE_SK     = CLR.GEO_BENE_SK
            AND CL.CLM_LINE_NUM    = CLR.CLM_LINE_NUM
                 
            -- two parameters 1) YEAR for CLM_FROM_DT range  2) for EFCTV_DT and OBSLT_DT	 
        WHERE C.CLM_TYPE_CD IN (1,2,4)
            --AND C.CLM_FINL_ACTN_IND = 'Y'
              AND C.CLM_FROM_DT  BETWEEN to_date('{CLM_PRIOR_YYYY}-01-01','YYYY-MM-DD') AND to_date('{CLM_PRIOR_YYYY}-12-31','YYYY-MM-DD') 
              AND C.CLM_EFCTV_DT <= to_date('{CLM_EFCTV_DT}','YYYY-MM-DD')
              AND C.CLM_OBSLT_DT >  to_date('{CLM_EFCTV_DT}','YYYY-MM-DD')
              AND (   (C.CLM_SBMTR_CNTRCT_NUM ='{CONTRACT_NUM}'         AND C.CLM_SBMTR_CNTRCT_PBP_NUM ='{PBP_NUM}')
                   OR (C.CLM_CNTRCT_OF_REC_CNTRCT_NUM ='{CONTRACT_NUM}' AND C.CLM_CNTRCT_OF_REC_PBP_NUM='{PBP_NUM}') )

            ORDER BY CLM_HIC_NUM  

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
      print("Closing snowflake connection")
      con.close()

   # Let shell script know that python code failed.      
   if bPythonExceptionOccurred == True:
      sys.exit(12) 
   else:   
      snowconvert_helpers.quit_application()
