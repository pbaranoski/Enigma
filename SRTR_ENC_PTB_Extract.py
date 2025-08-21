#!/usr/bin/env python
########################################################################################################
# Name:  SRTR_ENC_PTB_Extract.py
#
# Desc: SRTR Encounter PTB Extract for claim types: CAR, DME
#
# Created: Paul Baranoski  02/16/2022
# Modified: 
#
# Paul Baranoski 2023-02-16 Created python pgm/script.
# Paul Baranoski 2023-03-10 Modified SQL SELECT for BF.CNTRCT_PBP_PTC_NUM. Replace '~' with ' ', 
#                           and ensure field is 3 characters long. LRECL and field alignment was affected.
# Paul Baranoski 2023-05-16 For EFT processing, add new variable to include only the YY for the filename 
#                           instead of full 4 digit year.
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

EXT_CLM_TYPES=os.getenv('EXT_CLM_TYPES')
EXT_YEAR=os.getenv('EXT_YEAR')
EXT_YEAR_YY=EXT_YEAR[2:4]
CLM_TYPE_LIT=os.getenv('CLM_TYPE_LIT')
 

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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_SRTRENCPTAB_STG/SRTR_ENC_{CLM_TYPE_LIT}_Y{EXT_YEAR_YY}_ext_{TMSTMP}.txt.gz
                                                FROM (                                                           


                            WITH ADDRTEMP AS (

                                SELECT
                                   C.GEO_BENE_SK
                                  ,C.CLM_DT_SGNTR_SK
                                  ,C.CLM_TYPE_CD
                                  ,C.CLM_NUM_SK

                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD = 'P' AND CA.CLM_ADR_SBTYP_CD = 'BP' 
                                          THEN CA.CLM_ADR_LINE_1_TXT ELSE NULL END)          AS CLM_BPRVDR_ADR_LINE_1_TXT
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'P' AND CA.CLM_ADR_SBTYP_CD = 'BP'  
                                                   THEN CA.CLM_ADR_LINE_2_TXT ELSE NULL END) AS CLM_BPRVDR_ADR_LINE_2_TXT
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD = 'P' AND CA.CLM_ADR_SBTYP_CD = 'BP'   
                                                   THEN CA.CLM_ADR_LINE_3_TXT ELSE NULL END) AS CLM_BPRVDR_ADR_LINE_3_TXT
                                                   
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'P' AND CA.CLM_ADR_SBTYP_CD = 'BP'   
                                                   THEN CA.CLM_CITY_NAME ELSE NULL END)      AS CLM_BPRVDR_CITY_NAME
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'P' AND CA.CLM_ADR_SBTYP_CD = 'BP'   
                                                   THEN	CA.CLM_USPS_STATE_CD ELSE NULL END)  AS CLM_BPRVDR_USPS_STATE_CD
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'P' AND CA.CLM_ADR_SBTYP_CD = 'BP'   
                                                   THEN CA.GEO_ZIP5_CD ELSE NULL END)        AS BPRVDR_GEO_ZIP5_CD
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'P' AND CA.CLM_ADR_SBTYP_CD = 'BP'   
                                                   THEN CA.GEO_ZIP4_CD ELSE NULL END)        AS BPRVDR_GEO_ZIP4_CD

                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'   
                                                   THEN CA.CLM_ADR_LINE_1_TXT ELSE NULL END)  AS CLM_SUBSCR_ADR_LINE_1_TXT
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'   
                                                   THEN CA.CLM_ADR_LINE_2_TXT ELSE NULL END)  AS CLM_SUBSCR_ADR_LINE_2_TXT
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'    
                                                   THEN CA.CLM_ADR_LINE_3_TXT ELSE NULL END)  AS CLM_SUBSCR_ADR_LINE_3_TXT
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'   
                                                   THEN CA.CLM_CITY_NAME ELSE NULL END)       AS CLM_SUBSCR_CITY_NAME
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'   
                                                   THEN CA.CLM_USPS_STATE_CD ELSE NULL END)   AS CLM_SUBSCR_USPS_STATE_CD
                                                   
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'   
                                                   THEN CA.GEO_ZIP5_CD ELSE NULL END)         AS SUBSCR_GEO_ZIP5_CD
                                ,MAX(CASE WHEN CA.CLM_ADR_TYPE_CD =  'B' AND CA.CLM_ADR_SBTYP_CD = 'SB'   
                                                   THEN CA.GEO_ZIP4_CD ELSE NULL END)         AS SUBSCR_GEO_ZIP4_CD

                                FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C
                                
                                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_ADR CA
                                ON  C.GEO_BENE_SK     = CA.GEO_BENE_SK
                                AND C.CLM_DT_SGNTR_SK = CA.CLM_DT_SGNTR_SK
                                AND C.CLM_TYPE_CD     = CA.CLM_TYPE_CD
                                AND C.CLM_NUM_SK      = CA.CLM_NUM_SK
                                AND CA.CLM_ADR_SQNC_NUM = 1

                                INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE B
                                ON C.BENE_SK          = B.BENE_SK

                                INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.SRTR_SSN SRTR
                                ON B.BENE_SSN_NUM = LTRIM(RTRIM(SRTR.SSN))

                                WHERE C.CLM_TYPE_CD IN ({EXT_CLM_TYPES})
                                AND C.CLM_FINL_ACTN_IND = 'Y'
                                AND C.CLM_THRU_DT BETWEEN TO_DATE('{EXT_YEAR}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_YEAR}-12-31','YYYY-MM-DD')
                                 
                                GROUP BY 1,2,3,4

                            )



                            SELECT DISTINCT
                                'START' AS st_of_file
                                ,'|' 
                                ,TO_CHAR(CL.CLM_TYPE_CD,'FM00000')        AS CLM_TYPE_CD
                                ,'|'
                                ,RPAD(COALESCE(C.CLM_HIC_NUM,' '),12,' ') AS CLM_HIC_NUM
                                ,'|'
                                ,RPAD(REPLACE(GFCC.GEO_SSA_STATE_CD,'~',' '),2,' ')  AS GEO_SSA_STATE_CD
                                ,'|' 

                                ,CASE WHEN C.CLM_FROM_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(C.CLM_FROM_DT,'YYYYMMDD') END  AS  CLM_FROM_DT
                                ,'|'
                                ,CASE WHEN C.CLM_THRU_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(C.CLM_THRU_DT,'YYYYMMDD') END  AS  CLM_FROM_DT
                                ,'|' 
                                ,CASE WHEN CED.CLM_EDPS_CREATN_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(CED.CLM_EDPS_CREATN_DT,'YYYYMMDD') END  AS  CLM_EDPS_CREATN_DT
                                ,'|' 
                                ,RPAD(C.CLM_CNTL_NUM,40,' ') AS  CLM_CNTL_NUM
                                ,'|' 

                                ,CASE WHEN CED.CLM_EDPS_LD_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(CED.CLM_EDPS_LD_DT,'YYYYMMDD') END  AS  CLM_EDPS_LD_DT
                                ,'|' 

                                ,RPAD(CE.CLM_CNTRCT_NUM,5,' ')      AS CLM_CNTRCT_NUM
                                ,'|' 
                                ,RPAD(C.CLM_BILL_FREQ_CD,1,' ')     AS CLM_BILL_FREQ_CD 
                                ,'|' 
                                
                                ,RPAD(REPLACE(GFCC.GEO_SSA_CNTY_CD,'~',' '),3,' ') AS GEO_SSA_CNTY_CD
                                ,'|' 
                                ,CASE WHEN DT.CLM_SUBMSN_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(DT.CLM_SUBMSN_DT,'YYYYMMDD') END  AS  CLM_SUBMSN_DT
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(ZIP5.GEO_ZIP5_CD,' '),'~',' '),5,' ') || RPAD(REPLACE(COALESCE(ZIP9.GEO_ZIP4_CD,' '),'~',' '),4,' ')  AS GEO_ZIP9
                                ,'|' 
                                ,RPAD(REPLACE(C.BENE_SEX_CD,'~',' '),1,' ')                  AS BENE_SEX_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(BF.BENE_RACE_CD,' '),'~',' '),2,' ')  AS BENE_RACE_CD
                                ,'|' 

                                ,CASE WHEN C.CLM_PTNT_BIRTH_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(C.CLM_PTNT_BIRTH_DT,'YYYYMMDD') END  AS  CLM_PTNT_BIRTH_DT
                                ,'|' 


                                ,RPAD(COALESCE(BF.BENE_MDCR_STUS_CD,' '),2,' ')  AS BENE_MDCR_STUS_CD 
                                ,'|' 
                                ,RPAD(CDN.CLM_LAST_NAME,60,' ')                  AS CLM_LAST_NAME  
                                ,'|' 
                                ,RPAD(CDN.CLM_1ST_NAME,35,' ')                   AS CLM_1ST_NAME 
                                ,'|' 
                                ,RPAD(COALESCE(CDN.CLM_INTL_MDL_NAME,' '),1,' ') AS CLM_INTL_MDL_NAME
                                ,'|' 
                                

                                ,RPAD(REPLACE(PROD_MT.CLM_DGNS_PRCDR_ICD_IND,'~',' '),1,' ')             AS CLM_DGNS_PRCDR_ICD_IND
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_PRNCPL_DGNS_CD,' '),'~',' '),7,' ')   AS CLM_PRNCPL_DGNS_CD
                                ,'|' 
                                
                                ,RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_UPIN_NUM,' '),'~',' '),6,' ')        AS CLM_RFRG_PRVDR_UPIN_NUM
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_NPI_NUM,' '),'~',' '),10,' ')        AS CLM_RFRG_PRVDR_NPI_NUM
                                ,'|' 

                                ,TO_CHAR(CASE WHEN CEOPD.CLM_OTHR_PYR_RSPNSBLTY_SQNC_CD = 'P'
                                              THEN CEOPD.CLM_ENCTR_OTHR_PYR_PD_AMT  
                                              ELSE 0 
                                              END,'MI0000000000000.00')  AS CLM_ENCTR_OTHR_PYR_PD_AMT
                                ,'|' 
                                ,TO_CHAR(COALESCE(C.CLM_SBMT_CHRG_AMT,0),'MI0000000000000.00')   AS CLM_SBMT_CHRG_AMT
                                ,'|' 


                                ,RPAD(REPLACE(COALESCE(C.CLM_DCMTN_CD,' '),'~',' '),2,' ')       AS CLM_DCMTN_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CEOPP.CLM_ENCTR_ADJSTMT_RSN_CD,' '),'~',' '),3,' ')     AS CLM_ENCTR_ADJSTMT_RSN_CD
                                ,'|' 
                                ,RPAD(REPLACE(CED.CLM_RMTNC_ADVC_RMRK_1_CD,'~',' '),15,' ')      AS CLM_RMTNC_ADVC_RMRK_1_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_PIN_NUM,' '),'~',' '),14,' ')          AS CLM_RFRG_PRVDR_PIN_NUM
                                ,'|' 

                                ,TO_CHAR(COALESCE(C.CLM_BLOOD_PT_FRNSH_QTY,0),'FM000')        AS CLM_BLOOD_PT_FRNSH_QTY
                                ,'|' 
                                ,TO_CHAR(COALESCE(CDN.CLM_BLOOD_DDCTBL_PT_QTY,0),'FM0000')    AS CLM_BLOOD_DDCTBL_PT_QTY
                                ,'|' 

                                ,RPAD(REPLACE(C.CLM_BLG_PRVDR_NPI_NUM,'~',' '),10,' ')        AS CLM_BLG_PRVDR_NPI_NUM
                                ,'|' 
                                ,TO_CHAR(COALESCE(PROD_MT.CLM_DGNS_TOT_OCRNC_CNT,0),'FM00')   AS CLM_DGNS_TOT_OCRNC_CNT
                                ,'|' 


                                ,RPAD(REPLACE(CDN.CLM_MCO_1ST_CNTRCT_NUM,'~',' '),5,' ')      AS CLM_MCO_1ST_CNTRCT_NUM
                                ,'|' 
                                ,RPAD(REPLACE(CDN.CLM_MCO_2ND_CNTRCT_NUM,'~',' '),5,' ')      AS CLM_MCO_2ND_CNTRCT_NUM
                                ,'|' 
                                ,RPAD(REPLACE(CDN.CLM_MCO_1ST_HLTH_PLAN_ID,'~',' '),14,' ')   AS CLM_MCO_1ST_HLTH_PLAN_ID
                                ,'|' 
                                ,RPAD(REPLACE(CDN.CLM_MCO_2ND_HLTH_PLAN_ID,'~',' '),14,' ')   AS CLM_MCO_2ND_HLTH_PLAN_ID
                                ,'|' 


                                --- CLM-DGNS-D  ************
                                ,RPAD(REPLACE(COALESCE(PROD.CLM_DGNS_PRCDR_ICD_IND,' '),'~',' '),1,' ')  AS CLM_DGNS_PRCDR_ICD_IND
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_1_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_1_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_2_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_2_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_3_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_3_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_4_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_4_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_5_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_5_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_6_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_6_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_7_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_7_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_8_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_8_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_9_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_9_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_10_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_10_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_11_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_11_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_12_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_12_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(PROD_MT.CLM_DGNS_13_CD,' '),'~',' '),7,' ')   AS CLM_DGNS_13_CD
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(C.CLM_RNDRG_PRVDR_NPI_NUM,' '),'~',' '),10,' ')        AS CLM_RNDRG_PRVDR_NPI_NUM
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(C.CLM_RNDRG_PRVDR_UPIN_NUM,' '),'~',' '),10,' ')       AS CLM_RNDRG_PRVDR_UPIN_NUM
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_NPI_NUM,' '),'~',' '),10,' ')       AS CLM_LINE_RNDRG_PRVDR_NPI_NUM
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_GRP_NPI_NUM,' '),'~',' '),10,' ')   AS CLM_RNDRG_PRVDR_GRP_NPI_NUM
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_TYPE_CD,' '),'~',' '),2,' ')        AS CLM_RNDRG_PRVDR_TYPE_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_TAX_NUM,' '),'~',' '),10,' ')       AS CLM_RNDRG_PRVDR_TAX_NUM 
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.GEO_RNDRG_SSA_STATE_CD,' '),'~',' '),2,' ')         AS GEO_RNDRG_SSA_STATE_CD
                                ,'|' 


                                ,RPAD(REPLACE(COALESCE(ZIP5.GEO_ZIP5_CD,' '),'~',' '),5,' ') || RPAD(REPLACE(COALESCE(ZIP9.GEO_ZIP4_CD,' '),'~',' '),4,' ')  AS GEO_ZIP9
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,' '),'~',' '),2,' ')  AS CLM_RNDRG_FED_PRVDR_SPCLTY_CD
                                ,'|' 
                                ,TO_CHAR(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'FM00000000000000.0000')       AS CLM_LINE_SRVC_UNIT_QTY
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_POS_CD,' '),'~',' '),2,' ')  AS CLM_LINE_POS_CD
                                ,'|' 


                                ,TO_CHAR(COALESCE(CL.CLM_LINE_NUM,0),'FM0000000000')       AS CLM_LINE_NUM
                                ,'|' 
                                ,CASE WHEN CL.CLM_LINE_FROM_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(CL.CLM_LINE_FROM_DT,'YYYYMMDD') END  AS  CLM_LINE_FROM_DT
                                ,'|' 
                                ,CASE WHEN CL.CLM_LINE_THRU_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(CL.CLM_LINE_THRU_DT,'YYYYMMDD') END  AS  CLM_LINE_THRU_DT
                                ,'|' 

                                ,RPAD(CL.CLM_LINE_HCPCS_CD,5,' ')
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.HCPCS_1_MDFR_CD,' '),'~',' '),2,' ')   AS HCPCS_1_MDFR_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.HCPCS_2_MDFR_CD,' '),'~',' '),2,' ')   AS HCPCS_2_MDFR_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.HCPCS_3_MDFR_CD,' '),'~',' '),2,' ')   AS HCPCS_3_MDFR_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.HCPCS_4_MDFR_CD,' '),'~',' '),2,' ')   AS HCPCS_4_MDFR_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_LINE_NDC_CD,' '),'~',' '),11,' ')  AS CLM_LINE_NDC_CD
                                ,'|' 

                                ,TO_CHAR(COALESCE(CLEOPD.CLM_LINE_OTHR_PYR_PD_AMT,0),'MI0000000000000.00')   AS CLM_LINE_OTHR_PYR_PD_AMT
                                ,'|' 
                                ,TO_CHAR(COALESCE(CL.CLM_LINE_PTB_BLOOD_DDCTBL_QTY,0),'FM000')               AS CLM_LINE_PTB_BLOOD_DDCTBL_QTY
                                ,'|' 
                                ,TO_CHAR(COALESCE(CL.CLM_LINE_SBMT_CHRG_AMT,0),'MI0000000000000.00')         AS CLM_LINE_SBMT_CHRG_AMT
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(CL.CLM_LINE_DGNS_CD,' '),'~',' '),7,' ')       AS CLM_LINE_DGNS_CD
                                ,'|' 
                                ,TO_CHAR(COALESCE(CL.CLM_LINE_ANSTHSA_UNIT_CNT,0),'FM0000.000')       AS CLM_LINE_ANSTHSA_UNIT_CNT
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(CL.CLM_LINE_RX_NUM,' '),'~',' '),30,' ')       AS CLM_LINE_RX_NUM
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CL.CLM_LINE_DCMTN_CD,' '),'~',' '),2,' ')      AS CLM_LINE_DCMTN_CD
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_NAME,' '),'~',' '),120,' ') AS CLM_RNDRG_PRVDR_NAME
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CLEOPP.CLM_ADJSTMT_RSN_CD,' '),'~',' '),3,' ') AS CLM_ADJSTMT_RSN_CD
                                ,'|' 


                                ,CASE WHEN C.CLM_SBMTR_CNTRCT_NUM = BF.CNTRCT_PTC_NUM 
                                      THEN RPAD(REPLACE(BF.CNTRCT_PBP_PTC_NUM,'~',' '),3,' ') 
                                      ELSE REPEAT(' ',3) END  AS CNTRCT_PBP_PTC_NUM
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(CED.CLM_CNTRCT_TYPE_CD,' '),'~',' '),2,' ')       AS CLM_CNTRCT_TYPE_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CLED.CLM_LINE_CNTRCT_TYPE_CD,' '),'~',' '),2,' ') AS CLM_LINE_CNTRCT_TYPE_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(CED.CLM_CHRT_RVW_SW,' '),'~',' '),1,' ')          AS CLM_CHRT_RVW_SW
                                ,'|' 

                                ,C.CLM_FINL_ACTN_IND
                                ,'|' 
                                ,CL.CLM_LINE_FINL_ACTN_IND
                                ,'|' 
                                ,C.CLM_LTST_CLM_IND 
                                ,'|' 
                                ,CL.CLM_LINE_LTST_CLM_IND
                                ,'|' 
                                
                                ,RPAD(COALESCE(CLE.CLM_LINE_ENCTR_STUS_CD,' '),20,' ') AS CLM_LINE_ENCTR_STUS_CD
                                ,'|' 
                                ,RPAD(COALESCE(C.CLM_ORIG_CNTL_NUM,' '),40,' ')        AS CLM_ORIG_CNTL_NUM 
                                ,'|' 
                                ,RPAD(COALESCE(CDN.CLM_PTNT_MDCL_REC_NUM,' '),80,' ')  AS CLM_PTNT_MDCL_REC_NUM   
                                ,'|' 

                                ,RPAD(REPLACE(COALESCE(C.CLM_BLG_PRVDR_TXNMY_CD,' '),'~',' '),50,' ')   AS CLM_BLG_PRVDR_TXNMY_CD   
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(C.CLM_ATNDG_PRVDR_TXNMY_CD,' '),'~',' '),10,' ') AS CLM_ATNDG_PRVDR_TXNMY_CD   
                                ,'|'
                                ,RPAD(COALESCE(CE.CLM_EDPS_STUS_CD,' '),20,' ')        AS CLM_EDPS_STUS_CD   
                                ,'|'

                                ,CASE WHEN C.CLM_OBSLT_DT IS NULL  THEN REPEAT(' ',8) ELSE TO_CHAR(C.CLM_OBSLT_DT,'YYYYMMDD') END  AS  CLM_OBSLT_DT
                                ,'|'


                                ,TO_CHAR(COALESCE(CED.CLM_CNTRCT_AMT,0),'MI0000000000000.00')       AS CLM_CNTRCT_AMT
                                ,'|' 
                                ,TO_CHAR(COALESCE(C.CLM_PTNT_LBLTY_AMT,0),'MI0000000000000.00')     AS CLM_PTNT_LBLTY_AMT
                                ,'|' 


                                ,RPAD(REPLACE(COALESCE(BENE.BENE_EQTBL_BIC_CD,' '),'~',' '),2,' ')  AS  BENE_EQTBL_BIC_CD
                                ,'|' 

                                ,RPAD(COALESCE(ADRTEMP.CLM_BPRVDR_ADR_LINE_1_TXT,' '),55,' ') AS CLM_BPRVDR_ADR_LINE_1_TXT   
                                ,'|' 
                                ,RPAD(COALESCE(ADRTEMP.CLM_BPRVDR_ADR_LINE_2_TXT,' '),55,' ') AS CLM_BPRVDR_ADR_LINE_2_TXT   
                                ,'|' 
                                ,RPAD(COALESCE(ADRTEMP.CLM_BPRVDR_ADR_LINE_3_TXT,' '),55,' ') AS CLM_BPRVDR_ADR_LINE_3_TXT   
                                ,'|' 


                                ,RPAD(COALESCE(ADRTEMP.CLM_BPRVDR_CITY_NAME,' '),30,' ')    AS CLM_BPRVDR_CITY_NAME
                                ,'|' 
                                ,RPAD(COALESCE(ADRTEMP.CLM_BPRVDR_USPS_STATE_CD,' '),2,' ') AS CLM_BPRVDR_USPS_STATE_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(ADRTEMP.BPRVDR_GEO_ZIP5_CD,' '),'~',' '),5,' ') || RPAD(REPLACE(COALESCE(ADRTEMP.BPRVDR_GEO_ZIP4_CD,' '),'~',' '),4,' ')                                          AS ADR_BPRVDR_ZIP_CD
                                ,'|' 
                                                                              

                                ,RPAD(COALESCE(ADRTEMP.CLM_SUBSCR_ADR_LINE_1_TXT,' '),55,' ')   AS CLM_SUBSCR_ADR_LINE_1_TXT
                                ,'|' 
                                ,RPAD(COALESCE(ADRTEMP.CLM_SUBSCR_ADR_LINE_2_TXT,' '),55,' ')   AS CLM_SUBSCR_ADR_LINE_2_TXT
                                ,'|' 
                                ,RPAD(COALESCE(ADRTEMP.CLM_SUBSCR_ADR_LINE_3_TXT,' '),55,' ')   AS CLM_SUBSCR_ADR_LINE_3_TXT
                                ,'|' 


                                ,RPAD(COALESCE(ADRTEMP.CLM_SUBSCR_CITY_NAME,' '),30,' ')     AS CLM_SUBSCR_CITY_NAME
                                ,'|' 
                                ,RPAD(COALESCE(ADRTEMP.CLM_SUBSCR_USPS_STATE_CD,' '),2,' ')  AS CLM_SUBSCR_USPS_STATE_CD
                                ,'|' 
                                ,RPAD(REPLACE(COALESCE(ADRTEMP.SUBSCR_GEO_ZIP5_CD,' '),'~',' '),5,' ') || RPAD(REPLACE(COALESCE(ADRTEMP.SUBSCR_GEO_ZIP4_CD,' '),'~',' '),4,' ')                                           AS ADR_SUBSCR_ZIP_CD
                                ,'|' 
                                

                                ,RPAD(REPLACE(COALESCE(C.CLM_BLG_PRVDR_TAX_NUM ,' '),'~',' '),10,' ')   AS CLM_BLG_PRVDR_TAX_NUM 
                                ,'|' 
                                ,RPAD(C.BENE_ID_TYPE_CD,1,' ')         AS BENE_ID_TYPE_CD
                                ,'|' 
                                ,RPAD(COALESCE(BENE.BENE_MBI_ID,' '),11,' ')         AS BENE_MBI_ID
                                ,'|' 
                                ,RPAD(COALESCE(CED.CLM_CHRT_RVW_EFCTV_SW,' '),1,' ') AS CLM_CHRT_RVW_EFCTV_SW
                                ,'|' 
                                ,RPAD(COALESCE(CED.CLM_EDPS_CHRT_RVW_SW,' '),1,' ')  AS CLM_EDPS_CHRT_RVW_SW
                                ,'|' 
                                
                                ,RPAD(REPLACE(COALESCE(CE.CLM_POS_CD,' '),'~',' '),2,' ')   AS CLM_CE_POS_CD
                                ,'|'
                                ,RPAD(REPLACE(COALESCE(C.CLM_RNDRG_PRVDR_NPI_NUM,' '),'~',' '),10,' ')   AS CLM_RNDRG_PRVDR_NPI_NUM
                                ,'|'

                                ,'END'  AS END_OF_FILE


                            FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR DT
                            ON  DT.CLM_DT_SGNTR_SK  =  C.CLM_DT_SGNTR_SK

                            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE BENE
                            ON C.BENE_SK  =  BENE.BENE_SK

                            INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.SRTR_SSN SRTR
                            ON BENE.BENE_SSN_NUM = LTRIM(RTRIM(SRTR.SSN))
                             
                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                            ON C.GEO_BENE_SK      = CL.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                            AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

                            -- modify SQL to use parameter.
                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_BENE_MTRLZD_{ENVNAME}.BENE_FCT_TRANS BF
                            ON  BF.BENE_SK                     =  C.BENE_SK
                            AND TO_DATE(BF.IDR_TRANS_OBSLT_TS) = TO_DATE('9999-12-31','YYYY-MM-DD')
                            AND C.CLM_FROM_DT BETWEEN BENE_FCT_EFCTV_DT AND
                                CASE WHEN BF.BENE_DEATH_DT IS NOT NULL
                                THEN ADD_MONTHS( ( BENE_FCT_OBSLT_DT - EXTRACT(DAY FROM BENE_FCT_OBSLT_DT) + 1) ,1) -1
                                ELSE BENE_FCT_OBSLT_DT
                                END
                                
                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN CDN
                            ON  C.GEO_BENE_SK     = CDN.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_ENCTR CE
                            ON C.GEO_BENE_SK      = CE.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CE.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CE.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CE.CLM_NUM_SK

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_ENCTR CLE
                            ON  CL.GEO_BENE_SK     = CLE.GEO_BENE_SK
                            AND CL.CLM_DT_SGNTR_SK = CLE.CLM_DT_SGNTR_SK
                            AND CL.CLM_TYPE_CD     = CLE.CLM_TYPE_CD
                            AND CL.CLM_NUM_SK      = CLE.CLM_NUM_SK
                            AND CL.CLM_LINE_NUM    = CLE.CLM_LINE_NUM

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_ENCTR_DCMTN  CED
                            ON C.GEO_BENE_SK     = CED.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CED.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CED.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CED.CLM_NUM_SK

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_ENCTR_DCMTN  CLED
                            ON CL.GEO_BENE_SK      = CLED.GEO_BENE_SK
                            AND CL.CLM_DT_SGNTR_SK = CLED.CLM_DT_SGNTR_SK
                            AND CL.CLM_TYPE_CD     = CLED.CLM_TYPE_CD
                            AND CL.CLM_NUM_SK      = CLED.CLM_NUM_SK
                            AND CL.CLM_LINE_NUM    =  CLED.CLM_LINE_NUM

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_ENCTR_OTHR_PYR_DTL CEOPD
                            ON C.GEO_BENE_SK      = CEOPD.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CEOPD.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CEOPD.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CEOPD.CLM_NUM_SK
                            AND CEOPD.CLM_OTHR_PYR_RSPNSBLTY_SQNC_CD = 'P'

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_ENCTR_OTHR_PYR_PMT CEOPP
                            ON C.GEO_BENE_SK      = CEOPP.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = CEOPP.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = CEOPP.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = CEOPP.CLM_NUM_SK
                            AND CEOPP.CLM_OTHR_PYR_PMT_SQNC_NUM = 1

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_ENCTR_OTHR_PYR_DTL CLEOPD
                            ON CL.GEO_BENE_SK      =  CLEOPD.GEO_BENE_SK
                            AND CL.CLM_DT_SGNTR_SK =  CLEOPD.CLM_DT_SGNTR_SK
                            AND CL.CLM_TYPE_CD     =  CLEOPD.CLM_TYPE_CD
                            AND CL.CLM_NUM_SK      =  CLEOPD.CLM_NUM_SK
                            AND CL.CLM_LINE_NUM    =  CLEOPD.CLM_LINE_NUM
                            AND CLEOPD.CLM_LINE_OTHR_PYR_SQNC_NUM = 1

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_ENCTR_OTHR_PYR_PMT CLEOPP
                            ON CL.GEO_BENE_SK      =  CLEOPP.GEO_BENE_SK
                            AND CL.CLM_DT_SGNTR_SK =  CLEOPP.CLM_DT_SGNTR_SK
                            AND CL.CLM_TYPE_CD     =  CLEOPP.CLM_TYPE_CD
                            AND CL.CLM_NUM_SK      =  CLEOPP.CLM_NUM_SK
                            AND CL.CLM_LINE_NUM    =  CLEOPP.CLM_LINE_NUM
                            AND CLEOPP.CLM_LINE_OTHR_PYR_PMT_SQNC_NUM = 1

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PROD PROD
                            ON C.GEO_BENE_SK      = PROD.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = PROD.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = PROD.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = PROD.CLM_NUM_SK
                            AND PROD.CLM_PROD_TYPE_CD = 'D'

                            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PROD_MTRLZD PROD_MT
                            ON C.GEO_BENE_SK      = PROD_MT.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = PROD_MT.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = PROD_MT.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = PROD_MT.CLM_NUM_SK

                            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP5_CD ZIP5
                            ON ZIP5.GEO_SK = C.GEO_BENE_SK

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP9_CD ZIP9
                            ON ZIP9.GEO_SK          = ZIP5.GEO_SK
                            AND BENE.GEO_ZIP4_CD    = ZIP9.GEO_ZIP4_CD

                            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_FIPS_CNTY_CD GFCC
                            ON GFCC.GEO_FIPS_CNTY_CD   = ZIP5.GEO_FIPS_CNTY_CD
                            AND GFCC.GEO_FIPS_STATE_CD = ZIP5.GEO_FIPS_STATE_CD

                            LEFT OUTER JOIN ADDRTEMP ADRTEMP
                            ON  C.GEO_BENE_SK     = ADRTEMP.GEO_BENE_SK
                            AND C.CLM_DT_SGNTR_SK = ADRTEMP.CLM_DT_SGNTR_SK
                            AND C.CLM_TYPE_CD     = ADRTEMP.CLM_TYPE_CD
                            AND C.CLM_NUM_SK      = ADRTEMP.CLM_NUM_SK

                            WHERE C.CLM_TYPE_CD IN ({EXT_CLM_TYPES})
                            AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('{EXT_YEAR}-01-01','YYYY-MM-DD') AND TO_DATE('{EXT_YEAR}-12-31','YYYY-MM-DD')
                            AND C.CLM_FINL_ACTN_IND  = 'Y'


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
