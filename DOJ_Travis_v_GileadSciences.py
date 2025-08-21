#!/usr/bin/env python
########################################################################################################
# Name:  DOJ_Travis_v_GileadSciences.py
#
# DESC:  This python script extracts PTD data.
#
# Created: Paul Baranoski
# Modified: 12/20/2023
#           12/30/2024  N.Tinovsky  Updated length of fields for DDPS project
# 
########################################################################################################
# IMPORTS
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

import snowconvert_helpers
from snowconvert_helpers import Export

########################################################################################################
# VARIABLE ASSIGNMENT
########################################################################################################
script_name = os.path.basename(__file__)
con = None 
now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

ENVNAME=os.getenv('ENVNAME')
TMSTMP=os.getenv('UNIQUE_FILE_TMSTMP')

FROM_DT=os.getenv('FROM_DT')
TO_DT=os.getenv('TO_DT')
REQUESTOR=os.getenv('REQUESTOR')
NDC_CODES=os.getenv('NDC_CODES')

# boolean - Python Exception status
bPythonExceptionOccurred=False

try:
    snowconvert_helpers.configure_log()
    con = snowconvert_helpers.log_on()
    snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
    snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

    ########################################################################################################
    # Extract OPM-HI Part D data and write to S3
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DOJ_STG/DOJ_TRAVIS_V_GILEAD_{REQUESTOR}_{TMSTMP}.txt.gz
                FROM (
                
SELECT DISTINCT
    'START' AS START_REC,
    LPAD(CL.CLM_TYPE_CD,5,'0') AS CLM_TYPE_CD,
    RPAD(SUBSTR(C.CLM_HIC_NUM,1,12),12,' ') AS BENE_HICN,
    RPAD(COALESCE(ZIP5.GEO_ZIP5_CD,''),5,' ') AS BENE_MAILING_ZIPCD,
    C.BENE_SEX_CD AS BENE_GENDER,
    TO_CHAR(B.BENE_BRTH_DT, 'YYYYMMDD') AS BENE_DOB,
    RPAD(COALESCE(G.GEO_SSA_STATE_CD,''),2,' ') AS SSA_STATE_CD,
    C.CLM_CNTRCT_OF_REC_CNTRCT_NUM AS CNTRCT_NBR_OF_REC,
    C.CLM_CNTRCT_OF_REC_PBP_NUM AS PBP_NBR_OF_REC,
    TO_CHAR(CL.CLM_LINE_FROM_DT, 'YYYYMMDD') AS DATE_OF_SERVICE,
    RPAD(C.CLM_CNTL_NUM,40,' ') AS CLAIM_CONTROL_NBR,
    RPAD(C.CLM_CARDHLDR_ID,20,' ') AS CARDHOLDER_ID, 
    RPAD(C.CLM_SRVC_PRVDR_GNRC_ID_NUM,20,' ') AS CLM_SRVC_PROVIDER_GNRC_ID,
    C.PRVDR_SRVC_ID_QLFYR_CD AS CLM_SRVC_PROVIDER_ID_QUAL,

    -- Note:  Requester always wants Prescribing Physician NPI number populated in the field CLM_PRSBNG_PRVDR_GNRC_ID_NUM. 
    -- This means the associated field PRVDR_PRSBNG_ID_QLFYR_CD should always be populated with ‘01’.  
    -- If the Prescribing Physician field is not populated on the IDR, we will leave those two fields blank unless told otherwise.

    --****\/****
    --OLD: X(20) X(20) 	NEW: X(35) X(35)
    --RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,20,' ') AS CLM_PRSBNG_PRVDR_GNRC_ID_NUM,
      RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,35,' ') AS CLM_PRSBNG_PRVDR_GNRC_ID_NUM,
    --****/\****	  
            
    CASE WHEN RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,20,' ') > '  ' 
       THEN C.PRVDR_PRSBNG_ID_QLFYR_CD 
       ELSE '  '
    END AS PRVDR_PRSBNG_ID_QLFYR_CD,
    LPAD(C.CLM_SBMT_FRMT_CD,1,' ') AS NON_STAND_FRMT_CD,
    RPAD(CL.CLM_LINE_RX_NUM,30,' ') AS PRES_SVC_REF_NO,
    LPAD(CLR.CLM_LINE_RX_FILL_NUM,9,'0') AS CLM_LINE_RX_FILL_NUM,
    RPAD(TO_CHAR(CL.CLM_LINE_SRVC_UNIT_QTY,'FM00000000000000.0000'),20,' ') AS QUANITIY_DISPENSED,
    RPAD(COALESCE(CL.CLM_LINE_NDC_CD,''),11,' ') AS NDC_DRUG_CD,
    COALESCE(FDB.NDC_DRUG_FORM_CD,' ') AS NDC_DRUG_FORM_CD,
    LPAD(CLR.CLM_LINE_DAYS_SUPLY_QTY, 9, '0') DAYS_SUPLY, 
    RPAD(TO_CHAR(CL.CLM_LINE_BENE_PMT_AMT, 'FM000000000.00'),13,' ') AS BENE_PAYMENT_AMT,
    RPAD(TO_CHAR(CL.CLM_LINE_OTHR_TP_PD_AMT, 'FM0000000000000.00'),17,' ') AS BENE_OTHER_TP_AMT,
    RPAD(TO_CHAR(CL.CLM_LINE_CVRD_PD_AMT, 'FM000000000.00'),13,' ') AS CLM_CVRD_D_PLAN_PAID,
    
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CL.CLM_LINE_NCVRD_PD_AMT, 'FM0000000.00'),11,' ') 	AS CLM_NON_CVRD_PLAN_PAID,
           TO_CHAR(CL.CLM_LINE_NCVRD_PD_AMT, 'MI000000000.00') 			AS CLM_NON_CVRD_PLAN_PAID,
    --****/\****	  
            
    RPAD(COALESCE(CLR.CLM_DSPNSNG_STUS_CD,' '),1,' ') AS DISPENSING_STUS_CD,
    CLR.CLM_CMPND_CD AS COMPOUND_CD,
    CASE WHEN CLR.CLM_FRMLRY_CD = '~' 
       THEN ' ' 
       ELSE COALESCE(CLR.CLM_FRMLRY_CD, ' ') END AS FRMLRY_CD,


    LPAD(TO_CHAR(CLR.CLM_LINE_FRMLRY_TIER_LVL_ID),2,'0') AS FRMLRY_TIER,
    LPAD(CLR.CLM_DAW_PROD_SLCTN_CD,1,' ') AS DAW_PROD_SLCTN_CD,
    LPAD(CLR.CLM_CTSTRPHC_CVRG_IND_CD,1,' ') AS CAT_COV_CD,
    LPAD(CLR.CLM_PRCNG_EXCPTN_CD,1,' ') AS PRCNG_EXCPTN_CD,
    LPAD(CLR.CLM_DRUG_CVRG_STUS_CD,1,' ') AS DRUG_CVRG_STUS_CD,
    CLR.CLM_RSN_CD AS CLM_REASON_CD,
    CASE WHEN CLR.CLM_LINE_RX_ORGN_CD = '~' 
       THEN ' '  
       ELSE LPAD(CLR.CLM_LINE_RX_ORGN_CD,1,' ') END AS CLM_RX_ORIGIN_CD,
    CASE WHEN CLR.CLM_BRND_GNRC_CD = '~' 
       THEN ' '  
       ELSE LPAD(CLR.CLM_BRND_GNRC_CD,1,' ') END AS CLM_BRND_GNRC_CD,
    CASE WHEN CLR.CLM_BGN_BNFT_PHASE_CD = '~' 
       THEN ' '  
       ELSE LPAD(CLR.CLM_BGN_BNFT_PHASE_CD,1,' ') END AS CLM_BGN_BNFT_PHASE,
    CASE WHEN CLR.CLM_END_BNFT_PHASE_CD = '~' 
       THEN ' ' 
       ELSE LPAD(CLR.CLM_END_BNFT_PHASE_CD,1,' ') END AS CLM_END_BNFT_PHASE,
    CASE WHEN CLR.CLM_PTNT_RSDNC_CD = '~' 
       THEN '  '  
       ELSE LPAD(CLR.CLM_PTNT_RSDNC_CD,2,' ') END AS CLM_PTNT_RSDNC_CD,
    CASE WHEN CLR.CLM_PHRMCY_SRVC_TYPE_CD = '~' 
       THEN '  ' 
       ELSE LPAD(CLR.CLM_PHRMCY_SRVC_TYPE_CD,2,' ') END AS CLM_PHRMCY_SRVC_TYPE_CD,
       
    --****\/****			
    --OLD: X(2) X(2) 	NEW: X(3) X(3)			   
    --CASE WHEN CLR.CLM_LTC_DSPNSNG_MTHD_CD  = '~' 
    --   THEN '  ' 
    --   ELSE LPAD(CLR.CLM_LTC_DSPNSNG_MTHD_CD,2,' ') END AS CLM_LTC_DSPNSNG_MTHD_CD,
      CASE WHEN SUBSTR(CLR.CLM_LTC_DSPNSNG_MTHD_CD,1,1)  = '~' 
         THEN '   ' 
         ELSE LPAD(CLR.CLM_LTC_DSPNSNG_MTHD_CD,3,' ') END AS CLM_LTC_DSPNSNG_MTHD_CD,
    --****/\****	  
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99				 
    --RPAD(TO_CHAR(CLR.CLM_LINE_INGRDNT_CST_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_INGRDNT_CST_AMT,
           TO_CHAR(CLR.CLM_LINE_INGRDNT_CST_AMT, 'MI000000000.00') 			AS CLM_LINE_INGRDNT_CST_AMT,
    --****/\****	  

    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99			
    --RPAD(TO_CHAR(CLR.CLM_LINE_SRVC_CST_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_SRVC_CST_AMT,
           TO_CHAR(CLR.CLM_LINE_SRVC_CST_AMT, 'MI000000000.00')  		AS CLM_LINE_SRVC_CST_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_SLS_TAX_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_SLS_TAX_AMT,
           TO_CHAR(CLR.CLM_LINE_SLS_TAX_AMT, 'MI000000000.00')   		AS CLM_LINE_SLS_TAX_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(8,2) -9(6)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT, 'FM000000.00'),10,' ') AS CLM_LINE_BELOW_THRHLD_AMT,
           TO_CHAR(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT, 'MI000000000.00')  	AS CLM_LINE_BELOW_THRHLD_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(8,2) -9(6)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT, 'FM000000.00'),10,' ') 	AS CLM_LINE_GRS_ABOVE_THRSHLD_AMT,
           TO_CHAR(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT, 'MI000000000.00')  		AS CLM_LINE_GRS_ABOVE_THRSHLD_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_LIS_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_LIS_AMT,
           TO_CHAR(CLR.CLM_LINE_LIS_AMT, 'MI000000000.00')  		AS CLM_LINE_LIS_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_PLRO_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_PLRO_AMT,
           TO_CHAR(CLR.CLM_LINE_PLRO_AMT, 'MI000000000.00')  		AS CLM_LINE_PLRO_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_VCCN_ADMIN_FEE_AMT,
           TO_CHAR(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT, 'MI000000000.00')    		AS CLM_LINE_VCCN_ADMIN_FEE_AMT,
    --****/\****	  
    
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_GAP_DSCNT_AMT,
           TO_CHAR(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT, 'MI000000000.00')  			AS CLM_LINE_GAP_DSCNT_AMT,
    --****/\****	  

         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_GRS_TOT_CVRD_AMT,
           TO_CHAR(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT, 'MI000000000.00')  		AS CLM_LINE_GRS_TOT_CVRD_AMT,
    --****/\****	  
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_TROOP_TOT_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_TROOP_TOT_AMT,
           TO_CHAR(CLR.CLM_LINE_TROOP_TOT_AMT, 'MI000000000.00')  		AS CLM_LINE_TROOP_TOT_AMT,
    --****/\****	  
         
    --****\/****			
    --OLD: NUMBER(9,2) -9(7)v99 	NEW: NUMBER(11,2) S9(9)v99		
    --RPAD(TO_CHAR(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT, 'FM0000000.00'),11,' ') 	AS CLM_LINE_PTAP_CMS_GAP_DSCNT_AMT,
           TO_CHAR(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT, 'MI000000000.00')   		AS CLM_LINE_PTAP_CMS_GAP_DSCNT_AMT,
    --****/\****	  
    
    TO_CHAR(CLR.CLM_LINE_ORGNL_RCVD_DT, 'YYYYMMDD') AS CLMLINE_ORGNL_RECD_DT,
    TO_CHAR(CDS.CLM_CMS_PROC_DT, 'YYYYMMDD') AS PDE_PROCESSING_DT,
    TO_CHAR(CDS.CLM_SCHLD_PMT_DT, 'YYYYMMDD') AS CLM_SCHLD_PMT_DT,
    TO_CHAR(CDS.CLM_PD_DT, 'YYYYMMDD') AS CLM_PD_DT,
    RPAD(TO_CHAR(CLR.CLM_LINE_ADJDCTN_BGN_TS, 'YYYYMMDDHHMISS'),16,' ') AS CLM_LINE_ADJ_BGN_TS,
    LPAD(C.CLM_FINL_ACTN_IND,1,' ') AS CLM_FINAL_ACTION_IND,
    'END' AS END_REC
    FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

    /* BENEFICIARY INFORMATION */
    INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE B
       ON C.BENE_SK        = B.BENE_SK

    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
       ON C.CLM_NUM_SK      = CL.CLM_NUM_SK
      AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
      AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
      AND C.GEO_BENE_SK     = CL.GEO_BENE_SK
      AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX CLR
       ON CL.CLM_NUM_SK      = CLR.CLM_NUM_SK
      AND CL.CLM_TYPE_CD     = CLR.CLM_TYPE_CD
      AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
      AND CL.GEO_BENE_SK     = CLR.GEO_BENE_SK
      AND CL.CLM_LINE_NUM    = CLR.CLM_LINE_NUM

    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
       ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

    /* DRUG NATIONAL DRUG CODE FIRST DATA BANK */
    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_NDC_{ENVNAME}.NDC_FDB FDB
      ON CL.CLM_LINE_NDC_CD = FDB.NDC_CD

    /* USPS ZIP CODE */
    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP5_CD ZIP5
      ON B.GEO_SK          = ZIP5.GEO_SK

    /* STATE CODE */
    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_FIPS_CNTY_CD G
      ON ZIP5.GEO_FIPS_CNTY_CD   = G.GEO_FIPS_CNTY_CD
     AND ZIP5.GEO_FIPS_STATE_CD  = G.GEO_FIPS_STATE_CD
     
    WHERE C.CLM_TYPE_CD IN (1,2,4)
      --Final Action Paid or Denied Claims including denial for claims due to not obtaining prior authorization
       AND C.CLM_FINL_ACTN_IND = 'Y'

      AND CL.CLM_LINE_FROM_DT BETWEEN to_date('{FROM_DT}','YYYY-MM-DD') AND to_date('{TO_DT}','YYYY-MM-DD')
      AND CL.CLM_LINE_NDC_CD in ({NDC_CODES})

      -- Sovaldi      
      --AND CL.CLM_LINE_FROM_DT BETWEEN to_date('2013-12-06','YYYY-MM-DD') AND to_date('2022-12-31','YYYY-MM-DD')
      --AND CL.CLM_LINE_NDC_CD in ('61958150101','61958150301','61958150401','61958150501')
      -- Harvoni  
      --AND CL.CLM_LINE_FROM_DT BETWEEN to_date('2014-10-10','YYYY-MM-DD') AND to_date('2022-12-31','YYYY-MM-DD')
      --AND CL.CLM_LINE_NDC_CD in ('61958180101','61958180201','61958180301','61958180401','61958180501')
                     
                ) FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER='|' ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE) 
                  SINGLE=TRUE max_file_size=5368709120 """,con,exit_on_error=True)

                  
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
