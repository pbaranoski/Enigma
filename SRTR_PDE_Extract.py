#!/usr/bin/env python
########################################################################################################
# Name:  SRTR_PDE_Extract.py
# DESC:  This python script extracts data from IDRC for the SRTR PDE extracts
#
# Created: Viren Khanna
# Modified: 02/13/2023
#
#Sumathi Gayam  2023-06-06  Changed the file name to match the EFT name 
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

EXT_YEAR = os.getenv('EXT_YEAR')
ENVNAME = os.getenv('ENVNAME') 
TMSTMP = os.getenv('TMSTMP')
CLM_TYPE = os.getenv('CLM_TYPE')
CLM_TYPE_CD= os.getenv('CLM_TYPE_CD')
SRTR_PDE_BUCKET = os.getenv('SRTR_PDE_BUCKET')

# Get the last two digits of the year for the extract filename
YY = EXT_YEAR[2:]

 
########################################################################################################
# Execute extract based on parameters set in the RUN section at the bottom of the script
########################################################################################################
def execute_pde_extract():
    # boolean - Python Exception status
    bPythonExceptionOccurred=False

    try:
        snowconvert_helpers.configure_log()
        con = snowconvert_helpers.log_on()
        snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
        snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

        ########################################################################################################
        # Extract SRTR PDE data and write to S3 as a flat file
        ########################################################################################################
        snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_SRTRPDE_STG/{XTR_FILE_NAME}
                FROM (
              
SELECT DISTINCT
   'START' AS st_of_file
  ,'|' 
 ,TO_CHAR(CL.CLM_TYPE_CD,'FM00000')        AS CLM_TYPE_CD
 ,'|' 
  ,RPAD(COALESCE(C.CLM_HIC_NUM,' '),12,' ')    AS BENE_HICN
 ,'|' 
  ,RPAD(COALESCE(ZIP5.GEO_ZIP5_CD,' '),5,' ')   AS BENE_MAILING_ZIPCD
 ,'|' 
  ,RPAD(COALESCE(C.BENE_SEX_CD,' '),1,' ')   AS BENE_GENDER
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(B.BENE_BRTH_DT, 'YYYYMMDD'),' '),8,' ')  AS BENE_DOB
 ,'|' 
  ,RPAD(COALESCE(G.GEO_SSA_STATE_CD,' '),2,' ') AS SSA_STATE_CD
 ,'|' 
  ,RPAD(C.CLM_CNTRCT_OF_REC_CNTRCT_NUM,5,' ')   AS CNTRCT_NBR_OF_REC
 ,'|' 
  ,RPAD(C.CLM_CNTRCT_OF_REC_PBP_NUM,3,' ')  AS PBP_NBR_OF_REC
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(CL.CLM_LINE_FROM_DT ,'YYYYMMDD'),' '),8,' ')   AS DATE_OF_SERVICE
 ,'|' 
  ,RPAD(C.CLM_CNTL_NUM,40,' ')  AS CLAIM_CONTROL_NBR
 ,'|' 
 , RPAD(C.CLM_CARDHLDR_ID,20,' ')   AS CARDHOLDER_ID
 ,'|' 
 , RPAD(C.CLM_SRVC_PRVDR_GNRC_ID_NUM,20,' ') AS CLM_SRVC_PROVIDER_GNRC_ID
 ,'|' 
  ,RPAD(COALESCE(C.PRVDR_SRVC_ID_QLFYR_CD,' '),2,' ')   AS CLM_SRVC_PROVIDER_ID_QUAL
 ,'|' 
  ,RPAD(COALESCE(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,' '),20,' ')  AS CLM_PRESCRIBER_GNRC_ID
 ,'|' 
  ,RPAD(COALESCE(C.PRVDR_PRSBNG_ID_QLFYR_CD,' '),2,' ')   AS CLM_PRESCRIBER_ID_QUAL
 ,'|' 
  ,RPAD(COALESCE(C.CLM_SBMT_FRMT_CD,' '),1,' ')   AS NON_STAND_FRMT_CD
 ,'|' 
  ,RPAD(CL.CLM_LINE_RX_NUM,30,' ')  AS PRES_SVC_REF_NO
 ,'|' 
  ,RPAD(TO_CHAR(CLR.CLM_LINE_RX_FILL_NUM,'FM000000000'),9,' ') AS CLM_LINE_RX_FILL_NUM
 ,'|' 
 -- format string is 20 character but CAST is for 19 characters -9(14).9(4)
,TO_CHAR(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'MI00000000000000.0000')  AS QUANITIY_DISPENSED
 ,'|' 
 ,RPAD(COALESCE(CL.CLM_LINE_NDC_CD,' '),11,' ') AS NDC_DRUG_CD
 ,'|' 
   ,RPAD(COALESCE(FDB.NDC_DRUG_FORM_CD,' '),1,' ')   AS NDC_DRUG_FORM_CD
 ,'|' 
 ,RPAD(TO_CHAR(CLR.CLM_LINE_DAYS_SUPLY_QTY,'FM000000000'),9,' ') AS DAYS_SUPLY
 ,'|' 
  ,TO_CHAR(COALESCE(CL.CLM_LINE_BENE_PMT_AMT,0),'MI000000000.00') AS BENE_PAYMENT_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CL.CLM_LINE_OTHR_TP_PD_AMT,0),'MI0000000000000.00')  AS BENE_OTHER_TP_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CL.CLM_LINE_CVRD_PD_AMT,0),'MI000000000.00')  AS CLM_CVRD_D_PLAN_PAID
 ,'|' 
  ,TO_CHAR(COALESCE(CL.CLM_LINE_NCVRD_PD_AMT,0), 'MI0000000.00') AS CLM_NON_CVRD_PLAN_PAID
 ,'|' 
  ,RPAD(COALESCE(CLR.CLM_DSPNSNG_STUS_CD,' '),1,' ') AS DISPENSING_STUS_CD
 ,'|' 
  ,RPAD(COALESCE(CLR.CLM_CMPND_CD,' '),1,' ')   AS COMPOUND_CD
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_FRMLRY_CD,''),'~',''),1,' ') AS FRMLRY_CD
 ,'|' 
  ,TO_CHAR(CLR.CLM_LINE_FRMLRY_TIER_LVL_ID,'FM00') AS FRMLRY_TIER
 ,'|' 
  ,RPAD(COALESCE(CLR.CLM_DAW_PROD_SLCTN_CD,' '),1,' ')   AS DAW_PROD_SLCTN_CD
 ,'|' 
  ,RPAD(COALESCE(CLR.CLM_CTSTRPHC_CVRG_IND_CD,' '),1,' ')  AS CAT_COV_CD
 ,'|' 
  ,RPAD(COALESCE(CLR.CLM_PRCNG_EXCPTN_CD,' '),1,' ') AS PRCNG_EXCPTN_CD
 ,'|' 
   ,RPAD(COALESCE(CLR.CLM_DRUG_CVRG_STUS_CD,' '),1,' ') AS DRUG_CVRG_STUS_CD
 ,'|' 
  ,RPAD(COALESCE(CLR.CLM_RSN_CD,' '),1,' ')  AS CLM_REASON_CD
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_LINE_RX_ORGN_CD,''),'~',''),1,' ')  AS CLM_RX_ORIGIN_CD
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_BRND_GNRC_CD,''),'~',''),1,' ')  AS CLM_BRND_GNRC_CD
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_BGN_BNFT_PHASE_CD,''),'~',''),1,' ')  AS CLM_BGN_BNFT_PHASE
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_END_BNFT_PHASE_CD,''),'~',''),1,' ')  AS CLM_END_BNFT_PHASE
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_PTNT_RSDNC_CD,''),'~',''),2,' ')   AS CLM_PTNT_RSDNC_CD
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_PHRMCY_SRVC_TYPE_CD,''),'~',''),2,' ')  AS CLM_PHRMCY_SRVC_TYPE_CD
 ,'|' 
  ,RPAD(REPLACE(COALESCE(CLR.CLM_LTC_DSPNSNG_MTHD_CD,''),'~',''),2,' ') AS CLM_LTC_DSPNSNG_MTHD_CD
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_INGRDNT_CST_AMT,0),'MI0000000.00') AS CLM_LINE_INGRDNT_CST_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_SRVC_CST_AMT,0),'MI0000000.00')  AS CLM_LINE_SRVC_CST_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_SLS_TAX_AMT,0),'MI0000000.00') AS CLM_LINE_SALES_TAX_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,0),'MI000000.00')  AS CLM_LINE_BELOW_THRHLD_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,0),'MI000000.00') AS CLM_LINE_ABOVE_THRHLD_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_LIS_AMT,0),'MI0000000.00')  AS CLM_LINE_LIS_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_PLRO_AMT,0),'MI0000000.00') AS CLM_LINE_PLRO_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT,0),'MI0000000.00') AS CLM_LINE_VCCN_ADMIN_FEE_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,0),'MI0000000.00')  AS CLM_LINE_GAP_DSCNT_AMT
 ,'|' 
   ,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT,0),'MI0000000.00')  AS CLM_LINE_GRS_TOT_CVRD_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_TROOP_TOT_AMT,0),'MI0000000.00')  AS   CLM_LINE_TROOP_TOT_AMT
 ,'|' 
  ,TO_CHAR(COALESCE(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT,0),'MI0000000.00')    AS  CLM_LINE_PTAP_CMS_GAP_DSCNT_AMT
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(CLR.CLM_LINE_ORGNL_RCVD_DT,'YYYYMMDD'),' '),8,' ') AS  CLMLINE_ORGNL_RECD_DT
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(CDS.CLM_CMS_PROC_DT,'YYYYMMDD'),' '),8,' ')  AS  PDE_PROCESSING_DT
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(CDS.CLM_SCHLD_PMT_DT,'YYYYMMDD'),' '),8,' ') AS CLM_SCHLD_PMT_DT
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(CDS.CLM_PD_DT,'YYYYMMDD'),' '),8,' ')  AS CLM_PD_DT
 ,'|' 
  ,RPAD(COALESCE(TO_CHAR(CLR.CLM_LINE_ADJDCTN_BGN_TS,'YYYYMMDDHHMISS'),' '),16,' ') AS  CLM_LINE_ADJ_BGN_TS
 ,'|' 
  ,RPAD(COALESCE(C.CLM_FINL_ACTN_IND,' '),1,' ')  AS  CLM_FINAL_ACTION_IND
 ,'|' 
  ,'END' AS END_OF_FILE

  FROM
/* Claim Header */
  IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C
/* Claim Line */
  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
       ON  C.GEO_BENE_SK           = CL.GEO_BENE_SK
   AND C.CLM_DT_SGNTR_SK  = CL.CLM_DT_SGNTR_SK
   AND C.CLM_TYPE_CD            = CL.CLM_TYPE_CD
   AND C.CLM_NUM_SK             = CL.CLM_NUM_SK
   AND C.CLM_FROM_DT = CL.CLM_FROM_DT
/* PDE Claim Line Detail */
  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX CLR
       ON  CL.CLM_NUM_SK            = CLR.CLM_NUM_SK
     AND CL.CLM_TYPE_CD           = CLR.CLM_TYPE_CD
     AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
     AND CL.GEO_BENE_SK          = CLR.GEO_BENE_SK
     AND CL.CLM_LINE_NUM         = CLR.CLM_LINE_NUM
/* Claim Date Signature SurrogateKey */
  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
       ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK
/* Drug National Drug Code First Data Bank */
  LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_NDC_{ENVNAME}.NDC_FDB FDB
     ON CL.CLM_LINE_NDC_CD = FDB.NDC_CD
/* Beneficiary Information */
  INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE B
       ON  C.BENE_SK= B.BENE_SK
/* USPS Zip Code */
  LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP5_CD ZIP5
     ON B.GEO_SK= ZIP5.GEO_SK
/* State Code */
  LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_FIPS_CNTY_CD G
    ON  ZIP5.GEO_FIPS_CNTY_CD   = G.GEO_FIPS_CNTY_CD
    AND ZIP5.GEO_FIPS_STATE_CD = G.GEO_FIPS_STATE_CD
/*FIRST FINDER FILE WITH BENES */
  INNER JOIN "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."SRTR_SSN" SRTR
   ON B.BENE_SSN_NUM = LTRIM(RTRIM(SRTR.SSN))

  WHERE
  C.CLM_TYPE_CD IN {CLM_TYPE_CD}
  AND  C.CLM_FINL_ACTN_IND='Y'
  AND C.CLM_FROM_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
                
                ) FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
                  SINGLE=TRUE max_file_size=5368709120 """, con, exit_on_error=True)
                  
    except Exception as e:
        print(e)
        # Let shell script know that python code failed.
        bPythonExceptionOccurred=True 
        
    finally:
        if con is not None:
            con.close()

        return bPythonExceptionOccurred

########################################################################################################
# RUN
# If CLM_TYPE is PDE, set range parms and loop to call the extracts

########################################################################################################
bErrorOccurred = False
print('')
print("Run date and time: " + date_time  )
print

if CLM_TYPE == "PDRX":
    start_date_parms = ['-01-01', '-04-01', '-07-01', '-10-01']
    end_date_parms = ['-03-31', '-06-30', '-09-30', '-12-31']
    
    for i in range(len(start_date_parms)):
        START_DATE = EXT_YEAR + start_date_parms[i]
        END_DATE = EXT_YEAR + end_date_parms[i]
        RNG = i + 1
        RNGLIT = f"{YY}P{RNG}"	
        XTR_FILE_NAME = f"SRTR_{CLM_TYPE}_Y{RNGLIT}_{TMSTMP}.txt.gz"
        
        bErrorOccurred = execute_pde_extract()

        # Let shell script know that python code failed.
        if bErrorOccurred == True:
            sys.exit(12)
   
        os.system(f"/app/IDRC/XTR/CMS/scripts/run/CombineS3Files.sh {SRTR_PDE_BUCKET} {XTR_FILE_NAME}")         		

else:
    # Invalid CTYP code supplied to the script
    sys.exit(12) 

snowconvert_helpers.quit_application()
