#!/usr/bin/env python
########################################################################################################
# Name:  SAF_PDE_Extract.py
# DESC:  This python script extracts data from IDRC for the SAF PDE extracts
#
# Created: Viren Khanna
# Modified: 04/07/2023
#
#Natalya Tinovsky	11/25/2024	Updated column lengths for : CLM_PRSBNG_PRVDR_GNRC_ID_NUM, CLM_LINE_SRVC_CST_AMT
#			, CLM_LINE_SLS_TAX_AMT, CLM_LINE_GRS_BLW_THRSHLD_AMT,CLM_LINE_GRS_ABOVE_THRSHLD_AMT
#			, CLM_LINE_LIS_AMT, CLM_LINE_PLRO_AMT, CLM_LINE_NCVRD_PD_AMT, CLM_LINE_VCCN_ADMIN_FEE_AMT, 
#			CLM_LINE_RPTD_GAP_DSCNT_AMT, CLM_LINE_GRS_CVRD_CST_TOT_AMT, CLM_LINE_TROOP_TOT_AMT, CLM_LTC_DSPNSNG_MTHD_CD
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

EXT_YR = os.getenv('EXT_YR')
CUR_YR = os.getenv('CUR_YR')
ENVNAME = os.getenv('ENVNAME')
TMSTMP = os.getenv('TMSTMP')
CLM_TYPE = os.getenv('CLM_TYPE')
CLM_TYPE_CD= os.getenv('CLM_TYPE_CD')
DATADIR = os.getenv('DATADIR')


 
########################################################################################################
# Execute extract based on parameters set in the RUN section at the bottom of the script
########################################################################################################
def execute_PDE_extract():
    # boolean - Python Exception status
    bPythonExceptionOccurred=False

    try:
        snowconvert_helpers.configure_log()
        con = snowconvert_helpers.log_on()
        snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
        snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

        ########################################################################################################
        # Extract SAF PDE data and write to S3 as a flat file
        ########################################################################################################
        snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_SAFPDE_STG/{XTR_FILE_NAME}
                FROM (
              
SELECT
    CASE C.CLM_TYPE_CD  WHEN 1 THEN ' ' WHEN 2 THEN 'A' WHEN 3 THEN 'D'
        WHEN 4 THEN 'R'  ELSE 'X' END AS  PTAP_ADJ_DEL_CD
    ,RPAD(COALESCE(CLR.CLM_DSPNSNG_STUS_CD,' '),1,' ') AS PTAP_DISP_STAT_CD
    ,RPAD(COALESCE(C.CLM_CNTL_NUM,' '),40,' ')  AS PTAP_RX_CLAIM_NUM
    ,RPAD(COALESCE(C.CLM_CARDHLDR_ID,' '),20,' ') AS PTAP_RX_CARDHOLDER_ID
    ,RPAD(COALESCE(CL.CLM_LINE_RX_NUM,' '),30,' ')  AS PTAP_RX_SERV_REF_NUM
    ,RPAD(COALESCE(TO_CHAR(C.CLM_FROM_DT,'YYYYMMDD'),' '),8,' ')    AS PTAP_RX_DOS_DT
    ,RPAD(TO_CHAR(CLR.CLM_LINE_RX_FILL_NUM,'FM000000000'),9,' ')  AS  PTAP_FILL_NUM
    ,RPAD(COALESCE(C.CLM_HIC_NUM,' '),20,' ') AS PTAP_INS_CLAIM_NUM
    ,'  '   AS PTAP_MEDICARE_STAT
    ,RPAD(COALESCE(TO_CHAR(C.CLM_PTNT_BIRTH_DT,'YYYYMMDD'),' '),8,' ')   AS  PTAP_PATIENT_DOB
    ,RPAD(COALESCE(C.BENE_SEX_CD,' '),1,' ') AS PTAP_PATIENT_GENDER
    ,'   ' AS PTAP_BENE_AGE
    ,'  '  AS PTAP_FIPS_STATE_CD
    ,'   ' AS  PTAP_FIPS_COUNTY_CD
    ,'  '  AS PTAP_SSA_STATE_CD
    ,'   ' AS PTAP_SSA_COUNTY_CD
    ,'         ' AS PTAP_ZIP_CD
    ,RPAD(COALESCE(CLR.CLM_CMPND_CD,' '),1,' ') AS PTAP_COMPUND_CD
    ,RPAD(COALESCE(CLR.CLM_DAW_PROD_SLCTN_CD,' '),1,' ') AS PTAP_DAW_CD
    ,TO_CHAR(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'MI00000000000000.0000') AS PTAP_QUANITIY_DISPENSED
    ,' ' AS PTAP_DRUG_FORM_CD
    ,RPAD(TO_CHAR(CLR.CLM_LINE_DAYS_SUPLY_QTY,'FM000000000'),9,' ') AS PTAP_DAYS_SUPPLY
    ,RPAD(COALESCE(CLR.CLM_CTSTRPHC_CVRG_IND_CD,' '),1,' ') AS PTAP_COVERAGE_CD
    ,RPAD(COALESCE(C.CLM_SBMT_FRMT_CD,' '),1,' ')  AS  PTAP_NON_STAND_FMT_CD
    ,RPAD(COALESCE(TO_CHAR(C.CLM_SCHLD_PMT_DT,'YYYYMMDD'),' '),8,' ') AS PTAP_PAID_DT
    ,RPAD(COALESCE(CLR.CLM_PRCNG_EXCPTN_CD,' '),1,' ') AS PTAP_PRICE_EXCEPT_CD
    ,RPAD(COALESCE(CLR.CLM_DRUG_CVRG_STUS_CD,' '),1,' ') AS PTAP_COVERAGE_STAT_CD
    ,RPAD(COALESCE(CL.CLM_LINE_NDC_CD,' '),11,' ')  AS PTAP_PROD_SERVICE_ID
    ,RPAD(COALESCE(C.CLM_SRVC_PRVDR_GNRC_ID_NUM,' '),20,' ') AS PTAP_SRVC_PROVIDER_ID
    ,RPAD(COALESCE(C.PRVDR_SRVC_ID_QLFYR_CD,' '),2,' ')   AS PTAP_SRVC_PROVIDER_ID_QUAL
    
    --****\/****
    --OLD: X(20) X(20) 				NEW: X(35) X(35)
    --,RPAD(COALESCE(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,' '),25,' ')	AS PTAP_PRESCRIBER_ID
      ,RPAD(COALESCE(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,' '),35,' ')	AS PTAP_PRESCRIBER_ID
    --****/\****
    
    ,RPAD(COALESCE(C.PRVDR_PRSBNG_ID_QLFYR_CD,' '),2,' ')  AS PTAP_PRESCRIBER_ID_QUAL
    ,RPAD(COALESCE(TO_CHAR(CDS.CLM_CMS_PROC_DT,'YYYYMMDD'),' '),8,' ') AS PTAP_PROCESS_DT
    ,RPAD(COALESCE(C.CLM_CNTRCT_OF_REC_CNTRCT_NUM,' '),5,' ')  AS PTAP_CNTRT_OF_REC
    ,RPAD(COALESCE(C.CLM_CNTRCT_OF_REC_PBP_NUM,' '),3,' ')  AS  PTAP_PBP_OF_REC
    ,RPAD(COALESCE(C.CLM_SBMTR_CNTRCT_NUM,' '),5,' ') AS  PTAP_CONTRACT_NUM
    ,RPAD(COALESCE(C.CLM_SBMTR_CNTRCT_PBP_NUM,' '),3,' ') AS  PTAP_PBP_ID
    ,RPAD(COALESCE(CLR.CLM_RSN_CD,' '),1,' ') AS PTAP_P2P_RSN_CODE
    ,RPAD(COALESCE(C.CLM_OTHR_PRVDR_GNRC_ID_NUM,' '),15,' ') AS PTAP_ALT_SRVC_PROV_ID
    ,RPAD(COALESCE(C.PRVDR_OTHR_ID_QLFYR_CD,' '),2,' ')  AS  PTAP_ALT_SRVC_PROV_ID_QUAL
                
    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_INGRDNT_CST_AMT,'MI0000000.00') 	AS PTAP_INGRDNT_COST_PD
      ,TO_CHAR(CLR.CLM_LINE_INGRDNT_CST_AMT,'MI000000000.00') 	AS PTAP_INGRDNT_COST_PD
    --****/\****

    --****\/****			
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_SRVC_CST_AMT,'MI000000000.00') AS PTAP_DSPNSNG_FEE_PD
      ,TO_CHAR(CLR.CLM_LINE_SRVC_CST_AMT,'MI000000000.00') AS PTAP_DSPNSNG_FEE_PD
    --****/\****

    --****\/****			
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_SLS_TAX_AMT,'MI0000000.00') 	AS PTAP_AMT_SALES_TAX
      ,TO_CHAR(CLR.CLM_LINE_SLS_TAX_AMT,'MI000000000.00') 	AS PTAP_AMT_SALES_TAX
    --****/\****

    --****\/****			
    --OLD: NUMBER(8,2) S9(6)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,'MI000000.00') 		AS PTAP_BELOW_OOP_THRHLD
      ,TO_CHAR(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,'MI000000000.00') 	AS PTAP_BELOW_OOP_THRHLD
    --****/\****

    --****\/****			
    --OLD: NUMBER(8,2) S9(6)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,'MI000000.00') 	AS PTAP_ABOVE_OOP_THRHLD
      ,TO_CHAR(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,'MI000000000.00') AS PTAP_ABOVE_OOP_THRHLD
    --****/\****
    
    ,TO_CHAR(CL.CLM_LINE_BENE_PMT_AMT,'MI000000000.00') AS PTAP_PATIENT_PAY_AMT
    ,TO_CHAR(CL.CLM_LINE_OTHR_TP_PD_AMT,'MI0000000000000.00') AS PTAP_OTHER_TROOP_AMT

    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_LIS_AMT,'MI0000000.00') AS PTAP_LICS_AMT
      ,TO_CHAR(CLR.CLM_LINE_LIS_AMT,'MI000000000.00') AS PTAP_LICS_AMT
    --****/\****

    --****\/****			
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_PLRO_AMT,'MI0000000.00') 	AS PTAP_PLRO_AMT
      ,TO_CHAR(CLR.CLM_LINE_PLRO_AMT,'MI000000000.00') 	AS PTAP_PLRO_AMT
    --****/\****			
    
    ,TO_CHAR(CL.CLM_LINE_CVRD_PD_AMT,'MI000000000.00') AS PTAP_CVRD_D_PLAN_PAID

    --****\/****			
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CL.CLM_LINE_NCVRD_PD_AMT,'MI0000000.00')  	AS PTAP_NON_CVRD_PLAN_PAID
      ,TO_CHAR(CL.CLM_LINE_NCVRD_PD_AMT,'MI000000000.00')  	AS PTAP_NON_CVRD_PLAN_PAID
    --****/\****
    
    ,'        'AS PTAP_REBATE_PASS_THRU
    ,RPAD(TO_CHAR(C.BENE_LINK_KEY,'FM0000000000'),10,' ')   AS PTAP_BENE_LINK_KEY

    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT,'MI0000000.00') 		AS CLM_LINE_VCCN_ADMIN_FEE_AMT
      ,TO_CHAR(CLR.CLM_LINE_VCCN_ADMIN_FEE_AMT,'MI000000000.00') 	AS CLM_LINE_VCCN_ADMIN_FEE_AMT
    --****/\****
    
    ,RPAD(COALESCE(CLR.CLM_LINE_RX_ORGN_CD,' '),1,' ') AS PTAP_PRESC_ORIGIN
    ,RPAD(COALESCE(TO_CHAR(CLR.CLM_LINE_ORGNL_RCVD_DT,'YYYYMMDD'),' '),8,' ') AS PTAP_PRG_RECD_DT
    ,RPAD(COALESCE(TO_CHAR(CLR.CLM_LINE_ADJDCTN_BGN_TS,'YYYYMMDDHHMISS'),' '),16,' ') AS PTAP_ADJ_TS

    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,'MI0000000.00') 		AS PTAP_GAP_DSCNT_AMT
      ,TO_CHAR(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,'MI000000000.00') 	AS PTAP_GAP_DSCNT_AMT
    --****/\****
    
    ,RPAD(COALESCE(CLR.CLM_FRMLRY_CD,' '),1,' ') AS PTAP_FRMLRY_CD

    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT,'MI0000000.00') 	AS PTAP_TOT_CVRD_DRG_ACC
      ,TO_CHAR(CLR.CLM_LINE_GRS_CVRD_CST_TOT_AMT,'MI000000000.00') 	AS PTAP_TOT_CVRD_DRG_ACC
    --****/\****

    --****\/****			
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_TROOP_TOT_AMT,'MI0000000.00')   		AS  PTAP_TROOP_ACC
      ,TO_CHAR(CLR.CLM_LINE_TROOP_TOT_AMT,'MI000000000.00')  		AS  PTAP_TROOP_ACC
    --****/\****
    
    ,RPAD(COALESCE(CLR.CLM_BRND_GNRC_CD,' '),1,' ')  AS PTAP_BRND_GNRC_CD
    ,RPAD(COALESCE(CLR.CLM_BGN_BNFT_PHASE_CD,' '),1,' ') AS PTAP_BGN_BNFT_PHASE
    ,RPAD(COALESCE(CLR.CLM_END_BNFT_PHASE_CD,' '),1,' ') AS PTAP_END_BNFT_PHASE
    ,RPAD(TO_CHAR(CLR.CLM_LINE_FRMLRY_TIER_LVL_ID,'FM00'),2,' ')  AS PTAP_FRMLRY_TIER

    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,TO_CHAR(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT,'MI0000000.00')   	AS  PTAP_CMS_GAP_DSCNT_AMT
      ,TO_CHAR(CLR.CLM_LINE_CALCD_GAP_DSCNT_AMT,'MI000000000.00')   AS  PTAP_CMS_GAP_DSCNT_AMT
    --****/\****
    
    ,' ' AS PTAP_GAP_DSCNT_OVRRD_CD
    ,RPAD(COALESCE(CLR.CLM_PTNT_RSDNC_CD,' '),2,' ')  as PTAP_PTNT_RSDNC_CD
    ,RPAD(COALESCE(CLR.CLM_PHRMCY_SRVC_TYPE_CD,' '),2,' ') as PTAP_PHRMCY_SRVC_TYPE_CD

    --****\/****
    --OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
    --,RPAD(COALESCE(CLR.CLM_LTC_DSPNSNG_MTHD_CD,' '),2,' ') AS PTAP_SUB_CLA_CD
      ,RPAD(COALESCE(CLR.CLM_LTC_DSPNSNG_MTHD_CD,' '),3,' ') AS PTAP_SUB_CLA_CD
    --****/\****
        
  FROM
  IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE  CL
   ON C.GEO_BENE_SK = CL.GEO_BENE_SK
   AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
   AND C.CLM_TYPE_CD = CL.CLM_TYPE_CD
   AND C.CLM_NUM_SK = CL.CLM_NUM_SK
   AND C.CLM_FROM_DT = CL.CLM_FROM_DT

  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX CLR
   ON CL.GEO_BENE_SK = CLR.GEO_BENE_SK
   AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
   AND CL.CLM_TYPE_CD = CLR.CLM_TYPE_CD
   AND CL.CLM_NUM_SK = CLR.CLM_NUM_SK
   AND CL.CLM_LINE_NUM = CLR.CLM_LINE_NUM

 INNER JOIN  IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR   CDS
   ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK


 WHERE
   C.CLM_TYPE_CD IN (1,2,4)
   AND C.CLM_FROM_DT BETWEEN to_date('{START_DATE}','YYYY-MM-DD') AND to_date('{END_DATE}','YYYY-MM-DD')
   AND C.CLM_EFCTV_DT <=  '{CUR_YR}-06-30'
   AND C.CLM_OBSLT_DT >   '{CUR_YR}-06-30'
                
                ) FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
                  max_file_size=5368709120 """, con, exit_on_error=True)
                  
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
FILE_LIST = open(f"{DATADIR}temp_SAF_PDE_Files.txt", "a")
print('')
print("Run date and time: " + date_time  )
print

if CLM_TYPE == "PDE":
    ext_months = [
        {"ext_month":"JAN",
         "ext_month_no": "-01",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        },
        {"ext_month":"FEB",
         "ext_month_no": "-02",
         "start_dates": ["-01"],
         "end_date": ["-28"]
        },
        {"ext_month":"MAR",
         "ext_month_no": "-03",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        },
        {"ext_month":"APR",
         "ext_month_no": "-04",
         "start_dates": ["-01"],
         "end_date": ["-30"]
        },
        {"ext_month":"MAY",
         "ext_month_no": "-05",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        },
        {"ext_month":"JUN",
         "ext_month_no": "-06",
         "start_dates": ["-01"],
         "end_date": ["-30"]
        },
        {"ext_month":"JUL",
         "ext_month_no": "-07",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        },
        {"ext_month":"AUG",
         "ext_month_no": "-08",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        },
        {"ext_month":"SEP",
         "ext_month_no": "-09",
         "start_dates": ["-01"],
         "end_date": ["-30"]
        },
        {"ext_month":"OCT",
         "ext_month_no": "-10",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        },
        {"ext_month":"NOV",
         "ext_month_no": "-11",
         "start_dates": ["-01"],
         "end_date": ["-30"]
        },
        {"ext_month":"DEC",
         "ext_month_no": "-12",
         "start_dates": ["-01"],
         "end_date": ["-31"]
        }  
    ]

    for i in range(len(ext_months)):
        for j in range(len(ext_months[i]["start_dates"])):
            RNG = j + 1
            START_DATE = EXT_YR + ext_months[i]["ext_month_no"] + ext_months[i]["start_dates"][j]

            if ext_months[i]['ext_month'] == "FEB" and RNG == 4:
                is_leap_year = calendar.isleap(int(EXT_YR))
                if is_leap_year == True:
                    END_DATE = EXT_YR + ext_months[i]["ext_month_no"] + "-29"
                else:
                    END_DATE = EXT_YR + ext_months[i]["ext_month_no"] + ext_months[i]["end_date"][j]
            else:
                END_DATE = EXT_YR + ext_months[i]["ext_month_no"] + ext_months[i]["end_date"][j]
            
            XTR_FILE_NAME = f"SAF_{CLM_TYPE}_Y{EXT_YR}_{ext_months[i]['ext_month']}_{TMSTMP}.csv.gz"
        
            bErrorOccurred = execute_PDE_extract()

            FILE_LIST.write(f"{XTR_FILE_NAME}\n")
            		

else:
    # Invalid CTYP code supplied to the script
    sys.exit(12) 
    FILE_LIST.close() 

FILE_LIST.close()


snowconvert_helpers.quit_application()
