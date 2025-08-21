#!/usr/bin/env python
########################################################################################################
# Name:  RAND_PDE_Extract.py
# DESC:  This python script extracts data from IDRC for the RAND PDE extracts
#
# Created: Viren Khanna
# Modified: 03/06/2023
#
# Paul Baranoski    2024-03-13 Add ${TMSTMP} to temp_RAND_PARTA_Files_${TMSTMP}.txt. When Part A or B jobs
#                              are run concurrently, the later job over-writes the temp file. The presence 
#                              of the timestamp will all for jobs to be run concurrently.
#	
#                              Change table IDRC_{ENVNAME}.CMS_DIM_PROD_{ENVNAME}.PROD_NDC_EFCTV to 
#                              IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_PROD_NDC_EFCTV since 
#                              the former table does not exist in Snowflake.  
# Natalya Tinovsky  2024-11-17  Increased length for following fields:
#				                CLM_LINE_INGRDNT_CST_AMT,CLM_LTC_DSPNSNG_MTHD_CD,CLM_PRSBNG_PRVDR_GNRC_ID_NUM ,
#				                CLM_LINE_GRS_BLW_THRSHLD_AMT,CLM_LINE_GRS_ABOVE_THRSHLD_AMT,CLM_LINE_LIS_AMT,
#				                CLM_LINE_PLRO_AMT,CLM_LINE_NCVRD_PD_AMT,CLM_LINE_RPTD_GAP_DSCNT_AMT
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
        # Extract RAND PDE data and write to S3 as a flat file
        ########################################################################################################
        snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_RANDPDE_STG/{XTR_FILE_NAME}
                FROM (
              
 SELECT DISTINCT
		RPAD(COALESCE(C.CLM_HIC_NUM,' '),12,' ')    AS HIC_NUM
	   ,RPAD(COALESCE(PRVDR.PRVDR_NCPDP_ID,' '),7,' ')  as PRVDR_NCPDP_ID
	   ,RPAD(COALESCE(TO_CHAR(CL.CLM_LINE_FROM_DT ,'YYYYMMDD'),' '),8,' ') AS SRVC_DT
	   ,RPAD(COALESCE(C.CLM_SRVC_PRVDR_GNRC_ID_NUM,' '),20,' ')  AS PROD_SRVC_ID
	   ,TO_CHAR(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'MI00000000000000.0000')  AS QTY_DSPNSD_NUM
	   ,CASE WHEN clr.CLM_LINE_DAYS_SUPLY_QTY IS NULL THEN '         ' ELSE
			TO_CHAR(clr.CLM_LINE_DAYS_SUPLY_QTY,'FM000000000') END AS DAYS_SUPLY_NUM
		,TO_CHAR(COALESCE(CL.CLM_LINE_BENE_PMT_AMT,0),'MI000000000.00') AS PTNT_PAY_AMT

		--****\/****
		--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CLR.CLM_LINE_INGRDNT_CST_AMT,0),'MI0000000.00') 	AS   TOT_RX_CST_AMT
		  ,TO_CHAR(COALESCE(CLR.CLM_LINE_INGRDNT_CST_AMT,0),'MI000000000.00') 	AS   TOT_RX_CST_AMT
		--****/\****


		,RPAD(COALESCE(TO_CHAR( C.CLM_PTNT_BIRTH_DT,'YYYYMMDD'),' '),8,' ') AS DOB_DT
		,RPAD(COALESCE(C.BENE_SEX_CD,' '),1,' ')  AS GNDR_CD
		,RPAD(COALESCE(CLR.CLM_PTNT_RSDNC_CD,' '),2,' ')  AS PTNT_RSDNC_CD

		--****\/****
		--OLD: X(2) X(2) 	NEW: X(3) X(3)
		--,RPAD(COALESCE(CLR.CLM_LTC_DSPNSNG_MTHD_CD,' '),2,' ') AS  SUBMSN_CLR_CD
		  ,RPAD(COALESCE(CLR.CLM_LTC_DSPNSNG_MTHD_CD,' '),3,' ') AS  SUBMSN_CLR_CD
		--****/\****

		,SUBSTR(CLR.CLM_DRUG_CVRG_STUS_CD,1,1) AS  DRUG_CVRG_STUS_CD
		,RPAD(COALESCE(CLR.CLM_BRND_GNRC_CD,' '),1,' ') AS BRND_GNRC_CD
		,RPAD(COALESCE(CLR.CLM_CMPND_CD,' '),1,' ') AS CMPND_CD
		,RPAD(COALESCE(CLR.CLM_DAW_PROD_SLCTN_CD,' '),1,' ') AS DAW_PROD_SLCTN_CD
		,RPAD(COALESCE(CLR.CLM_DSPNSNG_STUS_CD,' '),1,' ') AS DSPNSNG_STUS_CD
		,RPAD(TO_CHAR(CLR.CLM_LINE_RX_FILL_NUM,'FM000000000'),9,' ') AS FILL_NUM
		,CASE C.CLM_TYPE_CD  WHEN 1 THEN ' ' WHEN 2 THEN 'A' WHEN 3 THEN 'D'
		   WHEN 4 THEN 'R'  ELSE 'X' END AS ADJSTMT_DLTN_CD
		,RPAD(COALESCE(C.CLM_SBMT_FRMT_CD,' '),1,' ') AS  NSTD_FRMT_CD
		,RPAD(COALESCE(CLR.CLM_PRCNG_EXCPTN_CD,' '),1,' ') AS PRCNG_EXCPTN_CD
		,RPAD(COALESCE(CL.CLM_LINE_RX_NUM,' '),30,' ') AS RX_SRVC_RFRNC_NUM
		,RPAD(COALESCE(CLR.CLM_PHRMCY_SRVC_TYPE_CD,' '),2,' ') as PHRMCY_SRVC_TYPE_CD
		,RPAD(COALESCE(CLR.CLM_LINE_RX_ORGN_CD,' '),1,' ') AS RX_ORGN_CD
		,'  '  AS CCW_PHARM_ID
		
		--****\/****
		--OLD: X(20) X(20) 	NEW: X(35) X(35)
		--,RPAD(COALESCE(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,' '),20,' ')  AS CCW_PRSCRBR_ID
		  ,RPAD(COALESCE(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,' '),35,' ')  AS CCW_PRSCRBR_ID
		--****/\****

		,RPAD(COALESCE(C.PRVDR_SRVC_ID_QLFYR_CD,' '),2,' ')   AS PTD_PRSCRBR_ID_FRMT_CD
		,TO_CHAR(CLR.CLM_LINE_FRMLRY_TIER_LVL_ID,'FM00') AS FORMULARY_ID
		,RPAD(COALESCE(CLR.CLM_FRMLRY_CD,' '),1,' ') AS FRMLRY_RX_ID
		,RPAD(COALESCE(C.CLM_CNTRCT_OF_REC_CNTRCT_NUM,' '),5,' ')  AS PLAN_CNTRCT_REC_ID
		,RPAD(COALESCE(C.CLM_CNTRCT_OF_REC_PBP_NUM,' '),3,' ') AS  PLAN_PBP_REC_NUM

		--****\/****
		--OLD: NUMBER(8,2) S9(6)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,0),'MI000000.00') 		AS  GDC_BLW_OOPT_AMT
		  ,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,0),'MI000000000.00') 	AS  GDC_BLW_OOPT_AMT
		--****/\****

		--****\/****
		--OLD: NUMBER(8,2) S9(6)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,0),'MI000000.00') 	AS GDC_ABV_OOPT_AMT
		  ,TO_CHAR(COALESCE(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,0),'MI000000000.00') AS GDC_ABV_OOPT_AMT
		--****/\****

		,TO_CHAR(COALESCE(CL.CLM_LINE_OTHR_TP_PD_AMT,0),'MI0000000000000.00')  AS OTHR_TROOP_AMT

		--****\/****
		--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CLR.CLM_LINE_LIS_AMT,0),'MI0000000.00')  	AS LICS_AMT
		  ,TO_CHAR(COALESCE(CLR.CLM_LINE_LIS_AMT,0),'MI000000000.00')  	AS LICS_AMT
		--****/\****

		--****\/****
		--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CLR.CLM_LINE_PLRO_AMT,0),'MI0000000.00')  	AS PLRO_AMT
		  ,TO_CHAR(COALESCE(CLR.CLM_LINE_PLRO_AMT,0),'MI000000000.00')  AS PLRO_AMT
		--****/\****

		,TO_CHAR(COALESCE(CL.CLM_LINE_CVRD_PD_AMT,0),'MI000000000.00') AS CVRD_D_PLAN_PD_AMT

		--****\/****
		--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CL.CLM_LINE_NCVRD_PD_AMT,0), 'MI0000000.00') 	AS NON_CVRD_PLAN_PAID
		  ,TO_CHAR(COALESCE(CL.CLM_LINE_NCVRD_PD_AMT,0), 'MI000000000.00') 	AS NON_CVRD_PLAN_PAID
		--****/\****

		--****\/****
		--OLD: NUMBER(9,2) S9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
		--,TO_CHAR(COALESCE(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,0),'MI0000000.00') 		AS GAP_DSCNT_AMT
		  ,TO_CHAR(COALESCE(CLR.CLM_LINE_RPTD_GAP_DSCNT_AMT,0),'MI000000000.00') 	AS GAP_DSCNT_AMT
		--****/\****

		,RPAD(COALESCE(TO_CHAR(CDS.CLM_SCHLD_PMT_DT,'YYYYMMDD'),' '),8,' ') AS PD_DT
		,RPAD(COALESCE(CLR.CLM_CTSTRPHC_CVRG_IND_CD,' '),1,' ') AS CTSTRPHC_CVRG_CD
		,RPAD(COALESCE(CLR.CLM_BGN_BNFT_PHASE_CD,' '),1,' ') AS BGN_BNFT_PHASE
		,RPAD(COALESCE(CLR.CLM_END_BNFT_PHASE_CD,' '),1,' ') AS END_BNFT_PHASE
		,RPAD(COALESCE(CL.CLM_LINE_NDC_CD,' '),11,' ') AS CLM_LINE_NDC_CD
		,RPAD(COALESCE(NDCTV.PROD_NDC_BRAND_NAME,' '),30,' ') AS NDC_BRAND_NAME
		,RPAD(COALESCE(NDCTV.PROD_NDC_GNRC_NAME,' '),100,' ')  AS NDC_GNRC_NAME
		,RPAD(COALESCE(NDCDB.NDC_STGTH_UNIT_DESC,' '),11,' ') AS NDC_STGTH_UNIT_DESC
		,RPAD(COALESCE(NDCDB.NDC_DRUG_FORM_CD,' '),2,' ') AS NDC_DRUG_FORM_CD
		,RPAD(COALESCE(NDCDB.NDC_DSG_FORM_DESC,' '),4,' ') AS NDC_DSG_FORM_DESC
		,RPAD(COALESCE(Bene.BENE_EQTBL_BIC_HICN_NUM,' '),11,' ') AS BENE_EQTBL_BIC_HICN_NUM
		,RPAD(COALESCE(BENE.BENE_MBI_ID,' '),11,' ') AS BENE_MBI_ID
  FROM
   IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

  INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE BENE   
   ON C.BENE_SK = BENE.BENE_SK

  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE  CL
   ON C.GEO_BENE_SK = CL.GEO_BENE_SK
   AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
   AND C.CLM_TYPE_CD = CL.CLM_TYPE_CD
   AND C.CLM_NUM_SK = CL.CLM_NUM_SK
   AND C.CLM_FROM_DT = CL.CLM_FROM_DT

  INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX  CLR
   ON CL.GEO_BENE_SK = CLR.GEO_BENE_SK
   AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
   AND CL.CLM_TYPE_CD = CLR.CLM_TYPE_CD
   AND CL.CLM_NUM_SK = CLR.CLM_NUM_SK
   AND CL.CLM_LINE_NUM = CLR.CLM_LINE_NUM

  INNER JOIN  IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
   ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

  INNER JOIN IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR  PRVDR
     ON C.PRVDR_SRVC_PRVDR_NPI_NUM = PRVDR.PRVDR_NPI_NUM

  LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_PROD_NDC_EFCTV  NDCTV
     ON CL.CLM_LINE_NDC_CD = NDCTV.PROD_NDC_CD

  LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_NDC_{ENVNAME}.NDC_MDDB NDCDB
     ON CL.CLM_LINE_NDC_CD = NDCDB.NDC_CD

 WHERE
 C.CLM_TYPE_CD IN(1,2,4)
    AND  C.CLM_FINL_ACTN_IND='Y'
    AND C.CLM_FROM_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
                
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
FILE_LIST = open(f"{DATADIR}temp_RAND_PDE_Files_{TMSTMP}.txt", "a")
print('')
print("Run date and time: " + date_time  )
print

if CLM_TYPE == "PDE":
    ext_months = [
        {"ext_month":"JAN",
         "ext_month_no": "-01",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
        },
        {"ext_month":"FEB",
         "ext_month_no": "-02",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-28"]
        },
        {"ext_month":"MAR",
         "ext_month_no": "-03",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
        },
        {"ext_month":"APR",
         "ext_month_no": "-04",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-30"]
        },
        {"ext_month":"MAY",
         "ext_month_no": "-05",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
        },
        {"ext_month":"JUN",
         "ext_month_no": "-06",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-30"]
        },
        {"ext_month":"JUL",
         "ext_month_no": "-07",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
        },
        {"ext_month":"AUG",
         "ext_month_no": "-08",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
        },
        {"ext_month":"SEP",
         "ext_month_no": "-09",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-30"]
        },
        {"ext_month":"OCT",
         "ext_month_no": "-10",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
        },
        {"ext_month":"NOV",
         "ext_month_no": "-11",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-30"]
        },
        {"ext_month":"DEC",
         "ext_month_no": "-12",
         "start_dates": ["-01","-16"],
         "end_date": ["-15","-31"]
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
            
            XTR_FILE_NAME = f"RAND_{CLM_TYPE}_Y{EXT_YR}_{ext_months[i]['ext_month']}P{RNG}_{TMSTMP}.csv.gz"
        
            bErrorOccurred = execute_PDE_extract()

            FILE_LIST.write(f"{XTR_FILE_NAME}\n")
            		

else:
    # Invalid CTYP code supplied to the script
    sys.exit(12) 
    FILE_LIST.close() 

FILE_LIST.close()


snowconvert_helpers.quit_application()
