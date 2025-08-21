#!/usr/bin/env python
########################################################################################################
# Name:  DEMOFNDR_PTD.py
#
# Desc: Script to Extract Demo Finder Part D data
#
# Created: Sumathi Gayam 
#
# Modified:
#
# Sumathi Gayam  2022-09-01 Created script.
# Paul Baranoski 2023-03-31 Modified SQL for performance. 
#                           1) Changed join to Finder File table to use Claim table HICN_NUM instead 
#                              of BENE table HICN_NUM 
#                           2) Change filter CLM_THRU_DT to use column on Claim table instead of CDS. 
#                           3) Removed join to CDS (Claim Date Signature).   
# Paul Baranoski 2023-05-16 Parameterized CDS.CLM_THRU_DT values instead of using SQL calculation. 
# N. Tinovsky	 2024-11-25 Increased length for CLM_PRSBNG_PRVDR_GNRC_ID_NUM
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
PLAN_NUM=os.getenv('PLAN_NUM')
CUR_YR=os.getenv('CUR_YR')
PRV_MN=os.getenv('PRIOR_MN')

EXT_FROM_DT=os.getenv('EXT_FROM_DT')
EXT_TO_DT=os.getenv('EXT_TO_DT')

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
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)
   
   #**************************************
   #   Extract Part D claim data  
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DEMOFNDR_STG/DEMOFNDR_PTD_{PLAN_NUM}_{CUR_YR}{PRV_MN}_{TMSTMP}.txt.gz
                                                FROM (

SELECT
	RPAD(BENE.BENE_EQTBL_BIC_HICN_NUM,11,' '),
	
	CASE WHEN CL.CLM_LINE_FROM_DT is NULL
		 THEN REPEAT(' ',8) 
		 ELSE to_char(CL.CLM_LINE_FROM_DT, 'YYYYMMDD')
	END AS  CLM_LINE_SRVC_DT,
		
	CASE WHEN C.CLM_PD_DT is NULL
		 THEN REPEAT(' ',8) 
		 ELSE to_char(C.CLM_PD_DT, 'YYYYMMDD')
	END AS  CLM_PD_DT,   
	   
	RPAD(CL.CLM_LINE_NDC_CD,11,' '),
	RPAD(C.PRVDR_SRVC_ID_QLFYR_CD,2,' ')        AS  SRVC_GNRC_ID_QLFYR,
	RPAD(C.CLM_SRVC_PRVDR_GNRC_ID_NUM,20,' ')   AS  SRVC_PRVDR_ID,
	
	to_char(COALESCE(CLR.CLM_LINE_RX_FILL_NUM,0),'FM000000000') AS CLM_LINE_RX_FILL_NUM,
	 
	RPAD(COALESCE(CLR.CLM_DSPNSNG_STUS_CD,' '),1,' ')       AS  CLM_DSPNSNG_STUS_CD,
	RPAD(COALESCE(CLR.CLM_CMPND_CD,' '),1,' ')              AS  CLM_CMPND_CD,
	RPAD(COALESCE(CLR.CLM_DAW_PROD_SLCTN_CD,' '),1,' ')     AS  CLM_DAW_PROD_SLCTN_CD,
	
	to_char(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'FM00000000000000.000')
	                                            AS  PTAP_QUANITIY_DISPENSED,
	to_char(COALESCE(CLR.CLM_LINE_DAYS_SUPLY_QTY,0),'FM000000000')   
	                                            AS  CLM_LINE_DAYS_SUPLY_QTY,
	  
	RPAD(C.PRVDR_PRSBNG_ID_QLFYR_CD,2,' ')      AS  PRSCRB_ID_QLFYR,
	
	--****\/****
	--OLD: X(20) X(20) 	NEW: X(35) X(35)
	--RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,20,' ') AS  PRSCRB_ID,
      RPAD(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM,35,' ') AS  PRSCRB_ID,
	--****/\****
				  
	RPAD(CLR.CLM_DRUG_CVRG_STUS_CD,2,' ')       AS  CLM_DRUG_CVRG_STUS_CD,
	  
	CASE WHEN C.CLM_TYPE_CD = '1' THEN ' '
		 WHEN C.CLM_TYPE_CD = '2' THEN 'A'
		 WHEN C.CLM_TYPE_CD = '3' THEN 'D'
		 WHEN C.CLM_TYPE_CD = '4' THEN 'R'
		 ELSE 'X' 
		 END  AS ADJ_DEL_CD,
		 
    RPAD(COALESCE(C.CLM_SBMT_FRMT_CD,' '),1,' ')            AS  CLM_SBMT_FRMT_CD,
	RPAD(COALESCE(CLR.CLM_PRCNG_EXCPTN_CD,' '),1,' ')       AS  CLM_PRCNG_EXCPTN_CD,
	
	to_char(COALESCE(C.CLM_UNIQ_ID,0),'FM0000000000000'),
	
	COALESCE(CLD.CLM_REFL_IND,' '),
	
	RPAD(BENE.BENE_LAST_NAME,40,' ')            AS BENE_LAST_NAME,
	RPAD(BENE.BENE_1ST_NAME,30,' ')             AS BENE_1ST_NAME,
	RPAD(COALESCE(BENE.BENE_MIDL_NAME,' '),15,' ')            AS MDL_NAME,
	  
	CASE WHEN BENE.BENE_BRTH_DT IS NULL
	     THEN REPEAT(' ',8)
		 ELSE to_char(BENE.BENE_BRTH_DT,'YYYYMMDD')
	END AS BENE_BRTH_DT,
	  
	COALESCE(C.BENE_SEX_CD,' '),
	  
	CASE WHEN CLR.CLM_LINE_RX_FILL_DT IS NULL
	     THEN REPEAT(' ',8)
         ELSE to_char(CLR.CLM_LINE_RX_FILL_DT,'YYYYMMDD')	
    END AS CLM_LINE_RX_FILL_DT,
	  
	RPAD(BENE.BENE_MBI_ID,11,' ')              AS BENE_MBI_ID

FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE BENE
ON C.BENE_SK =  BENE.BENE_SK

--FINDER FILE
INNER JOIN BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DEMOFNDR_HICN_PLAN FNDR
ON BENE.BENE_HIC_NUM =  TRIM(FNDR.HICN)
--ON C.CLM_HIC_NUM =  TRIM(FNDR.HICN)

INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
ON  C.GEO_BENE_SK     = CL.GEO_BENE_SK
AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX CLR
ON CL.CLM_NUM_SK = CLR.CLM_NUM_SK
AND CL.CLM_TYPE_CD = CLR.CLM_TYPE_CD
AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
AND CL.GEO_BENE_SK = CLR.GEO_BENE_SK
AND CL.CLM_LINE_NUM = CLR.CLM_LINE_NUM

-- remove join for performance reasons
--INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
--ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN CLD
ON CLD.GEO_BENE_SK = C.GEO_BENE_SK
AND CLD.CLM_DT_SGNTR_SK = C.CLM_DT_SGNTR_SK
AND CLD.CLM_TYPE_CD = C.CLM_TYPE_CD
AND CLD.CLM_NUM_SK = C.CLM_NUM_SK

WHERE C.CLM_FINL_ACTN_IND = 'Y'
 --AND CDS.CLM_THRU_DT  BETWEEN  to_date('2022-07-01','YYYY-MM-DD') AND to_date('2022-07-01','YYYY-MM-DD')
 --AND CDS.CLM_THRU_DT  BETWEEN trunc(to_date(Dateadd(Month, -12, GETDATE())), 'MONTH') AND last_day(DATEADD(month,-1,GETDATE()))
 --AND C.CLM_THRU_DT  BETWEEN trunc(to_date(Dateadd(Month, -12, GETDATE())), 'MONTH') AND last_day(DATEADD(month,-1,GETDATE()))
   AND C.CLM_THRU_DT  BETWEEN to_date('{EXT_FROM_DT}','YYYY-MM-DD' ) AND to_date('{EXT_TO_DT}','YYYY-MM-DD' )


 AND C.CLM_TYPE_CD IN (1,2,3,4)
 ORDER BY 1
 
  )
                        FILE_FORMAT = (TYPE = CSV field_delimiter = none  ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY = none )
                        SINGLE = TRUE  max_file_size=5368709120  """, con, exit_on_error=True)


   
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

