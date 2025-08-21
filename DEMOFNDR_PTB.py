#!/usr/bin/env python
########################################################################################################
# Name:  DEMOFNDR_PTB.py
#
# Desc: Script to Extract Demo Finder Part B data
#
# Created: Sumathi Gayam 
#
# Sumathi Gayam  2022-09-01 Created script.
# Paul Baranoski 2023-03-31 Modified SQL for performance. 
#                           1) Changed join to Finder File table to use Claim table HICN_NUM instead 
#                              of BENE table HICN_NUM 
# Paul Baranoski 2023-04-03 Modified SQL for performance.
#                           1) Changed join to Finder File table back to using BENE.HICN_NUM instead of CLM.
#                              NOF matched rows was highter with BENE.HICN_NUM. Sometimes CLM_HICN_NUM <> BENE.HICN_NUM.
#                           2) Modified query to use WITH statement to initially filter results before joining to other tables
#                              for performance reasons. Appeared to shave a minute off run-time using multiple bench-marks.
# Paul Baranoski 2023-04-21 Modified SQL for performance. Reorganized to isolate join between finder file 
#                           table and BENE table into FINDER_BENES view, and use that view in main query. SQL ran in 51.16 seconds.  
# Paul Baranoski 2023-05-16 Parameterized CDS.CLM_THRU_DT values instead of using SQL calculation.
# Nat. Tinovsky  2025_01-07 Increase length for PRVDR_LGL_NAME from 70 to 100 
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
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DEMOFNDR_STG/DEMOFNDR_PTB_{PLAN_NUM}_{CUR_YR}{PRV_MN}_{TMSTMP}.txt.gz
                                                FROM (

                        WITH FINDER_BENES as (

                            SELECT   BENE_SK 	 
                                    ,BENE.BENE_EQTBL_BIC_HICN_NUM
                                    ,BENE.BENE_HIC_NUM
                                    ,BENE.BENE_MBI_ID
                                    ,BENE.BENE_BRTH_DT
                                    ,BENE.BENE_LAST_NAME
                                    ,BENE.BENE_1ST_NAME
                                    ,BENE.BENE_MIDL_NAME
                            FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DEMOFNDR_HICN_PLAN FNDR

                            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE BENE
                            ON BENE.BENE_HIC_NUM =  TRIM(FNDR.HICN)

                        )

                             SELECT
                                    RPAD(BENE.BENE_EQTBL_BIC_HICN_NUM,11,' '),
                                    '     ',
                                    to_char(CDS.CLM_ACTV_CARE_FROM_DT,'YYYYMMDD'),
                                    to_char(CDS.CLM_DSCHRG_DT,'YYYYMMDD'),
                                    to_char(CDS.CLM_FROM_DT,'YYYYMMDD'),
                                    to_char(CDS.CLM_THRU_DT,'YYYYMMDD'),
                                    RPAD(CPM.CLM_DGNS_1_CD,7,' '),
                                    RPAD(DC1.DGNS_CD_DESC,50,' '),
                                    RPAD(CPM.CLM_DGNS_2_CD,7,' '),
                                    RPAD(DC2.DGNS_CD_DESC,50,' '),
                                        

                                    to_char(C.CLM_TYPE_CD,'FM00'),
                                    RPAD(C.CLM_CNTL_NUM,40,' '),
                                    RPAD(C.CLM_ADJSTMT_RSN_CD,3,' '),
                                    RPAD(C.CLM_ORIG_CNTL_NUM,40,' '),
                                    RPAD(CL.CLM_LINE_HCPCS_CD,5,' '),
                                    RPAD(CL.HCPCS_1_MDFR_CD,2,' '),
                                    RPAD(CL.HCPCS_2_MDFR_CD,2,' '),
                                    RPAD(CL.HCPCS_3_MDFR_CD,2,' '),
                                    RPAD(CL.HCPCS_4_MDFR_CD,2,' '),
                                    RPAD(CL.CLM_POS_CD,2,' '),
                                    RPAD(CL.CLM_LINE_NDC_CD,11,' '),
                                    RPAD(C.CLM_BLG_PRVDR_NPI_NUM,10,' '),
                                    RPAD(COALESCE(C.CLM_BLG_PRVDR_GNRC_ID_NUM,' '),20,' '),
                                                                        
                                    --****\/****
                                    --OLD: X(70)  	NEW: X(100)   
                                    --RPAD(COALESCE(PRVDR.PRVDR_LGL_NAME,' '),70,' '),
                                      RPAD(COALESCE(PRVDR.PRVDR_LGL_NAME,' '),100,' '),
                                    --****/\****   
                                    
                                    RPAD(COALESCE(PRVDR.PRVDR_MLG_TEL_NUM,' '),70,' '),
                                    RPAD(COALESCE(C.CLM_BLG_PRVDR_TYPE_CD,' '),3,' '),
                                    RPAD(CL.CLM_RNDRG_PRVDR_TYPE_CD,3,' '),
                                    RPAD(CLP.CLM_PRVDR_SPCLTY_CD,2,' '),
                                    RPAD(CL.CLM_RNDRG_PRVDR_NPI_NUM,10,' '),
                                    RPAD(' ',10,' '),
                                    RPAD(C.PRVDR_PRSBNG_ID_QLFYR_CD,2,' '),

                                    RPAD(SUBSTR(C.CLM_CNTL_NUM,1,17) || to_char(CL.CLM_LINE_NUM,'FM000'),40,' '),
                                    '  ',
                                    RPAD(CL.CLM_LINE_REV_CTR_CD,4,' '),

                                    RPAD(BENE.BENE_LAST_NAME,40,' '),
                                    RPAD(BENE.BENE_1ST_NAME,30,' '),
                                    RPAD(COALESCE(BENE.BENE_MIDL_NAME,' '),15,' '),
                                    RPAD(CPM.CLM_DGNS_PRCDR_ICD_IND,1,' '),

                                    CASE WHEN BENE.BENE_BRTH_DT IS NULL
                                         THEN '        '
                                         ELSE to_char(BENE.BENE_BRTH_DT,'YYYYMMDD')
                                    END AS BENE_BRTH_DT, 

                                    RPAD(C.BENE_SEX_CD,1,' '),
                                            
                                    RPAD(BENE.BENE_MBI_ID,11,' ') --AS BENE_MBI_ID
                          
                                                            
                                FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

                                INNER JOIN FINDER_BENES BENE
                                ON C.BENE_SK =  BENE.BENE_SK

                                INNER JOIN  IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
                                ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

                                INNER JOIN  IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                                ON  C.GEO_BENE_SK     = CL.GEO_BENE_SK
                                AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                                AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                                AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                                AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

                                INNER JOIN    IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_PRFNL CLP
                                ON  CL.GEO_BENE_SK     = CLP.GEO_BENE_SK
                                AND CL.CLM_DT_SGNTR_SK = CLP.CLM_DT_SGNTR_SK
                                AND CL.CLM_TYPE_CD     = CLP.CLM_TYPE_CD
                                AND CL.CLM_NUM_SK      = CLP.CLM_NUM_SK
                                AND CL.CLM_LINE_NUM    = CLP.CLM_LINE_NUM
                          
                                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PROD_MTRLZD  CPM
                                ON  C.GEO_BENE_SK     = CPM.GEO_BENE_SK
                                AND C.CLM_DT_SGNTR_SK = CPM.CLM_DT_SGNTR_SK
                                AND C.CLM_TYPE_CD     = CPM.CLM_TYPE_CD
                                AND C.CLM_NUM_SK      = CPM.CLM_NUM_SK
                                AND C.CLM_THRU_DT     = CPM.CLM_THRU_DT
                         
                                INNER JOIN IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_DGNS_CD DC1
                                ON  CPM.CLM_DGNS_1_CD = DC1.DGNS_CD
                                AND CPM.CLM_THRU_DT BETWEEN DC1.DGNS_CD_BGN_DT AND DC1.DGNS_CD_END_DT
                         
                                INNER JOIN IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_DGNS_CD DC2
                                ON CPM.CLM_DGNS_2_CD = DC2.DGNS_CD
                                AND CPM.CLM_THRU_DT BETWEEN DC2.DGNS_CD_BGN_DT AND DC2.DGNS_CD_END_DT

                                LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR   PRVDR
                                ON C.CLM_BLG_PRVDR_NPI_NUM = PRVDR.PRVDR_NPI_NUM

                                WHERE C.CLM_FINL_ACTN_IND='Y'
                                --AND CDS.CLM_THRU_DT  BETWEEN  to_date('2022-07-01','YYYY-MM-DD') AND to_date('2022-07-01','YYYY-MM-DD')
                                --AND CDS.CLM_THRU_DT  BETWEEN trunc(to_date(Dateadd(Month, -12, GETDATE())), 'MONTH') AND last_day(DATEADD(month,-1,GETDATE()))
                                AND CDS.CLM_THRU_DT  BETWEEN to_date('{EXT_FROM_DT}','YYYY-MM-DD' ) AND to_date('{EXT_TO_DT}','YYYY-MM-DD' )

                                AND C.CLM_TYPE_CD IN (71,72,81,82)

                                AND (SUBSTR(CPM.CLM_DGNS_1_CD,1,3)
                                        NOT IN ('291','292','303','304','305')
                                AND CPM.CLM_DGNS_1_CD   <> '7903   '
                                AND CPM.CLM_DGNS_1_CD   <> 'V6542  '
                                AND SUBSTR(CPM.CLM_DGNS_2_CD,1,3) NOT IN ('291','292','303','304','305')
                                AND CPM.CLM_DGNS_2_CD   <> '7903   '
                                AND CPM.CLM_DGNS_2_CD   <> 'V6542  '
                                AND SUBSTR(CL.CLM_LINE_HCPCS_CD,1,4) NOT IN ('9445','9446',
                                             '9453','9454',
                                             '946 ','9461',
                                             '9462','9463',
                                             '9464','9465',
                                             '9466','9467',
                                             '9468','9469')
                                AND CL.CLM_LINE_HCPCS_CD NOT IN ('99408','99409','4320F','H0005',
                                   'H0006','H0007','H0008','H0009',
                                   'H0010','H0011','H0012','H0013',
                                   'H0014','H0015','H0020','H0050',
                                   'H0034','H0047','H2035','H2036',
                                   'S9475','T1006','T1007','T1008',
                                   'T1009','T1010','T1011','T1012'))

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

