#!/usr/bin/env python
########################################################################################################
# Name:  HCPP_Extract.py
#
# Desc: Script to Extract HCPP data for a Plan/Year
#
# Author: Paul Baranoski
#
# Modified:
#
# Paul Baranoski 2023-02-06 Created script.
# Paul Baranoski 2023-04-25 Added Coalesce to several fields in SQL.
# Paul Baranoski 2023-04-27 Modify Extract filename to make conversion to EFT filename easier.
# Sean Whitelock 2025-01-16 Updated the length of PRVDR_LGL_NAME from 70 to 100 and added the COALESCE function to CLM_BLG_PRVDR_NPI_NUM and CLM_RNDRG_PRVDR_NPI_NUM
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
CONTRACT_NUM=os.getenv('CONTRACT_NUM')
EXT_YR=os.getenv('EXT_YR')
EXT_YY=str(EXT_YR)[2:4]
CONTRACTOR=os.getenv('CONTRACTOR')

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
   
 
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_HCPP_STG/HCPP_{CONTRACTOR}_{CONTRACT_NUM}_PY{EXT_YY}_{TMSTMP}.txt.gz
                                                FROM (

                        SELECT DISTINCT
                            '%'  AS D1
                            ,RPAD(C.CLM_HIC_NUM,20,' ')                AS CLM_HIC_NUM
                            ,'%' AS D2
                            ,RPAD(COALESCE(BH.BENE_LAST_NAME,' '),40,' ')   AS BENE_LAST_NAME
                            ,'%'  AS D3
                            ,TO_CHAR(CL.CLM_LINE_FROM_DT,'YYYYMMDD')   AS CLM_LINE_FROM_DT
                            ,'%' AS D4
                            ,TO_CHAR(CL.CLM_LINE_THRU_DT,'YYYYMMDD')   AS CLM_LINE_THRU_DT
                            ,'%' AS D5
                            ,RPAD(C.CLM_DISP_CD,2,' ')                 AS CLM_DISP_CD
                            ,'%' AS D6
                            
                            
                            ,TO_CHAR(
                                CASE  WHEN C.CLM_DISP_CD = '03' 
                                      THEN (CL.CLM_LINE_ALOWD_CHRG_AMT) * -1
                                      ELSE  CL.CLM_LINE_ALOWD_CHRG_AMT
                                END,'MI0000000000000.00')               AS CLM_LINE_ALOWD_CHRG_AMT
                            ,'%' AS D7
                            
                            ,TO_CHAR(
                                CASE  WHEN C.CLM_DISP_CD = '03' 
                                      THEN (CL.CLM_LINE_PRVDR_PMT_AMT) * -1
                                      ELSE  CL.CLM_LINE_PRVDR_PMT_AMT
                                END,'MI000000000.00')               AS CLM_LINE_PRVDR_PMT_AMT
                            ,'%' AS D8

                            ,TO_CHAR(
                                CASE  WHEN C.CLM_DISP_CD = '03' 
                                      THEN (CL.CLM_LINE_BENE_PD_AMT) * -1
                                      ELSE  CL.CLM_LINE_BENE_PD_AMT
                                END,'MI0000000000000.00')               AS CLM_LINE_BENE_PD_AMT
                            ,'%' AS D9 
                            
                            ,TO_CHAR(
                                CASE  WHEN C.CLM_DISP_CD = '03' 
                                      THEN (CL.CLM_LINE_MDCR_COINSRNC_AMT) * -1
                                      ELSE  CL.CLM_LINE_MDCR_COINSRNC_AMT
                                END,'MI000000000.00')               AS CLM_LINE_MDCR_COINSRNC_AMT
                            ,'%' AS D10

                            ,TO_CHAR(
                                CASE  WHEN C.CLM_DISP_CD = '03' 
                                      THEN (CL.CLM_LINE_MDCR_DDCTBL_AMT) * -1
                                      ELSE  CL.CLM_LINE_MDCR_DDCTBL_AMT
                                END,'MI000000000.00')               AS CLM_LINE_MDCR_DDCTBL_AMT
                            ,'%' AS D11
                            
                            ,RPAD(
                                CASE WHEN C.CLM_TYPE_CD IN (71,72) THEN CL.CLM_RNDRG_PRVDR_PIN_NUM
                                     WHEN C.CLM_TYPE_CD IN (81,82) THEN CL.CLM_RNDRG_PRVDR_NSC_NUM
                                END,14,' ')  AS Phy_Supp_id 
                            ,'%' AS D12
                            
                            
                            ,C.CLM_FINL_ACTN_IND           AS CLM_FINL_ACTN_IND 
                            ,'%' AS D13
                            ,RPAD(C.CLM_CNTRCTR_NUM,5,' ') AS CNTRCTR_NUM
                            ,'%' AS D14
                            ,TO_CHAR(CDS.CLM_SCHLD_PMT_DT,'YYYYMMDD') AS SCHLD_PMT_DT
                            ,'%' AS D15
                            ,RPAD(C.CLM_CNTL_NUM,40,' ')      AS CLM_CNTL_NUM
                            ,'%' AS D16
                            ,RPAD(C.CLM_TYPE_CD,5,' ')        AS S_CLM_TYPE_CD
                            ,'%'  AS D17

                            ,RPAD(C.CLM_QUERY_CD,1,' ')       AS CLM_QUERY_CD
                            ,'%'  AS D18
                            ,RPAD(BMER.BENE_CNTRCT_NUM,5,' ') AS BENE_CNTRCT_NUM
                            ,'%' AS D19
                            
                            
                            ,TO_CHAR(BMER.BENE_SK,'FM000000000000000000')  AS BMER_BENE_SK
                            ,'%'  AS D20
                            ,RPAD(COALESCE(BH.BENE_1ST_NAME,' '),30,' ')                AS BENE_1ST_NAME
                            ,'%'  AS D21
                           
                           
                            ,RPAD(COALESCE(TO_CHAR(BH.BENE_BRTH_DT,'YYYYMMDD'),' '),8,' ')   AS BENE_BRTH_DT
                            ,'%'  AS D22
                            
                            
                            ,RPAD(COALESCE(BH.BENE_SEX_CD,' '),1,' ')                    AS B_SEX_CD
                            ,'%'  AS D23
                            ,TO_CHAR(BMER.BENE_ENRLMT_CNTRCT_EFCTV_DT,'YYYYMMDD') AS  BENE_ENRLMT_CNTRCT_EFCTV_DT
                            ,'%'  AS D233
                            ,TO_CHAR(BMER.BENE_ENRLMT_END_DT,'YYYYMMDD')           AS BENE_ENRLMT_END_DT
                            ,'%'  AS D24

                            --***\/***
			    --,RPAD(COALESCE(PR.PRVDR_LGL_NAME,' '),70,' ')          AS BLG_LGL_NAME
			    ,RPAD(COALESCE(PR.PRVDR_LGL_NAME,' '),100,' ')          AS BLG_LGL_NAME
			    --***/\***
   
                            ,'%'  AS D25

			    --***\/***
                            --,RPAD(COALESCE(PRB.PRVDR_LGL_NAME,' '),70,' ')         AS RNDRG_LGL_NAME
			    ,RPAD(COALESCE(PRB.PRVDR_LGL_NAME,' '),100,' ')         AS RNDRG_LGL_NAME
			    --***/\***
 
                            ,'%'  AS D26
                            ,RPAD(COALESCE(C.CLM_BLG_PRVDR_NPI_NUM,' '),10,' ')    AS CLM_BLG_PRVDR_NPI_NUM  
                            ,'%'  AS D27
                            ,RPAD(COALESCE(CL.CLM_RNDRG_PRVDR_NPI_NUM,' '),10,' ') AS CLM_RNDRG_PRVDR_NPI_NUM 
                            ,'%'  AS D28
                            ,RPAD(COALESCE(CLP.CLM_POS_PHYSN_ORG_NAME,' '),60,' ') AS CLM_POS_PHYSN_ORG_NAME
                            ,'%'  AS D29
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_1ST_NAME,' '),35,' ') AS CLM_POS_PRVDR_1ST_NAME
                            ,'%'  AS D30
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_MDL_NAME,' '),25,' ') AS CLM_POS_PRVDR_MDL_NAME
                            ,'%'  AS D31
                            
                            
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_1ST_LINE_ADR,' '),55,' ')  AS CLM_POS_PRVDR_1ST_LINE_ADR
                            ,'%'  AS D32
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_2ND_LINE_ADR,' '),55,' ')  AS CLM_POS_PRVDR_2ND_LINE_ADR 
                            ,'%'  AS D33
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_CITY_NAME,' '),30,' ')     AS CLM_POS_PRVDR_CITY_NAME 
                            ,'%'  AS D34
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_USPS_STATE_CD,' '),2,' ')  AS CLM_POS_PRVDR_USPS_STATE_CD
                            ,'%'  AS D35
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_ZIP5_CD,' '),5,' ')        AS CLM_POS_PRVDR_ZIP5_CD
                            ,'%'  AS D36
                            ,RPAD(COALESCE(CLP.CLM_POS_PRVDR_ZIP4_CD,' '),4,' ')        AS CLM_POS_PRVDR_ZIP4_CD
                            ,'%'  AS D37
                            ,RPAD(CLP.CLM_PRVDR_SPCLTY_CD,2,' ')          AS CLM_PRVDR_SPCLTY_CD
                            ,'%'  AS D38

                            ,RPAD(CLP.CLM_TYPE_SRVC_CD,2,' ') AS CLM_TYPE_SRVC_CD
                            ,'%'  AS D39
                            ,TO_CHAR(CLP.CLM_LINE_NUM,'FM0000000000')     AS CLM_LINE_NUM
                            ,'%'  AS D40
                            
                            ,RPAD(CL.CLM_LINE_DGNS_CD,7,' ')  AS CLM_LINE_DGNS_CD
                            ,'%'  AS D41
                            ,RPAD(CL.CLM_LINE_HCPCS_CD,5,' ') AS CLM_LINE_HCPCS_CD
                            ,'%'  AS D42
                            ,RPAD(CL.HCPCS_1_MDFR_CD,2,' ')   AS HCPCS_1_MDFR_CD
                            ,'%'  AS D43
                            ,RPAD(CL.HCPCS_2_MDFR_CD,2,' ')   AS HCPCS_2_MDFR_CD 
                            ,'%'  AS D44
                            ,RPAD(CL.HCPCS_3_MDFR_CD,2,' ')   AS HCPCS_3_MDFR_CD
                            ,'%'  AS D45
                            ,RPAD(CL.HCPCS_4_MDFR_CD,2,' ')   AS HCPCS_4_MDFR_CD
                            ,'%'  AS D46
                            
                            ,TO_CHAR(CL.CLM_LINE_SRVC_UNIT_QTY,'MI00000000000000.0000') AS CLM_LINE_SRVC_UNIT_QTY
                            ,'%'  AS D47
                            ,TO_CHAR(CL.CLM_LINE_SBMT_CHRG_AMT,'MI0000000000000.00')    AS CLM_LINE_SBMT_CHRG_AMT
                            ,'%'  AS D48
                            ,TO_CHAR(CDS.CLM_SUBMSN_DT,'YYYYMMDD')        AS CLM_SUBMSN_DT
                            ,'%'  AS D49
                            ,TO_CHAR(CDS.CLM_PD_DT,'YYYYMMDD')            AS CLM_PD_DT 
                            ,'%'  AS D50
                            ,RPAD(CLP.CLM_PRCSG_IND_CD,2,' ')             AS CLM_PRCSG_IND_CD
                            ,'%'   AS D51
                            ,RPAD(COALESCE(BH.BENE_MBI_ID,' '),11,' ')    AS BENE_MBI_ID
                            ,'%'   AS D52


                        FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

                        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
                        ON CDS.CLM_DT_SGNTR_SK = C.CLM_DT_SGNTR_SK

                        LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE_HSTRY BH
                        ON  C.BENE_SK             = BH.BENE_SK
                        AND C.CLM_FROM_DT   BETWEEN to_date(BH.IDR_TRANS_EFCTV_TS)
                                            AND     to_date(BH.IDR_TRANS_OBSLT_TS)

                        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                              ON  C.GEO_BENE_SK     = CL.GEO_BENE_SK
                              AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                              AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                              AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                              AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

                        INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_PRFNL CLP
                              ON  CLP.GEO_BENE_SK     = CL.GEO_BENE_SK
                              AND CLP.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                              AND CLP.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                              AND CLP.CLM_NUM_SK      = CL.CLM_NUM_SK
                              AND CLP.CLM_LINE_NUM    = CL.CLM_LINE_NUM

                        INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE_MAPD_ENRLMT BMER
                        ON C.BENE_SK = BMER.BENE_SK
                        AND BMER.IDR_LTST_TRANS_FLG   = 'Y'
                        AND BMER.IDR_TRANS_OBSLT_TS   = to_date('9999-12-31','YYYY-MM-DD')
                        AND C.CLM_FROM_DT  BETWEEN bene_enrlmt_bgn_dt AND bene_enrlmt_end_dt

                        LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR PR
                        ON PR.PRVDR_NPI_NUM = C.PRVDR_BLG_PRVDR_NPI_NUM

                        LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR PRB
                        ON PRB.PRVDR_NPI_NUM = CL.PRVDR_RNDRNG_PRVDR_NPI_NUM

                        WHERE CDS.CLM_NCH_WKLY_PROC_DT between to_date('{EXT_YR}-01-01','YYYY-MM-DD') and to_date('{EXT_YR}-12-31','YYYY-MM-DD')
                          AND BMER.BENE_CNTRCT_NUM  = '{CONTRACT_NUM}' 
                          AND C.CLM_TYPE_CD IN (71,72,81,82)

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

