#!/usr/bin/env python
########################################################################################################
# Name:  RAND_PartA_Extract_v2.py
# DESC:  This python script extracts data from IDRC for the RAND Part A (OPT, INP, SNF, HHA, HSP) extracts
#
# Created: Paul Baranoski
# Modified: 04/27/2026
#
# Modified:
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

ENVNAME = os.getenv('ENVNAME')
CLM_TYPE_CD = os.getenv('CLM_TYPE_CD')
START_DATE = os.environ["START_DATE"]
END_DATE = os.environ["END_DATE"] 

XTR_FILE_NAME = os.environ["XTR_FILE_NAME"]  

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


    ########################################################################################################
    # Extract RAND Part A data and write to S3 as a flat file
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_RANDFFSPTAB_STG/{XTR_FILE_NAME}
            FROM (
            
            SELECT DISTINCT
                'START' AS ST_OF_FILE,
                LPAD(C.CLM_TYPE_CD,2,'0') AS CLM_TYPE_CD,
                LPAD(CL.CLM_LINE_NUM,5,'0') AS CLM_LINE_NUM,
                RPAD(COALESCE(SUBSTR(LTRIM(RTRIM(C.CLM_HIC_NUM)),1,12),''),12,' ') AS BENE_HICN,
                RPAD(COALESCE(C.BENE_SEX_CD,''),1,' ') AS BENE_GENDER,
                RPAD(COALESCE(TO_CHAR(BENE.BENE_BRTH_DT, 'YYYYMMDD'),''),8,' ') AS BENE_DOB,
                RPAD(COALESCE(BENE.BENE_LAST_NAME,''),40,' ') AS BENE_LAST_NAME,
                RPAD(COALESCE(BENE.BENE_1ST_NAME,''),30,' ') AS BENE_1ST_NAME,
                RPAD(COALESCE(BENE.BENE_MIDL_NAME,''),15,' ') AS BENE_MIDL_NAME,
                RPAD(COALESCE(G.GEO_SSA_STATE_CD,''),2,' ') AS SSA_STATE_CD,
                RPAD(COALESCE(ZIP5.GEO_ZIP5_CD,''),5,' ') AS BENE_MAILING_ZIPCD,
                RPAD(COALESCE(TO_CHAR(CDS.CLM_ACTV_CARE_FROM_DT, 'YYYYMMDD'),''),8,' ') AS CLM_ACTV_CARE_FROM_DT,
                RPAD(COALESCE(TO_CHAR(CDS.CLM_DSCHRG_DT, 'YYYYMMDD'),''),8,' ') AS CLM_DSCHRG_DT,
                RPAD(COALESCE(TO_CHAR(CDS.CLM_FROM_DT, 'YYYYMMDD'),''),8,' ') AS CLM_FROM_DT,
                RPAD(COALESCE(TO_CHAR(CDS.CLM_THRU_DT, 'YYYYMMDD'),''),8,' ') AS CLM_THRU_DT,
                RPAD(C.CLM_CNTL_NUM,40,' ') AS CLAIM_CONTROL_NBR,
                RPAD(COALESCE(C.CLM_ORIG_CNTL_NUM,''),40,' ') AS ORIG_CLAIM_CONTROL_NBR,
                RPAD(COALESCE(CI.CLM_TRANS_CD,''),1,' ') AS CLM_TRANS_CD,
                RPAD(COALESCE(C.CLM_DISP_CD,''),2,' ') AS CLM_DISP_CD,
                RPAD(COALESCE(CI.BENE_PTNT_STUS_CD,''),2,' ') AS BENE_PTNT_STUS_CD,
                RPAD(REPLACE(COALESCE(CI.CLM_ADMSN_TYPE_CD,''),'~',''),2,' ') AS CLM_ADMSN_TYPE_CD,
                RPAD(REPLACE(COALESCE(CI.CLM_ADMSN_SRC_CD,''),'~',''),2,' ') AS CLM_ADMSN_SRC_CD,
                RPAD(COALESCE(CL.CLM_LINE_REV_CTR_CD,''),4,' ') AS CLM_LINE_REV_CTR_CD,
                RPAD(COALESCE(C.CLM_BILL_FAC_TYPE_CD,''),1,' ') AS CLM_BILL_FAC_TYPE_CD,
                RPAD(COALESCE(C.CLM_BILL_CLSFCTN_CD,''),1,' ') AS CLM_BILL_CLSFCTN_CD,
                RPAD(COALESCE(C.CLM_BILL_FREQ_CD,''),1,' ') AS CLM_BILL_FREQ_CD,
                LPAD(COALESCE(TO_CHAR(CI.DGNS_DRG_VRSN_NUM),''),2,'0') AS DGNS_DRG_VRSN_NUM,
                LPAD(COALESCE(TO_CHAR(CI.DGNS_DRG_CD),''),4,'0') AS DGNS_DRG_CD,
                RPAD(COALESCE(CPM.CLM_PRNCPL_DGNS_CD,''),7,' ') AS CLM_PRNCPL_DGNS_CD,
                RPAD(COALESCE(DC1.DGNS_CD_DESC,''),250,' ') AS DGNS_CD_DESC,
                RPAD(REPLACE(COALESCE(CL.CLM_LINE_NDC_CD,''),'~',''),11,' ') AS CLM_LINE_NDC_CD,
                RPAD(REPLACE(COALESCE(CL.CLM_LINE_HCPCS_CD,''),'~',''),5,' ') AS CLM_LINE_HCPCS_CD,	
                RPAD(REPLACE(COALESCE(CL.HCPCS_1_MDFR_CD,''),'~',''),2,' ') AS HCPCS_1_MDFR_CD,
                RPAD(REPLACE(COALESCE(CL.HCPCS_2_MDFR_CD,''),'~',''),2,' ') AS HCPCS_2_MDFR_CD,
                RPAD(REPLACE(COALESCE(CL.HCPCS_3_MDFR_CD,''),'~',''),2,' ') AS HCPCS_3_MDFR_CD,	
                RPAD(REPLACE(COALESCE(CL.HCPCS_4_MDFR_CD,''),'~',''),2,' ') AS HCPCS_4_MDFR_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_1_CD,''),'~',''),7,' ') AS CLM_DGNS_1_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_2_CD,''),'~',''),7,' ') AS CLM_DGNS_2_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_3_CD,''),'~',''),7,' ') AS CLM_DGNS_3_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_4_CD,''),'~',''),7,' ') AS CLM_DGNS_4_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_5_CD,''),'~',''),7,' ') AS CLM_DGNS_5_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_6_CD,''),'~',''),7,' ') AS CLM_DGNS_6_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_7_CD,''),'~',''),7,' ') AS CLM_DGNS_7_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_8_CD,''),'~',''),7,' ') AS CLM_DGNS_8_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_9_CD,''),'~',''),7,' ') AS CLM_DGNS_9_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_10_CD,''),'~',''),7,' ') AS CLM_DGNS_10_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_11_CD,''),'~',''),7,' ') AS CLM_DGNS_11_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_12_CD,''),'~',''),7,' ') AS CLM_DGNS_12_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_13_CD,''),'~',''),7,' ') AS CLM_DGNS_13_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_14_CD,''),'~',''),7,' ') AS CLM_DGNS_14_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_15_CD,''),'~',''),7,' ') AS CLM_DGNS_15_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_16_CD,''),'~',''),7,' ') AS CLM_DGNS_16_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_17_CD,''),'~',''),7,' ') AS CLM_DGNS_17_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_18_CD,''),'~',''),7,' ') AS CLM_DGNS_18_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_19_CD,''),'~',''),7,' ') AS CLM_DGNS_19_CD,	
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_20_CD,''),'~',''),7,' ') AS CLM_DGNS_20_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_21_CD,''),'~',''),7,' ') AS CLM_DGNS_21_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_22_CD,''),'~',''),7,' ') AS CLM_DGNS_22_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_23_CD,''),'~',''),7,' ') AS CLM_DGNS_23_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_24_CD,''),'~',''),7,' ') AS CLM_DGNS_24_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_DGNS_25_CD,''),'~',''),7,' ') AS CLM_DGNS_25_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_1_CD,''),'~',''),7,' ') AS CLM_PRCDR_1_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_2_CD,''),'~',''),7,' ') AS CLM_PRCDR_2_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_3_CD,''),'~',''),7,' ') AS CLM_PRCDR_3_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_4_CD,''),'~',''),7,' ') AS CLM_PRCDR_4_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_5_CD,''),'~',''),7,' ') AS CLM_PRCDR_5_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_6_CD,''),'~',''),7,' ') AS CLM_PRCDR_6_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_7_CD,''),'~',''),7,' ') AS CLM_PRCDR_7_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_8_CD,''),'~',''),7,' ') AS CLM_PRCDR_8_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_9_CD,''),'~',''),7,' ') AS CLM_PRCDR_9_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_10_CD,''),'~',''),7,' ') AS CLM_PRCDR_10_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_11_CD,''),'~',''),7,' ') AS CLM_PRCDR_11_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_12_CD,''),'~',''),7,' ') AS CLM_PRCDR_12_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_13_CD,''),'~',''),7,' ') AS CLM_PRCDR_13_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_14_CD,''),'~',''),7,' ') AS CLM_PRCDR_14_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_15_CD,''),'~',''),7,' ') AS CLM_PRCDR_15_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_16_CD,''),'~',''),7,' ') AS CLM_PRCDR_16_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_17_CD,''),'~',''),7,' ') AS CLM_PRCDR_17_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_18_CD,''),'~',''),7,' ') AS CLM_PRCDR_18_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_19_CD,''),'~',''),7,' ') AS CLM_PRCDR_19_CD, 
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_20_CD,''),'~',''),7,' ') AS CLM_PRCDR_20_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_21_CD,''),'~',''),7,' ') AS CLM_PRCDR_21_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_22_CD,''),'~',''),7,' ') AS CLM_PRCDR_22_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_23_CD,''),'~',''),7,' ') AS CLM_PRCDR_23_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_24_CD,''),'~',''),7,' ') AS CLM_PRCDR_24_CD,
                RPAD(REPLACE(COALESCE(CPM.CLM_PRCDR_25_CD,''),'~',''),7,' ') AS CLM_PRCDR_25_CD,
                RPAD(REPLACE(COALESCE(C.CLM_BLG_PRVDR_OSCAR_NUM,''),'~',''),20,' ') AS CLM_BLG_PRVDR_OSCAR_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_BLG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_BLG_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_BLG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_BLG_FED_PRVDR_SPCLTY_CD,
                RPAD(REPLACE(COALESCE(C.CLM_ATNDG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_ATNDG_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_ATNDG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_ATNDG_FED_PRVDR_SPCLTY_CD,
                RPAD(REPLACE(COALESCE(C.PRVDR_ATNDG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_ATNDG_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.PRVDR_OPRTG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_OPRTG_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_OPRTG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_OPRTG_FED_PRVDR_SPCLTY_CD,
                RPAD(REPLACE(COALESCE(C.PRVDR_OTHR_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_OTHR_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_OTHR_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_OTHR_FED_PRVDR_SPCLTY_CD,
                RPAD(REPLACE(COALESCE(C.CLM_RNDRG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_RNDRG_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_RNDRG_FED_PRVDR_SPCLTY_CD,
                RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_RFRG_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_RFRG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_RFRG_FED_PRVDR_SPCLTY_CD,
                RPAD(REPLACE(COALESCE(CL.PRVDR_FAC_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_FAC_PRVDR_NPI_NUM,
                RPAD(REPLACE(COALESCE(C.PRVDR_SRVC_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_SRVC_PRVDR_NPI_NUM,

                --***\/***
                --RPAD(COALESCE(PRVDR.PRVDR_LGL_NAME,''),70,' ') AS PRVDR_LGL_NAME,
                RPAD(COALESCE(PRVDR.PRVDR_LGL_NAME,''),100,' ') AS PRVDR_LGL_NAME,
                --***/\***

                RPAD(COALESCE(PRVDR.PRVDR_MLG_TEL_NUM,''),20,' ') AS PRVDR_MLG_TEL_NUM,
                RPAD(COALESCE(PRVDR.PRVDR_MLG_LINE_1_ADR,''),100,' ') AS PRVDR_MLG_LINE_1_ADR,
                RPAD(COALESCE(PRVDR.PRVDR_MLG_LINE_2_ADR,''),100,' ') AS PRVDR_MLG_LINE_2_ADR,
                RPAD(COALESCE(PRVDR.PRVDR_INVLD_MLG_PLC_NAME,''),40,' ') AS PRVDR_INVLD_MLG_PLC_NAME,
                RPAD(COALESCE(PRVDR.PRVDR_INVLD_MLG_STATE_CD,''),2,' ') AS PRVDR_INVLD_MLG_STATE_CD,
                RPAD(COALESCE(PRVDR.PRVDR_INVLD_MLG_ZIP_CD,''),9,' ') AS PRVDR_INVLD_MLG_ZIP_CD,
                TO_CHAR(COALESCE(C.CLM_PMT_AMT,0),'MI0000000000000.00') AS CLM_PMT_AMT,
                RPAD(COALESCE(CI.CLM_MDCR_NPMT_RSN_CD,''),2,' ') AS CLM_MDCR_NPMT_RSN_CD,
                TO_CHAR(COALESCE(C.CLM_PRVDR_PMT_AMT,0),'MI000000000.00') AS CLM_PRVDR_PMT_AMT,
                TO_CHAR(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'MI0000000000000.0000') AS CLM_LINE_SRVC_UNIT_QTY,
                TO_CHAR(COALESCE(C.CLM_BENE_PD_AMT,0),'MI0000000000000.00') AS CLM_BENE_PD_AMT,
                TO_CHAR(COALESCE(C.CLM_SBMT_CHRG_AMT,0),'MI0000000000000.00') AS CLM_SBMT_CHRG_AMT,
                TO_CHAR(COALESCE(C.CLM_ALOWD_CHRG_AMT,0),'MI0000000000000.00') AS CLM_ALOWD_CHRG_AMT,
                TO_CHAR(COALESCE(C.CLM_MDCR_DDCTBL_AMT,0),'MI0000000000000.00') AS CLM_MDCR_DDCTBL_AMT,
                TO_CHAR(COALESCE(CL.CLM_LINE_CVRD_PD_AMT,0),'MI000000000.00') AS CLM_LINE_CVRD_PD_AMT,	
                TO_CHAR(COALESCE(CL.CLM_LINE_BENE_PD_AMT,0),'MI0000000000000.00') AS CLM_LINE_BENE_PD_AMT, 
                TO_CHAR(COALESCE(CL.CLM_LINE_BENE_PMT_AMT,0),'MI0000000000000.00') AS CLM_LINE_BENE_PMT_AMT,	
                TO_CHAR(COALESCE(CL.CLM_LINE_PRVDR_PMT_AMT,0),'MI000000000.00') AS CLM_LINE_PRVDR_PMT_AMT,	
                TO_CHAR(COALESCE(CL.CLM_LINE_MDCR_DDCTBL_AMT,0),'MI000000000.00') AS CLM_LINE_MDCR_DDCTBL_AMT,
                RPAD(REPLACE(COALESCE(CI.CLM_NCH_PRMRY_PYR_CD,''),'~',''),1,' ') AS CLM_NCH_PRMRY_PYR_CD,
                TO_CHAR(COALESCE(CI.CLM_MDCR_INSTNL_BENE_PD_AMT,0),'MI000000000.00') AS CLM_MDCR_INSTNL_BENE_PD_AMT,
                TO_CHAR(COALESCE(CI.CLM_MDCR_INSTNL_PRMRY_PYR_AMT,0),'MI000000000.00') AS CLM_MDCR_INSTNL_PRMRY_PYR_AMT,
                TO_CHAR(COALESCE(CL.CLM_LINE_MDCR_COINSRNC_AMT,0),'MI0000000000.00') AS CLM_LINE_MDCR_COINSRNC_AMT,
                RPAD(TO_CHAR(COALESCE(CL.CLM_LINE_PTB_BLOOD_DDCTBL_QTY,0)),3,'0') AS CLM_LINE_PTB_BLOOD_DDCTBL_QTY,
                TO_CHAR(COALESCE(CL.CLM_LINE_SBMT_CHRG_AMT,0),'MI0000000000000.00') AS CLM_LINE_SBMT_CHRG_AMT, 
                TO_CHAR(COALESCE(CL.CLM_LINE_ALOWD_CHRG_AMT,0),'MI0000000000000.00') AS CLM_LINE_ALOWD_CHRG_AMT, 
                TO_CHAR(COALESCE(CL.CLM_LINE_OTHR_TP_PD_AMT,0),'MI0000000000000.00') AS CLM_LINE_OTHR_TP_PD_AMT, 
                TO_CHAR(COALESCE(C.CLM_OTHR_TP_PD_AMT,0),'MI0000000000000.00') AS CLM_OTHR_TP_PD_AMT, 
                TO_CHAR(COALESCE(CL.CLM_LINE_BENE_COPMT_AMT,0),'MI0000000000000.00') AS CLM_LINE_BENE_COPMT_AMT, 
                C.CLM_FINL_ACTN_IND AS CLM_FINAL_ACTION_IND,
                RPAD(COALESCE(BENE.BENE_MBI_ID,''),11,' ') AS BENE_MBI_ID,
                RPAD(COALESCE(BENE.BENE_EQTBL_BIC_HICN_NUM,''),11,' ') AS BENE_EQTBL_BIC_HICN_NUM,
                RPAD(REPLACE(COALESCE(C.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_RNDRG_FED_PRVDR_SPCLTY_CD,
                'END' AS END_OF_FILE
            FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C	

            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                ON	 C.GEO_BENE_SK    = CL.GEO_BENE_SK
                AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                AND C.CLM_FROM_DT     = CL.CLM_FROM_DT
                
            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PROD_MTRLZD CPM
                ON  C.GEO_BENE_SK     = CPM.GEO_BENE_SK
                AND C.CLM_DT_SGNTR_SK = CPM.CLM_DT_SGNTR_SK
                AND C.CLM_TYPE_CD     = CPM.CLM_TYPE_CD
                AND C.CLM_NUM_SK      = CPM.CLM_NUM_SK

            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_INSTNL  CI
                ON  C.GEO_BENE_SK     = CI.GEO_BENE_SK
                AND C.CLM_DT_SGNTR_SK = CI.CLM_DT_SGNTR_SK
                AND C.CLM_TYPE_CD     = CI.CLM_TYPE_CD
                AND C.CLM_NUM_SK      = CI.CLM_NUM_SK

            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
                ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK
                
            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE BENE
                ON  C.BENE_SK = BENE.BENE_SK
                
            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_VDM_VIEW_MDCR_{ENVNAME}.V2_MDCR_DGNS_CD DC1
                ON  CL.CLM_LINE_DGNS_CD = DC1.DGNS_CD
                AND C.CLM_THRU_DT BETWEEN DC1.DGNS_CD_BGN_DT AND DC1.DGNS_CD_END_DT

            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_PRVDR_{ENVNAME}.PRVDR PRVDR
                ON C.CLM_BLG_PRVDR_NPI_NUM = PRVDR.PRVDR_NPI_NUM

            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP5_CD ZIP5
                ON BENE.GEO_SK = ZIP5.GEO_SK

            LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_FIPS_CNTY_CD G
                ON  ZIP5.GEO_FIPS_CNTY_CD  = G.GEO_FIPS_CNTY_CD
                AND ZIP5.GEO_FIPS_STATE_CD = G.GEO_FIPS_STATE_CD

            WHERE C.CLM_TYPE_CD IN ({CLM_TYPE_CD})
              AND C.CLM_FINL_ACTN_IND = 'Y'
              AND C.CLM_FROM_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
            ) 
            
            FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER='|' ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
              max_file_size=5368709120 """,con,exit_on_error=True)
                        
                        
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
