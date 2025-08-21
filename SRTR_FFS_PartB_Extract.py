#!/usr/bin/env python
########################################################################################################
# Name:  SRTR_FFS_PartB_Extract.py
# DESC:  This python script extracts data from IDRC for the SRTR FFS Part B (CAR, DME) extracts
#
# Created: Joshua Turner
# Modified: 02/13/2023
#
# Joshua Turner  2023-05-17  Changed DME file name to match the standard for EFT functionality 
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

EXT_YR = os.getenv('EXT_YEAR')
ENVNAME = os.getenv('ENVNAME')
TMSTMP = os.getenv('TMSTMP')
CTYP = os.getenv('CTYP')
CLM_TYPE_CD1 = os.getenv('CLM_TYPE_CD1')
CLM_TYPE_CD2 = os.getenv('CLM_TYPE_CD2')
SRTR_FFS_BUCKET = os.getenv('SRTR_FFS_BUCKET')

# Get the last two digits of the year for the extract filename
YY = EXT_YR[2:]
 
########################################################################################################
# Execute extract based on parameters set in the RUN section at the bottom of the script
########################################################################################################
def execute_partb_extract():
    # boolean - Python Exception status
    bPythonExceptionOccurred=False

    try:
        snowconvert_helpers.configure_log()
        con = snowconvert_helpers.log_on()
        snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
        snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

        ########################################################################################################
        # Extract SRTR FFS Part B data and write to S3 as a flat file
        ########################################################################################################
        snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_SRTRFFSPTAB_STG/{XTR_FILE_NAME}
                FROM (
                SELECT DISTINCT
                    'START' AS ST_OF_FILE,
                    LPAD(CL.CLM_TYPE_CD,5,'0'),
                    LPAD(CL.CLM_LINE_NUM,5,'0'),
                    RPAD(COALESCE(C.CLM_HIC_NUM,''),12,' ') AS HIC_NUM,
                    RPAD(REPLACE(COALESCE(GFCC.GEO_SSA_STATE_CD,''),'~',''),2,' ') AS GEO_SSA_STATE_CD,
                    RPAD(COALESCE(TO_CHAR(C.CLM_FROM_DT, 'YYYYMMDD'),''),8,' ') AS CLM_FROM_DT,
                    RPAD(COALESCE(TO_CHAR(C.CLM_THRU_DT, 'YYYYMMDD'),''),8,' ') AS CLM_THRU_DT,
                    RPAD(C.CLM_CNTL_NUM,40,' ') AS CLM_CNTL_NUM,
                    RPAD(COALESCE(TO_CHAR(CDS.CLM_CWF_ACRTN_DT, 'YYYYMMDD'),''),8,' ') AS CLM_CWF_ACRTN_DT,
                    RPAD(COALESCE(TO_CHAR(CDS.CLM_NCH_WKLY_PROC_DT, 'YYYYMMDD'),''),8,' ') AS CLM_NCH_WKLY_PROC_DT,
                    RPAD(REPLACE(COALESCE(C.CLM_DISP_CD,''),'~',''),2,' ') AS CLM_DISP_CD,
                    RPAD(REPLACE(C.CLM_QUERY_CD,'~',''),1,' ') AS CLM_QUERY_CD,
                    RPAD(REPLACE(GFCC.GEO_SSA_CNTY_CD,'~',''),3,' ') AS GEO_SSA_CNTY_CD,		 
                    RPAD(COALESCE(TO_CHAR(CDS.CLM_SUBMSN_DT, 'YYYYMMDD'),''),8,' ') AS CLM_SUBMSN_DT,
                    RPAD(COALESCE(TO_CHAR(CDS.CLM_SCHLD_PMT_DT, 'YYYYMMDD'),''),8,' ') AS CLM_SCHLD_PMT_DT,
                    RPAD(REPLACE(COALESCE(CL.CLM_CNTRCTR_NUM,''),'~',''),5,' ') AS CLM_CNTRCTR_NUM,
                    RPAD(REPLACE(COALESCE(ZIP5.GEO_ZIP5_CD,''),'~',''),5,' ') AS GEO_ZIP5_CD,
                    RPAD(REPLACE(COALESCE(C.BENE_SEX_CD,''),'~',''),1,' ') AS BENE_SEX_CD,
                    RPAD(REPLACE(COALESCE(CDN.BENE_RACE_CD,''),'~',''),2,' ') AS BENE_RACE_CD,
                    RPAD(COALESCE(TO_CHAR(C.CLM_PTNT_BIRTH_DT, 'YYYYMMDD'),''),8,' ') AS CLM_PTNT_BIRTH_DT,
                    RPAD(COALESCE(CDN.CLM_CWF_BENE_MDCR_STUS_CD,''),2,' ') AS CLM_CWF_BENE_MDCR_STUS_CD,
                    RPAD(COALESCE(B.BENE_LAST_NAME,''),60,' ') AS BENE_LAST_NAME,
                    RPAD(COALESCE(B.BENE_1ST_NAME,''),35,' ') AS BENE_1ST_NAME,
                    COALESCE(B.BENE_MIDL_NAME,' ') AS BENE_MIDL_NAME,
                    COALESCE(CL.CLM_DGNS_PRCDR_ICD_IND, ' ') AS CLM_DGNS_PRCDR_ICD_IND,
                    RPAD(COALESCE(CPM.CLM_PRNCPL_DGNS_CD,''),7,' ') AS CLM_PRNCPL_DGNS_CD,
                    RPAD(REPLACE(COALESCE(PRFNL.CLM_CARR_PMT_DNL_CD,''),'~',''),2,' ') AS CLM_CARR_PMT_DNL_CD,
                    TO_CHAR(COALESCE(C.CLM_PMT_AMT,0),'MI0000000000000.00') AS CLM_PMT_AMT,
                    TO_CHAR(COALESCE(PRFNL.CLM_MDCR_PRFNL_PRMRY_PYR_AMT,0),'MI000000000.00') AS CLM_MDCR_PRFNL_PRMRY_PYR_AMT,
                    RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_UPIN_NUM,''),'~',''),6,' ') AS CLM_RFRG_PRVDR_UPIN_NUM,
                    RPAD(REPLACE(COALESCE(C.PRVDR_RFRG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_RFRG_PRVDR_NPI_NUM,
                    RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_RFRG_PRVDR_NPI_NUM,
                    TO_CHAR(COALESCE(C.CLM_PRVDR_PMT_AMT,0),'MI000000000.00') AS CLM_PRVDR_PMT_AMT,
                    TO_CHAR(COALESCE(C.CLM_BENE_PD_AMT,0),'MI0000000000000.00') AS CLM_BENE_PD_AMT,
                    TO_CHAR(COALESCE(C.CLM_SBMT_CHRG_AMT,0),'MI0000000000000.00') AS CLM_SBMT_CHRG_AMT,
                    TO_CHAR(COALESCE(C.CLM_ALOWD_CHRG_AMT,0),'MI0000000000000.00') AS CLM_ALOWD_CHRG_AMT,
                    TO_CHAR(COALESCE(C.CLM_MDCR_DDCTBL_AMT,0),'MI0000000000000.00') AS CLM_MDCR_DDCTBL_AMT,
                    RPAD(REPLACE(COALESCE(PRFNL.CLM_MCO_OVRRD_CD,''),'~',''),1,' ') AS CLM_MCO_OVRRD_CD,
                    RPAD(REPLACE(COALESCE(C.CLM_RAC_ADJSTMT_IND_CD,''),'~',''),1,' ') AS CLM_RAC_ADJSTMT_IND_CD,
                    RPAD(REPLACE(COALESCE(CDN.CLM_FPS_RMRK_CD,''),'~',''),5,' ') AS CLM_FPS_RMRK_CD,
                    RPAD(REPLACE(COALESCE(CDN.CLM_MASS_ADJSTMT_TYPE_CD,''),'~',''),1,' ') AS CLM_MASS_ADJSTMT_TYPE_CD,
                    RPAD(REPLACE(COALESCE(C.CLM_RFRG_PRVDR_PIN_NUM,''),'~',''),14,' ') AS CLM_RFRG_PRVDR_PIN_NUM,
                    RPAD(REPLACE(COALESCE(C.PRVDR_CPO_FAC_NPI_NUM,''),'~',''),10,' ') AS PRVDR_CPO_FAC_NPI_NUM,
                    RPAD(TO_CHAR(COALESCE(C.CLM_BLOOD_PT_FRNSH_QTY,0)),3,'0') AS CLM_BLOOD_PT_FRNSH_QTY,
                    RPAD(TO_CHAR(COALESCE(CDN.CLM_BLOOD_DDCTBL_PT_QTY,0)),4,'0') AS CLM_BLOOD_DDCTBL_PT_QTY,
                    RPAD(REPLACE(COALESCE(C.CLM_BLG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_BLG_PRVDR_NPI_NUM,
                    RPAD(REPLACE(COALESCE(C.PRVDR_BLG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_BLG_PRVDR_NPI_NUM,
                    LPAD(TO_CHAR(COALESCE(CPM.CLM_DGNS_TOT_OCRNC_CNT,0)),2,'0') AS CLM_DGNS_TOT_OCRNC_CNT,
                    RPAD(REPLACE(COALESCE(CDN.CLM_MCO_1ST_CNTRCT_NUM,''),'~',''),5,' ') AS CLM_MCO_1ST_CNTRCT_NUM,
                    RPAD(REPLACE(COALESCE(CDN.CLM_MCO_2ND_CNTRCT_NUM,''),'~',''),5,' ') AS CLM_MCO_2ND_CNTRCT_NUM,
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
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_PIN_NUM,''),'~',''),14,' ') AS CLM_RNDRG_PRVDR_PIN_NUM,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_UPIN_NUM,''),'~',''),6,' ') AS CLM_RNDRG_PRVDR_UPIN_NUM,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS CLM_RNDRG_PRVDR_NPI_NUM,	
                    RPAD(REPLACE(COALESCE(CL.PRVDR_RNDRNG_PRVDR_NPI_NUM,''),'~',''),10,' ') AS PRVDR_RNDRNG_PRVDR_NPI_NUM,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_GRP_NPI_NUM,''),'~',''),10,' ') AS CLM_RNDRG_PRVDR_GRP_NPI_NUM,	
                    RPAD(REPLACE(COALESCE(CL.PRVDR_RNDRNG_PRVDR_GRP_NPI_NUM,''),'~',''),10,' ') AS PRVDR_RNDRNG_PRVDR_GRP_NPI_NUM,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_TYPE_CD,''),'~',''),3,' ') AS CLM_RNDRG_PRVDR_TYPE_CD,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_TAX_NUM,''),'~',''),10,' ') AS CLM_RNDRG_PRVDR_TAX_NUM,	
                    RPAD(REPLACE(COALESCE(CL.GEO_RNDRG_SSA_STATE_CD,''),'~',''),2,' ') AS GEO_RNDRG_SSA_STATE_CD,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_ZIP5_CD,''),'~',''),5,' ') || RPAD(REPLACE(COALESCE(CL.GEO_RNDRG_ZIP4_CD,''),'~',''),4,' ') AS GEO_RNDRG_ZIP9_CD,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_RNDRG_FED_PRVDR_SPCLTY_CD,	
                    RPAD(REPLACE(COALESCE(CLP.CLM_PRVDR_SPCLTY_CD,''),'~',''),2,' ') AS CLM_PRVDR_SPCLTY_CD,	
                    RPAD(REPLACE(COALESCE(CL.CLM_RNDRG_PRVDR_PRTCPTG_CD,''),'~',''),1,' ') AS CLM_RNDRG_PRVDR_PRTCPTG_CD,
                    RPAD(REPLACE(COALESCE(CLP.CLM_PHYSN_ASTNT_CD,''),'~',''),1,' ') AS CLM_PHYSN_ASTNT_CD,
                    TO_CHAR(COALESCE(CL.CLM_LINE_SRVC_UNIT_QTY,0),'MI00000000000000.0000') AS CLM_LINE_SRVC_UNIT_QTY,
                    RPAD(REPLACE(COALESCE(CLP.CLM_FED_TYPE_SRVC_CD,''),'~',''),1,' ') AS CLM_FED_TYPE_SRVC_CD,
                    RPAD(REPLACE(COALESCE(CLP.CLM_TYPE_SRVC_CD,''),'~',''),2,' ') AS CLM_TYPE_SRVC_CD,
                    RPAD(REPLACE(COALESCE(CLP.CLM_PRCNG_LCLTY_CD,''),'~',''),2,' ') AS CLM_PRCNG_LCLTY_CD,
                    RPAD(REPLACE(COALESCE(CL.CLM_POS_CD,''),'~',''),2,' ') AS CLM_POS_CD,
                    RPAD(COALESCE(TO_CHAR(CL.CLM_LINE_FROM_DT, 'YYYYMMDD'),''),8,' ') AS CLM_LINE_FROM_DT,
                    RPAD(COALESCE(TO_CHAR(CL.CLM_LINE_THRU_DT, 'YYYYMMDD'),''),8,' ') AS CLM_LINE_THRU_DT,
                    RPAD(REPLACE(COALESCE(CL.CLM_LINE_HCPCS_CD,''),'~',''),5,' ') AS CLM_LINE_HCPCS_CD,
                    RPAD(REPLACE(COALESCE(CL.HCPCS_1_MDFR_CD,''),'~',''),2,' ') AS HCPCS_1_MDFR_CD,
                    RPAD(REPLACE(COALESCE(CL.HCPCS_2_MDFR_CD,''),'~',''),2,' ') AS HCPCS_2_MDFR_CD,
                    RPAD(REPLACE(COALESCE(CL.HCPCS_3_MDFR_CD,''),'~',''),2,' ') AS HCPCS_3_MDFR_CD,
                    RPAD(REPLACE(COALESCE(CL.HCPCS_4_MDFR_CD,''),'~',''),2,' ') AS HCPCS_4_MDFR_CD,
                    TO_CHAR(COALESCE(CL.CLM_LINE_CVRD_PD_AMT,0),'MI000000000.00') AS CLM_LINE_CVRD_PD_AMT, -- 9.2
                    TO_CHAR(COALESCE(CL.CLM_LINE_BENE_PD_AMT,0),'MI0000000000000.00') AS CLM_LINE_BENE_PD_AMT, -- 13.2
                    TO_CHAR(COALESCE(CL.CLM_LINE_BENE_PMT_AMT,0),'MI0000000000000.00') AS CLM_LINE_BENE_PMT_AMT,
                    TO_CHAR(COALESCE(CL.CLM_LINE_PRVDR_PMT_AMT,0),'MI000000000.00') AS CLM_LINE_PRVDR_PMT_AMT,
                    TO_CHAR(COALESCE(CL.CLM_LINE_MDCR_DDCTBL_AMT,0),'MI000000000.00') AS CLM_LINE_MDCR_DDCTBL_AMT,
                    RPAD(REPLACE(COALESCE(CLP.CLM_PRMRY_PYR_CD,''),'~',''),1,' ') AS CLM_PRMRY_PYR_CD,
                    TO_CHAR(COALESCE(CLP.CLM_BENE_PRMRY_PYR_PD_AMT,0),'MI000000000.00') AS CLM_BENE_PRMRY_PYR_PD_AMT,
                    TO_CHAR(COALESCE(CL.CLM_LINE_MDCR_COINSRNC_AMT,0),'MI000000000.00') AS CLM_LINE_MDCR_COINSRNC_AMT,
                    TO_CHAR(COALESCE(CLP.CLM_MDCR_PRMRY_PYR_ALOWD_AMT,0),'MI0000000000000.00') AS CLM_MDCR_PRMRY_PYR_ALOWD_AMT,
                    RPAD(TO_CHAR(COALESCE(CL.CLM_LINE_PTB_BLOOD_DDCTBL_QTY,0)),3,'0') AS CLM_LINE_PTB_BLOOD_DDCTBL_QTY,
                    TO_CHAR(COALESCE(CL.CLM_LINE_SBMT_CHRG_AMT,0),'MI0000000000000.00') AS CLM_LINE_SBMT_CHRG_AMT,
                    TO_CHAR(COALESCE(CL.CLM_LINE_ALOWD_CHRG_AMT,0),'MI0000000000000.00') AS CLM_LINE_ALOWD_CHRG_AMT,
                    TO_CHAR(COALESCE(CL.CLM_LINE_OTHR_TP_PD_AMT,0),'MI0000000000000.00') AS CLM_LINE_OTHR_TP_PD_AMT,
                    TO_CHAR(COALESCE(C.CLM_OTHR_TP_PD_AMT,0),'MI0000000000000.00') AS CLM_OTHR_TP_PD_AMT,
                    TO_CHAR(COALESCE(CL.CLM_LINE_BENE_COPMT_AMT,0),'MI0000000000000.00') AS CLM_LINE_BENE_COPMT_AMT,
                    RPAD(REPLACE(COALESCE(CLP.CLM_PMT_IND_CD,''),'~',''),1,' ') AS CLM_PMT_IND_CD,
                    RPAD(REPLACE(COALESCE(CL.CLM_LINE_DGNS_CD,''),'~',''),7,' ') AS CLM_LINE_DGNS_CD,
                    RPAD(COALESCE(TO_CHAR(CLP.CLM_LINE_CARR_DME_CVRG_BGN_DT, 'YYYYMMDD'),''),8,' ') AS CLM_LINE_CARR_DME_CVRG_BGN_DT,
                    TO_CHAR(COALESCE(CLP.CLM_LINE_PRFNL_DME_PRICE_AMT,0),'MI000000000.00') AS CLM_LINE_PRFNL_DME_PRICE_AMT,
                    RPAD(REPLACE(COALESCE(CLP.CLM_DUP_CHK_IND_CD,''),'~',''),1,' ') AS CLM_DUP_CHK_IND_CD,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PHYSN_ORG_NAME,''),'~',''),60,' ') AS CLM_POS_PHYSN_ORG_NAME,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_1ST_NAME,''),'~',''),35,' ') AS CLM_POS_PRVDR_1ST_NAME,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_MDL_NAME,''),'~',''),25,' ') AS CLM_POS_PRVDR_MDL_NAME,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_1ST_LINE_ADR,''),'~',''),55,' ') AS CLM_POS_PRVDR_1ST_LINE_ADR,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_2ND_LINE_ADR,''),'~',''),55,' ') AS CLM_POS_PRVDR_2ND_LINE_ADR,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_CITY_NAME,''),'~',''),30,' ') AS CLM_POS_PRVDR_CITY_NAME,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_USPS_STATE_CD,''),'~',''),2,' ') AS CLM_POS_PRVDR_USPS_STATE_CD,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_ZIP5_CD,''),'~',''),5,' ') AS CLM_POS_PRVDR_ZIP5_CD,
                    RPAD(REPLACE(COALESCE(CLP.CLM_POS_PRVDR_ZIP4_CD,''),'~',''),4,' ') AS CLM_POS_PRVDR_ZIP4_CD,
                    C.CLM_FINL_ACTN_IND,
                    RPAD(COALESCE(B.BENE_MBI_ID,''),11,' ') AS BENE_MBI_ID,
                    'END' AS END_OF_FILE
                FROM "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM" C 

                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM_LINE" CL
                    ON C.GEO_BENE_SK      = CL.GEO_BENE_SK
                    AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                    AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                    AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                    AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

                INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE" B
                    ON C.BENE_SK = B.BENE_SK

                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM_DT_SGNTR" CDS
                    ON CDS.CLM_DT_SGNTR_SK = C.CLM_DT_SGNTR_SK

                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM_PROD_MTRLZD" CPM
                    ON  C.GEO_BENE_SK     = CPM.GEO_BENE_SK
                    AND C.CLM_DT_SGNTR_SK = CPM.CLM_DT_SGNTR_SK
                    AND C.CLM_TYPE_CD     = CPM.CLM_TYPE_CD
                    AND C.CLM_NUM_SK      = CPM.CLM_NUM_SK

                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM_DCMTN" CDN
                    ON C.GEO_BENE_SK      = CDN.GEO_BENE_SK
                    AND C.CLM_DT_SGNTR_SK = CDN.CLM_DT_SGNTR_SK
                    AND C.CLM_TYPE_CD     = CDN.CLM_TYPE_CD
                    AND C.CLM_NUM_SK      = CDN.CLM_NUM_SK

                INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_ZIP5_CD" ZIP5
                    ON ZIP5.GEO_SK = C.GEO_BENE_SK

                LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_FIPS_CNTY_CD" GFCC
                    ON GFCC.GEO_FIPS_CNTY_CD = ZIP5.GEO_FIPS_CNTY_CD
                    AND GFCC.GEO_FIPS_STATE_CD = ZIP5.GEO_FIPS_STATE_CD

                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM_LINE_PRFNL" CLP
                    ON  CL.GEO_BENE_SK     = CLP.GEO_BENE_SK
                    AND CL.CLM_DT_SGNTR_SK = CLP.CLM_DT_SGNTR_SK
                    AND CL.CLM_TYPE_CD     = CLP.CLM_TYPE_CD
                    AND CL.CLM_NUM_SK      = CLP.CLM_NUM_SK
                    AND CL.CLM_LINE_NUM    = CLP.CLM_LINE_NUM

                INNER JOIN "IDRC_{ENVNAME}"."CMS_FCT_CLM_{ENVNAME}"."CLM_PRFNL" PRFNL
                    ON  C.GEO_BENE_SK     = PRFNL.GEO_BENE_SK
                    AND C.CLM_DT_SGNTR_SK = PRFNL.CLM_DT_SGNTR_SK
                    AND C.CLM_TYPE_CD     = PRFNL.CLM_TYPE_CD
                    AND C.CLM_NUM_SK      = PRFNL.CLM_NUM_SK

                INNER JOIN "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."SRTR_SSN" SRTR
                    ON B.BENE_SSN_NUM = LTRIM(RTRIM(SRTR.SSN))

                WHERE C.CLM_TYPE_CD BETWEEN {CLM_TYPE_CD1} AND {CLM_TYPE_CD2}
                  AND C.CLM_FINL_ACTN_IND = 'Y'
                  AND C.CLM_FROM_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
                ) FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER='|' ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
                  max_file_size=5368709120 """,con,exit_on_error=True)

                  
    except Exception as e:
        print(e)
        # Let shell script know that python code failed.
        bPythonExceptionOccurred=True 

        return bPythonExceptionOccurred
        
    finally:
        if con is not None:
            con.close()

        return bPythonExceptionOccurred

########################################################################################################
# RUN
# If CTYP is CAR, set range parms and loop to call the extracts
# If CTYP is DME, set static date parms and call the extract
########################################################################################################
bErrorOccurred = False
print('')
print("Run date and time: " + date_time  )
print

if CTYP == "CAR":
    start_date_parms = ['-01-01', '-04-01', '-07-01', '-10-01']
    end_date_parms = ['-03-31', '-06-30', '-09-30', '-12-31']
    
    for i in range(len(start_date_parms)):
        START_DATE = EXT_YR + start_date_parms[i]
        END_DATE = EXT_YR + end_date_parms[i]
        RNG = i + 1
        XTR_FILE_NAME = f"SRTR_FFS_{CTYP}F_Y{YY}PF{RNG}_{TMSTMP}.txt.gz"
        
        bErrorOccurred = execute_partb_extract()

        # Let shell script know that python code failed.
        if bErrorOccurred == True:
            sys.exit(12)

        os.system(f"/app/IDRC/XTR/CMS/scripts/run/CombineS3Files.sh {SRTR_FFS_BUCKET} {XTR_FILE_NAME}")
        #os.system(f"python /app/IDRC/XTR/CMS/scripts/run/combineS3Files.py --bucket 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod' --folder 'xtr/DEV/SRTR_FFSPTAB' --prefix 'SRTR_FFS_{CTYP}_Y{EXT_YR}_P{RNG}_{TMSTMP}' --output 'xtr/DEV/SRTR_FFSPTAB/{XTR_FILE_NAME}' --filesize 5368709120")
		
elif CTYP == "DME":
    START_DATE = EXT_YR + '-01-01'
    END_DATE = EXT_YR + '-12-31'
    RNG = 1
    XTR_FILE_NAME = f"SRTR_FFS_{CTYP}F_Y{YY}PF{RNG}_{TMSTMP}.txt.gz"
    
    bErrorOccurred = execute_partb_extract()

    # Let shell script know that python code failed.
    if bErrorOccurred == True:
        sys.exit(12)

    os.system(f"/app/IDRC/XTR/CMS/scripts/run/CombineS3Files.sh {SRTR_FFS_BUCKET} {XTR_FILE_NAME}")
    #os.system(f"python /app/IDRC/XTR/CMS/scripts/run/combineS3Files.py --bucket 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod' --folder 'xtr/DEV/SRTR_FFSPTAB' --prefix 'SRTR_FFS_{CTYP}_Y{EXT_YR}_{TMSTMP}' --output 'xtr/DEV/SRTR_FFSPTAB/{XTR_FILE_NAME}' --filesize 5368709120")
    

else:
    # Invalid CTYP code supplied to the script
    sys.exit(12) 

snowconvert_helpers.quit_application()
