#!/usr/bin/env python
########################################################################################################
# Name:  HOS_EXTRACT.py
# DESC:  This script executes the python that creates the two HOS extracts for H and M contract types
#
# Author:  Joshua Turner
# Created: 01/19/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2023-10-19   Changed length of BENE_SK (BENE_LINK_KEY) from 38 to 9
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

SDATE = os.getenv('SDATE')
HOS_FF_TABLE = os.getenv('HOS_FF_TABLE')
FILETYPE = os.getenv('FILETYPE')
YEAR = os.getenv('YEAR')
ENVNAME = os.getenv('ENVNAME')
TMSTMP = os.getenv('TMSTMP')

# boolean - Python Exception status
bPythonExceptionOccurred=False

########################################################################################################
# RUN
########################################################################################################
print('')
print("Run date and time: " + date_time  )
print

try:
    snowconvert_helpers.configure_log()
    con = snowconvert_helpers.log_on()
    snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
    snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)
    
    ########################################################################################################
    # Delete all data from both of the FF tables
    ########################################################################################################
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_HOS_STG/HOS_XTR_Y{YEAR}_{FILETYPE}_{TMSTMP}.csv.gz
                    FROM (
                        SELECT DISTINCT
                        --BENE_FCT.BENE_CAN_NUM AS CHAR(9)) AS BENE_CAN_NUM,
                        --BENE_FCT.BENE_BIC_CD AS CHAR(2)) AS BIC_CD,
                        RPAD(COALESCE(BENE.BENE_1ST_NAME,''),30,' ') AS BENE_1ST_NAME,
                        RPAD(COALESCE(BENE.BENE_MIDL_NAME,''),15,' ') AS MDL_NAME,
                        RPAD(COALESCE(BENE.BENE_LAST_NAME,''),40, ' ') AS BENE_LAST_NAME,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_BIRTH_DT),''),39, ' ') AS BENE_BRTH_DT,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_DEATH_DT),''),39, ' ') AS BENE_DEATH_DT,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_SK),''),9,' ') AS BENE_LINK_KEY,
                        RPAD(COALESCE(BENE_FCT.BENE_SEX_CD ,''),1,' ') AS GNDR_CD,
                        RPAD(COALESCE(BENE_FCT.BENE_RACE_CD,''),1,' ') AS RACE_CD,
                        --RPAD(COALESCE(BENE.BENE_RRB_NUM,''),12,' ') AS RRB_HIC_NUM,
                        --RPAD(COALESCE(BENE_FCT.BENE_SSN_NUM,''),9,' ') AS SSN,
                        RPAD(COALESCE(GEO_FIPS_STATE_CD.GEO_USPS_STATE_CD,''),2,' ') AS DRVD_PSTL_STATE_CD,
                        RPAD(COALESCE(TRIM(GEO_ZIP5_CD.GEO_ZIP_PLC_NAME),''),40,' ') AS DRVD_ADR_CITY_NAME,
                        RPAD(COALESCE(GEO_ZIP5_CD.GEO_ZIP5_CD, ''),5,' ') || RPAD(COALESCE(BENE.GEO_ZIP4_CD,''),4,' ') AS DRVD_ADR_ZIP_CD,
                        RPAD(COALESCE(BENE.BENE_LINE_1_ADR,''),45,' ') AS DRVD_LINE_1_ADR,
                        RPAD(COALESCE(BENE.BENE_LINE_2_ADR,''),45,' ') AS DRVD_LINE_2_ADR,
                        RPAD(COALESCE(BENE.BENE_LINE_3_ADR,''),40,' ') AS DRVD_LINE_3_ADR,
                        RPAD(COALESCE(BENE.BENE_LINE_4_ADR,''),40,' ') AS DRVD_LINE_4_ADR,
                        RPAD(COALESCE(BENE.BENE_LINE_5_ADR,''),40,' ') AS DRVD_LINE_5_ADR,
                        RPAD(COALESCE(BENE.BENE_LINE_6_ADR,''),40,' ') AS DRVD_LINE_6_ADR,
                        RPAD(COALESCE(GEO_FIPS_CNTY_CD.GEO_SSA_STATE_CD,''),2,' ') AS SSA_STATE_CD,
                        RPAD(COALESCE(GEO_FIPS_CNTY_CD.GEO_SSA_CNTY_CD,''),3,' ') AS SSA_CNTY_CD,
                        RPAD(COALESCE(BENE_FCT.BENE_MDCR_STUS_CD,''),2,' ') AS MDCR_STUS_CD,
                        RPAD(COALESCE(BENE_FCT.CNTRCT_PTC_NUM,''),5,' ') AS PTAB_CNTRCT_NUM,
                        RPAD(COALESCE(BENE_FCT.CNTRCT_PBP_PTC_NUM,''),3,' ') AS PTAB_PBP_NUM,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_PTAPTB_STRT_DT),''),10,' ') AS PTAB_START_DT,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_PTAPTB_END_DT),''),10,' ') AS PTAB_END_DT,
                        RPAD(COALESCE(CNTRCT_PBP_CNTRCT.CNTRCT_NAME,''),100,' ') AS CNTRCT_NAME,
                        RPAD(COALESCE(CNTRCT_PBP_CNTRCT.CNTRCT_TYPE_CD,''),2,' ') AS ORG_TYPE_CD,
                        RPAD(COALESCE(BENE_FCT.BENE_CVRG_TYPE_CD,''),2,' ') AS CVRG_TYPE_CD,
                        RPAD(COALESCE(BENE_CVRG_TYPE_CD.BENE_CVRG_TYPE_CD_DESC,''),5,' ') AS CVRG_TYPE_SHRT_DESC,
                        RPAD(COALESCE(BENE_FCT.CNTRCT_PTD_NUM,''),5,' ') AS PTD_CNTRCT_NUM,
                        RPAD(COALESCE(BENE_FCT.CNTRCT_PBP_PTD_NUM,''),3,' ') AS PTD_PBP_NUM,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_PTD_STRT_DT),''),10,' ') AS PTD_START_DT,
                        RPAD(COALESCE(TO_CHAR(BENE_FCT.BENE_PTD_END_DT),''),10,' ') AS PTD_END_DT,
                        RPAD(COALESCE(BENE_FCT.BENE_MDCD_STUS_CD,''),1,' ') AS MDCD_STUS_CD,
                        RPAD(COALESCE(GEO_ZIP5_CD.GEO_FIPS_STATE_CD,''),2,' ') AS FIPS_STATE_CD,
                        RPAD(COALESCE(GEO_ZIP5_CD.GEO_FIPS_CNTY_CD,''),3,' ') AS FIPS_CNTY_CD,
                        RPAD(COALESCE(BENE_FCT.BENE_DUAL_STUS_CD,''),2,' ') AS DUAL_STUS_CD,
                        RPAD(COALESCE(BENE.CNTCT_LANG_CD,''),3,' ') AS CNTCT_LANG_CD,
                        RPAD(COALESCE(BENE_FCT.BENE_DUAL_MDCD_ELGBL_STUS_SW,''),1,' ') AS MDCD_ELGBL_STUS_SW,
                        CASE WHEN BENE_ESRD_CVRG.BENE_SK IS NULL
                             THEN 'N'
                             ELSE 'Y' END AS ESRD_STUS,
                        CASE WHEN BENE_HOSPC.BENE_SK IS NULL
                             THEN 'N'
                             ELSE 'Y' END AS HOSPC_STUS, 
                        RPAD(COALESCE(CNTRCT_PBP_NUM.CNTRCT_SPCL_PLAN_IND_CD,''),2,' ') AS PTD_CNTRCT_SPCL_PLAN_IND,
                        RPAD(COALESCE(CNTRCT_PBP_NUM2.CNTRCT_SPCL_PLAN_IND_CD,''),2,' ') AS PTAB_CNTRCT_SPCL_PLAN_IND,
                        --RPAD(COALESCE(APR.CNTRCT_PTD_NUM_APR,''),5,' ') AS CNTRCT_PTD_NUM_APR,
                        --RPAD(COALESCE(APR.CNTRCT_PTC_NUM_APR,''),5,' ') AS CNTRCT_PTC_NUM_APR,
                        --RPAD(COALESCE(OCT.CNTRCT_PTD_NUM_OCT,''),5,' ') AS CNTRCT_PTD_NUM_OCT,
                        --RPAD(COALESCE(OCT.CNTRCT_PTC_NUM_OCT,''),5,' ') AS CNTRCT_PTC_NUM_OCT,
                        RPAD(COALESCE(BENE.BENE_MBI_ID,''),11,' ') AS BENE_MBI_ID

                    FROM "IDRC_{ENVNAME}"."CMS_FCT_BENE_MTRLZD_{ENVNAME}"."BENE_FCT_TRANS" BENE_FCT

                    INNER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE" BENE 
                        ON BENE_FCT.BENE_SK = BENE.BENE_SK

                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_ZIP5_CD" GEO_ZIP5_CD 
                        ON BENE_FCT.GEO_SK = GEO_ZIP5_CD.GEO_SK

                    INNER JOIN"IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_FIPS_STATE_CD" GEO_FIPS_STATE_CD 
                        ON GEO_FIPS_STATE_CD.GEO_FIPS_STATE_CD = GEO_ZIP5_CD.GEO_FIPS_STATE_CD

                    LEFT OUTER JOIN"IDRC_{ENVNAME}"."CMS_DIM_GEO_{ENVNAME}"."GEO_FIPS_CNTY_CD" GEO_FIPS_CNTY_CD 
                        ON GEO_ZIP5_CD.GEO_FIPS_CNTY_CD = GEO_FIPS_CNTY_CD.GEO_FIPS_CNTY_CD 
                        AND GEO_ZIP5_CD.GEO_FIPS_STATE_CD = GEO_FIPS_CNTY_CD.GEO_FIPS_STATE_CD

                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_CD_{ENVNAME}"."BENE_CVRG_TYPE_CD" BENE_CVRG_TYPE_CD
                        ON BENE_FCT.BENE_CVRG_TYPE_CD = BENE_CVRG_TYPE_CD.BENE_CVRG_TYPE_CD

                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_CNTRCT_{ENVNAME}"."CNTRCT_PBP_NUM" CNTRCT_PBP_NUM
                        ON BENE_FCT.CNTRCT_PTD_NUM = CNTRCT_PBP_NUM.CNTRCT_NUM
                        AND BENE_FCT.CNTRCT_PBP_PTD_NUM = CNTRCT_PBP_NUM.CNTRCT_PBP_NUM
                        AND BENE_FCT.CNTRCT_PBP_PTD_EFCTV_DT =  CNTRCT_PBP_NUM.CNTRCT_PBP_SK_EFCTV_DT
                        AND '{SDATE}' BETWEEN CNTRCT_PBP_NUM.CNTRCT_PBP_SK_EFCTV_DT
                                      AND CNTRCT_PBP_NUM.CNTRCT_PBP_SK_OBSLT_DT

                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_CNTRCT_{ENVNAME}"."CNTRCT_PBP_NUM" CNTRCT_PBP_NUM2
                       ON BENE_FCT.CNTRCT_PTAPTB_NUM = CNTRCT_PBP_NUM2.CNTRCT_NUM
                       AND BENE_FCT.CNTRCT_PBP_PTAPTB_NUM = CNTRCT_PBP_NUM2.CNTRCT_PBP_NUM
                       AND BENE_FCT.CNTRCT_PBP_PTAPTB_EFCTV_DT =
                                                CNTRCT_PBP_NUM2.CNTRCT_PBP_SK_EFCTV_DT
                       AND '{SDATE}' BETWEEN CNTRCT_PBP_NUM2.CNTRCT_PBP_SK_EFCTV_DT
                              AND CNTRCT_PBP_NUM2.CNTRCT_PBP_SK_OBSLT_DT


                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_CNTRCT_{ENVNAME}"."CNTRCT_PBP_CNTRCT" CNTRCT_PBP_CNTRCT
                    ON  CNTRCT_PBP_NUM2.CNTRCT_NUM = CNTRCT_PBP_CNTRCT.CNTRCT_NUM
                    AND '{SDATE}' BETWEEN CNTRCT_PBP_CNTRCT.CNTRCT_PBP_BGN_DT
                    AND CNTRCT_PBP_CNTRCT.CNTRCT_END_DT

                    LEFT OUTER  JOIN "IDRC_{ENVNAME}"."CMS_DIM_CNTRCT_{ENVNAME}"."CNTRCT_PBP_CNTRCT" CNTRCT_PBP_CNTRCT2
                      ON  CNTRCT_PBP_NUM2.CNTRCT_NUM = CNTRCT_PBP_CNTRCT2.CNTRCT_NUM
                    AND '{SDATE}' BETWEEN CNTRCT_PBP_CNTRCT2.CNTRCT_PBP_BGN_DT
                       AND CNTRCT_PBP_CNTRCT2.CNTRCT_END_DT

                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE_ESRD_CVRG" BENE_ESRD_CVRG 
                        ON BENE_ESRD_CVRG.BENE_SK = BENE.BENE_SK 
                        AND '{SDATE}' BETWEEN BENE_ESRD_CVRG.BENE_RNG_BGN_DT AND BENE_ESRD_CVRG.BENE_RNG_END_DT

                    LEFT OUTER JOIN "IDRC_{ENVNAME}"."CMS_DIM_BENE_{ENVNAME}"."BENE_HOSPC" BENE_HOSPC 
                        ON BENE_HOSPC.BENE_SK = BENE.BENE_SK 
                        AND '{SDATE}' BETWEEN BENE_HOSPC.BENE_RNG_BGN_DT AND BENE_HOSPC.BENE_RNG_END_DT

                    INNER JOIN "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."{HOS_FF_TABLE}" TEMP
                        ON BENE_FCT.CNTRCT_PTC_NUM = TEMP.CNTRCT_NUM

                    WHERE BENE.BENE_DEATH_DT IS NULL
                      AND BENE_FCT.IDR_TRANS_OBSLT_TS ='9999-12-31'
                      AND '{SDATE}' BETWEEN BENE_FCT.BENE_FCT_EFCTV_DT AND BENE_FCT.BENE_FCT_OBSLT_DT
                    --AND BENE_FCT.BENE_CAN_NUM IS NOT NULL
                    ORDER BY 1
                 ) FILE_FORMAT = (TYPE=CSV, FIELD_DELIMITER=NONE ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY=NONE)
                    max_file_size=5368709120""", con, exit_on_error=True)
    
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