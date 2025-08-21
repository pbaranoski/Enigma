#!/usr/bin/env python
########################################################################################################
# Name:  ASC_PTB_Extract.py
#
# Desc: ASC (Ambulatory Surgical Center PTB extract. Designed to run in Annually in Apr
#
# Created: Paul Baranoski  01/23/2022
# Modified: 
# Modified:
#
# Paul Baranoski 2023-01-23 Created script.
# Paul Baranoski 2023-07-26 Modify extract file extension from .csv to .txt
# Joshua Turner  2023-08-14 Modified the extract output name to support EFT updates
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

CLM_EFCT_DT_BEG=os.getenv('CLM_EFCT_DT_BEG')
CLM_EFCT_DT_END=os.getenv('CLM_EFCT_DT_END')
CLM_LINE_FROM_DT_YYYY=os.getenv('CLM_LINE_FROM_DT_YYYY')
CURR_YYYY=os.getenv('CURR_YYYY')
PRIOR_YYYY=os.getenv('PRIOR_YYYY')
CURR_YY=CURR_YYYY[2:]

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

   snowconvert_helpers.execute_sql_statement("""USE DATABASE IDRC_${ENVNAME}""", con,exit_on_error = True)
   
   #**************************************
   #   Extract Drug Provider data  
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_ASCPTB_STG/ASCPS_PTB_Y{PRIOR_YYYY}_MAR{CURR_YY}_{TMSTMP}.txt.gz
                                                FROM (                                                           

                                    SELECT
                                         RPAD(C.CLM_CNTRCTR_NUM,5,' ')     AS CARRIER
                                        ,RPAD(CL.CLM_LINE_HCPCS_CD,5,' ')  AS HCPCS
                                        ,RPAD(CL.HCPCS_1_MDFR_CD,2,' ')    AS HCPS_INIT_MOD_CD
                                        ,RPAD(CL.HCPCS_2_MDFR_CD,2,' ')    AS HCPS_2ND_MOD_CD
                                        ,RPAD(
                                          ( CASE WHEN C.GEO_BENE_SK < 1 
                                            THEN '00000' 
                                            ELSE H.GEO_ZIP5_CD
                                            END ) || G.CLM_CWF_PLUS_4_ZIP_CD,9,' ')      AS BENE_ZIP_CD
                                            
                                        ,RPAD(COALESCE(K.GEO_SSA_STATE_CD,'  '),2,' ')   AS SSA_STATE
                                        ,RPAD(COALESCE(J.GEO_SSA_CNTY_CD,'   '),3,' ')   AS SSA_CNTY


                                    ,RPAD(CASE WHEN CL.GEO_RNDRG_SK > 0 
                                               THEN SUBSTR(RPAD(100000 + CL.GEO_RNDRG_SK,6,' '),2,5) ||
                                                     (CASE WHEN CL.GEO_RNDRG_ZIP4_CD = '~' 
                                                           THEN '0000'
                                                           ELSE CL.GEO_RNDRG_ZIP4_CD END )
                                               ELSE '000000000'
                                               END,9,' ')                      AS CARR_PROV_ZIP
                                               
                                    ,RPAD(CL.CLM_RNDRG_PRVDR_NPI_NUM,10,' ')   AS CARR_NPI
                                    ,RPAD(CL.CLM_RNDRG_PRVDR_UPIN_NUM,10,' ')  AS CARR_UPIN
                                    ,RPAD(CLP.CLM_FED_TYPE_SRVC_CD,1,' ')      AS LINE_TYPE_OF_SRVC

                                    ,to_char(SUM(CL.CLM_LINE_ALOWD_CHRG_AMT),'000000000000000000.00MI')  AS TOT_ALWD
                                    ,to_char(SUM(CL.CLM_LINE_CVRD_PD_AMT),'000000000000000000.00MI')     AS TOT_NCH_PAID
                                    ,to_char(SUM(CLP.CLM_LINE_PRFNL_MTUS_CNT),'FM00000000')             AS TOT_CAR_MTUS_CNT

                                    FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

                                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                                      ON  C.GEO_BENE_SK       = CL.GEO_BENE_SK
                                      AND C.CLM_DT_SGNTR_SK   = CL.CLM_DT_SGNTR_SK
                                      AND C.CLM_TYPE_CD       = CL.CLM_TYPE_CD
                                      AND C.CLM_NUM_SK        = CL.CLM_NUM_SK
                                      AND C.CLM_FROM_DT       = CL.CLM_FROM_DT

                                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN G
                                      ON  C.GEO_BENE_SK       = G.GEO_BENE_SK
                                      AND C.CLM_DT_SGNTR_SK   = G.CLM_DT_SGNTR_SK
                                      AND C.CLM_TYPE_CD       = G.CLM_TYPE_CD
                                      AND C.CLM_NUM_SK        = G.CLM_NUM_SK

                                    INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_PRFNL CLP
                                      ON  C.GEO_BENE_SK       = CLP.GEO_BENE_SK
                                      AND C.CLM_DT_SGNTR_SK   = CLP.CLM_DT_SGNTR_SK
                                      AND C.CLM_TYPE_CD       = CLP.CLM_TYPE_CD
                                      AND C.CLM_NUM_SK        = CLP.CLM_NUM_SK
                                      AND CL.CLM_LINE_NUM     = CLP.CLM_LINE_NUM

                                    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_ZIP5_CD H
                                      ON  C.GEO_BENE_SK       = H.GEO_SK

                                    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_FIPS_CNTY_CD J
                                      ON  H.GEO_FIPS_CNTY_CD  = J.GEO_FIPS_CNTY_CD
                                      AND H.GEO_FIPS_STATE_CD = J.GEO_FIPS_STATE_CD

                                    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_FIPS_STATE_CD K
                                      ON  H.GEO_FIPS_STATE_CD = K.GEO_FIPS_STATE_CD

                                    WHERE C.CLM_TYPE_CD IN (71, 72)
                                      AND CL.CLM_LINE_FINL_ACTN_IND IN ('Y')
                                      AND CL.CLM_LINE_ALOWD_CHRG_AMT > 0
                                      AND CLP.CLM_FED_TYPE_SRVC_CD = 'F'
                                      AND to_char(CL.CLM_LINE_FROM_DT,'YYYY') = '{CLM_LINE_FROM_DT_YYYY}'
                                      AND C.CLM_EFCTV_DT BETWEEN to_date('{CLM_EFCT_DT_BEG}','YYYYMMDD') and to_date('{CLM_EFCT_DT_END}','YYYYMMDD')

                                    GROUP BY
                                            CARRIER,
                                            HCPCS,
                                            HCPS_INIT_MOD_CD,
                                            HCPS_2ND_MOD_CD,
                                            BENE_ZIP_CD,
                                            SSA_STATE,
                                            SSA_CNTY,
                                            CARR_PROV_ZIP,
                                            CARR_NPI,
                                            CARR_UPIN,
                                            LINE_TYPE_OF_SRVC

                                    ORDER BY
                                            CARRIER,
                                            HCPCS,
                                            HCPS_INIT_MOD_CD,
                                            HCPS_2ND_MOD_CD,
                                            BENE_ZIP_CD,
                                            SSA_STATE,
                                            SSA_CNTY,
                                            CARR_NPI,
                                            CARR_UPIN,
                                            CARR_PROV_ZIP,
                                            LINE_TYPE_OF_SRVC


                        ) 
                        FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE  FIELD_OPTIONALLY_ENCLOSED_BY=none )
                        SINGLE=TRUE  max_file_size=5368709120  """, con, exit_on_error=True)


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
