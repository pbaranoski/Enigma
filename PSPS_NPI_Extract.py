#!/usr/bin/env python
########################################################################################################
# Name:  psps_npi_ext.py
#
# Desc: Script to Extract pbar psps with npi data
# On-Prem Version: 
# Mainframe PARMs: PSPSBTEQ,PSPSFEXP,PSNPISAS
# Date of Implementation: 01/20/2013
# Cloud Conversion scripts
# Created: Sumathi Gayam  12/15/2022
# Modified:
# 
# Paul Baranoski 01/22/2024 Add extract YYYY variable to add to extract filename.
# Paul Baranoski 05/03/2024 Add indenting of SQL for readability.
# Paul Baranoski 07/23/2024 Change extract filename from PSPS_NPI to PSPSNPI to distinguish it
#                           from the HCPCS category files (25 of them which are named PSPS_NPI).
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
ext_mon=os.getenv('ext_mon')
ENVNAME=os.getenv('ENVNAME')

ext_YYYY=os.getenv('ext_YYYY')

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
   snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}  """, con,exit_on_error = True)
   
   #**************************************
   #   Extract PSPS NPI data  Current Naming: P#IDR.XTR.PBAR.PSPS.NPI.JAN.Y2023.P20
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PSPSNPI_STG/PBAR_PSPSNPI_{ext_YYYY}_{ext_mon}_{TMSTMP}.txt.gz
                                                FROM (


WITH CLM_PTB_AGG_TMP as (

        SELECT
              A.CLM_CNTRCTR_NUM 
            , A.HCPCS_CD
            , A.CLNDR_HCPCS_YR_NUM
            , A.HCPCS_1_MDFR_CD
            , A.HCPCS_2_MDFR_CD
            , A.Pricing_Locality
            , A.Provider_Specialty
            , A.Type_of_Service
            , A.Place_of_Service
            , SUM(CASE WHEN (ZEROIFNULL(A.CLM_LINE_ALOWD_CHRG_AMT) > 0)
                         THEN CASE WHEN ((COALESCE(A.HCPCS_1_MDFR_CD,'') = '55') OR
                                         (COALESCE(A.HCPCS_2_MDFR_CD,'') = '55')
                                         )
                                   THEN 1
                                   ELSE ZEROIFNULL(A.CLM_LINE_SRVC_UNIT_QTY)
                               END
                          ELSE 0
                     END
                    )                            CLM_LINE_ALOWD_SRVC_UNIT_QTY
            , SUM(CASE WHEN (((COALESCE(A.HCPCS_1_MDFR_CD,'') = '55') OR
                                (COALESCE(A.HCPCS_2_MDFR_CD,'') = '55')
                               ) AND
                               (COALESCE(A.CLM_MDCR_PRFNL_PRVDR_ASGNMT_SW,'') = 'A')
                              )
                         THEN 1
                         WHEN (((COALESCE(A.HCPCS_1_MDFR_CD,'') = '55') OR
                                (COALESCE(A.HCPCS_2_MDFR_CD,'') = '55')
                               ) AND
                               (COALESCE(A.CLM_MDCR_PRFNL_PRVDR_ASGNMT_SW,'') = 'N')
                              )
                         THEN 0
                         WHEN (((COALESCE(A.HCPCS_1_MDFR_CD,'') <> '55') OR
                                (COALESCE(A.HCPCS_2_MDFR_CD,'') <> '55')
                               ) AND
                               (COALESCE(A.CLM_MDCR_PRFNL_PRVDR_ASGNMT_SW,'') = 'A')
              )
                          THEN ZEROIFNULL(A.CLM_LINE_SRVC_UNIT_QTY)
                          ELSE 0
                     END
                    )                               CLM_LINE_ASGND_SRVC_UNIT_QTY
             , SUM(ZEROIFNULL(A.CLM_LINE_ALOWD_CHRG_AMT)) CLM_LINE_ALOWD_CHRG_AMT
             , SUM(CASE WHEN (ZEROIFNULL(A.CLM_LINE_ALOWD_CHRG_AMT) = 0)
                          THEN ZEROIFNULL(A.CLM_LINE_SBMT_CHRG_AMT)
                          ELSE 0
                     END
                    )                               CLM_LINE_DND_AMT
            , SUM(CASE WHEN (ZEROIFNULL(A.CLM_LINE_ALOWD_CHRG_AMT) = 0)
                         THEN (CASE WHEN ((COALESCE(A.HCPCS_1_MDFR_CD,'') = '55') OR
                                          (COALESCE(A.HCPCS_2_MDFR_CD,'') = '55')
                                         )
                                    THEN 1
                                    ELSE ZEROIFNULL(A.CLM_LINE_SRVC_UNIT_QTY)
                               END
                              )
                         ELSE 0
                    END
                   )                               CLM_LINE_DND_SRVC_UNIT_QTY
            , SUM(ZEROIFNULL(A.CLM_LINE_CVRD_PD_AMT))  CLM_LINE_CVRD_PD_AMT
            , SUM(CASE WHEN ((COALESCE(A.HCPCS_1_MDFR_CD,'') = '55') OR
                               (COALESCE(A.HCPCS_2_MDFR_CD,'') = '55')
                              )
                         THEN 1
                         ELSE ZEROIFNULL(A.CLM_LINE_SRVC_UNIT_QTY)
                    END
                   ) CLM_LINE_SRVC_UNIT_QTY
            , A.CLM_LINE_INVLD_HCPCS_CD
            , A.REN_PRVDR_NPI_NUM, A.REF_PRVDR_NPI_NUM
        FROM
        (
                SELECT

                  C.CLM_TYPE_CD
                , C.CLM_CNTRCTR_NUM
                , (case when HCPCS.HCPCS_CD   IS NULL then '~' else
                       HCPCS.HCPCS_CD  end)     AS HCPCS_CD
                , (case when HCPCS.CLNDR_HCPCS_YR_NUM   IS NULL then 0 else
                      HCPCS.CLNDR_HCPCS_YR_NUM  end)     AS CLNDR_HCPCS_YR_NUM
                , COALESCE(CL.HCPCS_1_MDFR_CD,'&&')  AS HCPCS_1_MDFR_CD
                , COALESCE(CL.HCPCS_2_MDFR_CD,'&&')  AS HCPCS_2_MDFR_CD
                , CLP.CLM_PRCNG_LCLTY_CD  AS Pricing_Locality
                , CL.CLM_RNDRG_FED_PRVDR_SPCLTY_CD   AS Provider_Specialty
                , CLP.CLM_FED_TYPE_SRVC_CD     AS Type_of_Service
                , CL.CLM_POS_CD      AS Place_of_Service
                , CMP.CLM_MDCR_PRFNL_PRVDR_ASGNMT_SW
                , CL.CLM_LINE_CVRD_PD_AMT
                , CL.CLM_LINE_ALOWD_CHRG_AMT
                , CLP.CLM_BENE_PRMRY_PYR_PD_AMT
                , CL.CLM_LINE_PRVDR_PMT_AMT
                , CL.CLM_LINE_SBMT_CHRG_AMT
                , CL.CLM_LINE_SRVC_UNIT_QTY as CLM_LINE_SRVC_UNIT_QTY
                , (case when HCPCS.HCPCS_CD  IS NULL then CL.CLM_LINE_HCPCS_CD else '~' END) AS CLM_LINE_INVLD_HCPCS_CD
                , CL.CLM_RNDRG_PRVDR_NPI_NUM as REN_PRVDR_NPI_NUM
                , C.CLM_RFRG_PRVDR_NPI_NUM as REF_PRVDR_NPI_NUM
                 -- calculate the cutoff date, to check with CLM_NCH_WKLY_PROC_DT
                , EXTRACT(YEAR FROM CURRENT_DATE)  AS CURR_YEAR     ,
                 (CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 7 THEN (CURR_YEAR-1)||'-12-31'
                     WHEN EXTRACT(MONTH FROM CURRENT_DATE) > 6 THEN CURR_YEAR||'-06-30'  END)
                      as PSPS_CUTOFF_DT

                FROM   IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM  C

                INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
                 ON C.GEO_BENE_SK     = CL.GEO_BENE_SK
                 AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                 AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                 AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                 AND C.CLM_FROM_DT = CL.CLM_FROM_DT

                 INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_PRFNL CMP
                  ON  C.GEO_BENE_SK = CMP.GEO_BENE_SK
                  AND C.CLM_DT_SGNTR_SK = CMP.CLM_DT_SGNTR_SK
                  AND C.CLM_TYPE_CD = CMP.CLM_TYPE_CD
                  AND C.CLM_NUM_SK = CMP.CLM_NUM_SK

                 LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_HCPCS_{ENVNAME}.HCPCS_CD HCPCS
                   ON CL.CLM_LINE_HCPCS_CD = HCPCS.HCPCS_CD
                  AND EXTRACT(YEAR FROM CL.CLM_LINE_FROM_DT) = HCPCS.CLNDR_HCPCS_YR_NUM

                 INNER JOIN   IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_PRFNL CLP
                  ON CL.GEO_BENE_SK = CLP.GEO_BENE_SK
                  AND CL.CLM_DT_SGNTR_SK = CLP.CLM_DT_SGNTR_SK
                  AND CL.CLM_TYPE_CD = CLP.CLM_TYPE_CD
                  AND CL.CLM_NUM_SK = CLP.CLM_NUM_SK
                  AND CL.CLM_LINE_NUM = CLP.CLM_LINE_NUM

                 INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDT
                         ON  C.CLM_DT_SGNTR_SK = CDT.CLM_DT_SGNTR_SK

                 WHERE CL.CLM_LINE_FROM_DT between
                  (CURR_YEAR-1)||'-01-01'  and (CURR_YEAR-1)||'-12-31'
                 AND CL.CLM_TYPE_CD IN (71,72)
                 AND C.CLM_OBSLT_DT='9999-12-31' AND C.CLM_ERR_SGNTR_SK <> 3
                 AND CDT.CLM_NCH_WKLY_PROC_DT <= PSPS_CUTOFF_DT
                 ) A
        GROUP BY
          A.CLM_CNTRCTR_NUM
        , A.HCPCS_CD
        , A.CLNDR_HCPCS_YR_NUM
        , A.HCPCS_1_MDFR_CD
        , A.HCPCS_2_MDFR_CD
        , A.Pricing_Locality
        , A.Provider_Specialty
        , A.Type_of_Service
        , A.Place_of_Service
        , A.CLM_LINE_INVLD_HCPCS_CD
        , A.REN_PRVDR_NPI_NUM, A.REF_PRVDR_NPI_NUM),


        CLM_PTB_AGG_TMP_UPD as
                /*(
                UPDATE CMS_CLM_PTB_AGG_TMP
                SET CLM_LINE_INVLD_HCPCS_CD='UNK'
                WHERE CLM_LINE_ALOWD_CHRG_AMT=0
                  AND HCPCS_CD = '~'
                  AND CLNDR_HCPCS_YR_NUM = 0),
                  */
                (
                SELECT
                CLM_CNTRCTR_NUM
                , HCPCS_CD
                , CLNDR_HCPCS_YR_NUM
                , HCPCS_1_MDFR_CD
                , HCPCS_2_MDFR_CD
                , Pricing_Locality
                , Provider_Specialty
                , Type_of_Service
                , Place_of_Service
                , CLM_LINE_ALOWD_SRVC_UNIT_QTY
                , CLM_LINE_ASGND_SRVC_UNIT_QTY
                , CLM_LINE_ALOWD_CHRG_AMT
                , CLM_LINE_DND_AMT
                , CLM_LINE_DND_SRVC_UNIT_QTY
                , CLM_LINE_CVRD_PD_AMT
                , CLM_LINE_SRVC_UNIT_QTY

                ,case when (HCPCS_CD = '~' AND CLM_LINE_ALOWD_CHRG_AMT=0 AND CLNDR_HCPCS_YR_NUM = 0) 
                then 'UNK' else CLM_LINE_INVLD_HCPCS_CD END AS CLM_LINE_INVLD_HCPCS_CD

                --, CLM_LINE_INVLD_HCPCS_CD
                , REN_PRVDR_NPI_NUM,REF_PRVDR_NPI_NUM
                FROM  CLM_PTB_AGG_TMP
)  


,CLM_PTB_AGG_FINAL as (

        SELECT
        CLM_CNTRCTR_NUM
        , HCPCS_CD
        , CLNDR_HCPCS_YR_NUM
        , HCPCS_1_MDFR_CD
        , HCPCS_2_MDFR_CD
        , Pricing_Locality
        , Provider_Specialty
        , Type_of_Service
        , Place_of_Service
        , SUM(CLM_LINE_ALOWD_SRVC_UNIT_QTY) CLM_LINE_ALOWD_SRVC_UNIT_QTY
        , SUM(CLM_LINE_ASGND_SRVC_UNIT_QTY) CLM_LINE_ASGND_SRVC_UNIT_QTY
        , SUM(CLM_LINE_ALOWD_CHRG_AMT) CLM_LINE_ALOWD_CHRG_AMT
        , SUM(CLM_LINE_DND_AMT) CLM_LINE_DND_AMT
        , SUM(CLM_LINE_DND_SRVC_UNIT_QTY) CLM_LINE_DND_SRVC_UNIT_QTY
        , SUM(CLM_LINE_CVRD_PD_AMT) CLM_LINE_CVRD_PD_AMT
        , SUM(CLM_LINE_SRVC_UNIT_QTY) CLM_LINE_SRVC_UNIT_QTY
        , CLM_LINE_INVLD_HCPCS_CD
        , REN_PRVDR_NPI_NUM,REF_PRVDR_NPI_NUM
        FROM  CLM_PTB_AGG_TMP_UPD
        GROUP BY
          CLM_CNTRCTR_NUM
        , HCPCS_CD
        , CLNDR_HCPCS_YR_NUM
        , HCPCS_1_MDFR_CD
        , HCPCS_2_MDFR_CD
        , Pricing_Locality
        , Provider_Specialty
        , Type_of_Service
        , Place_of_Service
        , CLM_LINE_INVLD_HCPCS_CD
        , REN_PRVDR_NPI_NUM, REF_PRVDR_NPI_NUM
)



SELECT
        RPAD(CASE  WHEN A.CLM_LINE_INVLD_HCPCS_CD ='~' THEN HCPCS.HCPCS_CD
        ELSE A.CLM_LINE_INVLD_HCPCS_CD END,5,' ') AS HCPCS_CD,

        RPAD(A.HCPCS_1_MDFR_CD,2,' ') AS INIT_MOD,
        RPAD(A.PROVIDER_SPECIALTY,2,' ') AS SPECIALTY_CD,
        RPAD(A.CLM_CNTRCTR_NUM,5,' ') AS CLM_CNTRCTR_NUM,
        RPAD(A.PRICING_LOCALITY,2,' ') AS LOCALITY_CD,
        RPAD(A.TYPE_OF_SERVICE,1,' ') AS TOS,
        RPAD(A.PLACE_OF_SERVICE,2,' ') AS POS,
        RPAD(A.HCPCS_2_MDFR_CD,2,' ') AS SECOND_MOD,

        RPAD(TO_CHAR(SUM(A.CLM_LINE_SRVC_UNIT_QTY),'FM0000000000.000'),14,' ')
         AS CLM_LINE_SRVC_UNIT_QTY,
        RPAD(TO_CHAR(SUM(A.CLM_LINE_ALOWD_CHRG_AMT),'S000000000.00'),13,' ')
         AS CLM_LINE_ALOWD_CHRG_AMT,
        RPAD(TO_CHAR(SUM(A.CLM_LINE_DND_SRVC_UNIT_QTY), 'FM0000000000.000'),14,' ')
         AS CLM_LINE_DND_SRVC_UNIT_QTY,
        RPAD(TO_CHAR(SUM(A.CLM_LINE_DND_AMT), 'S000000000.00'),13,' ')
         AS CLM_LINE_DND_AMT,
        RPAD(TO_CHAR(SUM(A.CLM_LINE_ASGND_SRVC_UNIT_QTY), 'FM0000000000.000'),14,' ')
         AS CLM_LINE_ASGND_SRVC_UNIT_QTY,
        RPAD(TO_CHAR(SUM(A.CLM_LINE_CVRD_PD_AMT), 'S000000000.00'),13,' ')
         AS CLM_LINE_CVRD_PD_AMT,

        --RPAD(HCPCS.HCPCS_ASC_IND_CD,1,' ') AS HCPCS_ASC_IND_CD,
        RPAD(COALESCE(HCPCS.HCPCS_ASC_IND_CD,' '),1,' ') AS  HCPCS_ASC_IND_CD, 
        RPAD(HCPCS.HCPCS_BETOS_CD,3,' ') AS BETOS,
        RPAD(A.REN_PRVDR_NPI_NUM,10,' ') AS REN_PRVDR_NPI_NUM,
        RPAD(A.REF_PRVDR_NPI_NUM,10,' ') AS REF_PRVDR_NPI_NUM

 FROM  CLM_PTB_AGG_FINAL A
 LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_HCPCS_{ENVNAME}.HCPCS_CD HCPCS
   ON A.HCPCS_CD           = HCPCS.HCPCS_CD
  AND A.CLNDR_HCPCS_YR_NUM = HCPCS.CLNDR_HCPCS_YR_NUM

 GROUP BY
   A.CLM_CNTRCTR_NUM
 , A.PRICING_LOCALITY
 , A.PROVIDER_SPECIALTY
 , A.TYPE_OF_SERVICE
 , A.PLACE_OF_SERVICE
 , A.HCPCS_1_MDFR_CD
 , A.HCPCS_2_MDFR_CD
 , HCPCS.HCPCS_CD
 , HCPCS.HCPCS_BETOS_CD
 , HCPCS.HCPCS_ASC_IND_CD
 , A.CLM_LINE_INVLD_HCPCS_CD
 , A.REN_PRVDR_NPI_NUM, A.REF_PRVDR_NPI_NUM
 
 ) 
                        FILE_FORMAT = (TYPE=CSV field_delimiter=none ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY=none )
                        max_file_size=5368709120  """, con, exit_on_error=True)

   
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
