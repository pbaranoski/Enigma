#!/usr/bin/env python
########################################################################################################
# Name:  blbtn_clm_ext.py
#
# Desc: Script to Extract clm data (IDR#BLB2)
#
# Created: Paul Baranoski  06/09/2022  
# Modified: 
#
# Paul Baranoski 11/03/2022 Removed call to send Success email with Extract filename. Will
#                           do this from script instead.
# Paul Baranoski 07/19/2023 Change extract file extension from .csv to .txt 
# N. Tinovsky    10/30/2024 Change length of fields: from 15 to 35: CLM_PRSBNG_PRVDR_GNRC_ID_NUM;
#			                from 8 to 12 ("." is removed from NUMBERS:
#                           CLM_LINE_INGRDNT_CST_AMT,CLM_LINE_SRVC_CST_AMT,CLM_LINE_SLS_TAX_AMT,
#			                CLM_LINE_GRS_BLW_THRSHLD_AMT,CLM_LINE_GRS_ABOVE_THRSHLD_AMT,CLM_LINE_LIS_AMT,
#			                CLM_LINE_PLRO_AMT,CLM_LINE_NCVRD_PD_AMT
# Paul Baranoski 01/21/2025 Add {ENVNAME} back for all Database references. Remove filters added for Dev testing.
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
WKLY_STRT_DT=os.getenv('wkly_strt_dt')
WKLY_END_DT=os.getenv('wkly_end_dt')

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
   #   Extract Part D claim data  
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_BLBTN_STG/blbtn_clm_ext_{TMSTMP}.txt.gz
                                                FROM (

            WITH BLBTN_DTL_INFO as (

            SELECT
              RPAD(C.CLM_CNTL_NUM,40,' ')       AS  CLM_CNTL_NUM
            , RPAD(C.CLM_HIC_NUM,20,' ')        AS  CLM_HIC_NUM
            , RPAD(C.CLM_CARDHLDR_ID,20,' ')    AS  CLM_CARDHLDR_ID
            ,(CASE WHEN C.CLM_PTNT_BIRTH_DT IS NULL        THEN '00000000'
                   WHEN C.CLM_PTNT_BIRTH_DT = '0001-01-01' THEN '00000000'
                   WHEN C.CLM_PTNT_BIRTH_DT = '1000-01-01' THEN '00000000'
                   ELSE to_char(C.CLM_PTNT_BIRTH_DT,'YYYYMMDD') 
               END)            AS  CLM_PTNT_BIRTH_DT

            , RPAD(C.BENE_SEX_CD,1,' ')         AS  CLM_PTNT_SEX_CD

            ,(CASE WHEN CL.CLM_LINE_FROM_DT IS NULL         THEN '00000000'
                   WHEN CL.CLM_LINE_FROM_DT = '0001-01-01'  THEN '00000000'
                   WHEN CL.CLM_LINE_FROM_DT = '1000-01-01'  THEN '00000000'
                   ELSE to_char(CL.CLM_LINE_FROM_DT,'YYYYMMDD')
               END )                   AS  CLM_LINE_SRVC_DT
               
            ,(CASE WHEN C.CLM_PD_DT         IS NULL         THEN '00000000'
                   WHEN C.CLM_PD_DT        = '0001-01-01'   THEN '00000000'
                   WHEN C.CLM_PD_DT        = '1000-01-01'   THEN '00000000'
                   ELSE to_char(C.CLM_PD_DT,'YYYYMMDD')
               END )                   AS  CLM_SCHLD_PMT_DT

            -- Add leading zeroes for total of 11 bytes with last byte being space.   
            --,SUBSTRING('000000000000' FROM CHAR_LENGTH(RTRIM(CL.CLM_LINE_RX_NUM))+1)
            --  || RPAD(CL.CLM_LINE_RX_NUM),12,' ')AS CLM_LINE_RX_NUM
            ,to_char(to_number(cl.clm_line_rx_num,12,0),'FM000000000000') as CLM_LINE_RX_NUM

            ,RPAD(CL.CLM_LINE_NDC_CD,19,' ')                 AS  PROD_NDC_CODE
            ,RPAD(C.PRVDR_SRVC_ID_QLFYR_CD,2,' ')            AS  PRTY_GNRC_ID_QLFYR
            ,RPAD(TRIM(C.CLM_SRVC_PRVDR_GNRC_ID_NUM),15,' ') AS  PRTY_SRVC_PRVDR_ID

            -- is this field in MF file '01' or ' 1' or '1 '
            ,to_char(CLR.CLM_LINE_RX_FILL_NUM,'FM00')          AS  CLM_LINE_RX_FILL_NUM
            --,cast(CLR.CLM_LINE_RX_FILL_NUM as Decimal(2,0))   AS  CLM_LINE_RX_FILL_NUM2  

            ,RPAD(CLR.CLM_DSPNSNG_STUS_CD,1,' ')             AS  CLM_DSPNSNG_STUS_CD
            ,RPAD(CLR.CLM_CMPND_CD,1,' ')                    AS  CLM_CMPND_CD
            ,RPAD(CLR.CLM_DAW_PROD_SLCTN_CD,1,' ')           AS  CLM_DAW_PROD_SLCTN_CD
            ,replace(to_char(CL.CLM_LINE_NDC_QTY,'FM0000000.000'),'.','') 
                                                             AS  CLM_LINE_NDC_QTY

            ,to_char(CLR.CLM_LINE_DAYS_SUPLY_QTY,'FM000')      AS  CLM_LINE_DAYS_SUPLY_QTY
            ,RPAD(C.PRVDR_PRSBNG_ID_QLFYR_CD,2,' ')          AS  PRSCRB_ID_QLFYR

            --****\/****
            --OLD: X(20) X(20) 				NEW: X(35) X(35)
            --,RPAD(TRIM(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM),20,' ')
            --												 AS  PRTY_PRVDR_PHYSN_PRSCRB_ID		
              ,RPAD(TRIM(C.CLM_PRSBNG_PRVDR_GNRC_ID_NUM),35,' ')
                                                             AS  PRTY_PRVDR_PHYSN_PRSCRB_ID	
            --****/\****

            ,RPAD(TRIM(CLR.CLM_DRUG_CVRG_STUS_CD),1,' ')     AS CLM_DRUG_CVRG_STUS_CD

            , (CASE WHEN C.CLM_TYPE_CD = 2 THEN 'A'
                    WHEN C.CLM_TYPE_CD = 3 THEN 'D'
                         ELSE ' ' 
               END)                                          AS  CLM_ADJSMT_DEL_CD  

            ,RPAD(C.CLM_SBMT_FRMT_CD,1,' ')                  AS  CLM_SBMT_FRMT_CD
            ,RPAD(CLR.CLM_PRCNG_EXCPTN_CD,1,' ')             AS  CLM_PRCNG_EXCPTN_CD
            ,RPAD(CLR.CLM_CTSTRPHC_CVRG_IND_CD,1,' ')        AS  CLM_CTSTRPHC_CVRG_IND_CD

            --****\/****			
            --OLD: NUMBER(9,2) 9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
            --,replace(to_char(CLR.CLM_LINE_INGRDNT_CST_AMT,'FM0000000.00'),'.','')
            --												 AS  CLM_LINE_INGRDNT_CST_AMT
              ,replace(to_char(CLR.CLM_LINE_INGRDNT_CST_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_INGRDNT_CST_AMT
            --****/\****

            --****\/****			
            --OLD: NUMBER(9,2) 9(7)v99 	NEW: NUMBER(11,2) S9(9)v99
            --,replace(to_char(CLR.CLM_LINE_SRVC_CST_AMT,'FM0000000.00'),'.','')
            --												 AS  CLM_LINE_SRVC_CST_AMT
              ,replace(to_char(CLR.CLM_LINE_SRVC_CST_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_SRVC_CST_AMT												 
            --****/\****
                                                                                     
            --****\/****			
            --OLD: NUMBER(9,2) 9(7)v99 	NEW: NUMBER(11,2) S9(9)v99														 
            --,replace(to_char(CLR.CLM_LINE_SLS_TAX_AMT,'FM0000000.00'),'.','')
            --												 AS  CLM_LINE_SLS_TAX_AMT
              ,replace(to_char(CLR.CLM_LINE_SLS_TAX_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_SLS_TAX_AMT
            --****\/****			
            --OLD: NUMBER(8,2) 9(6)v99 	NEW: NUMBER(11,2) S9(9)v99				 
            --,replace(to_char(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,'FM000000.00'),'.','')
            --												 AS  CLM_LINE_GRS_BLW_THRSHLD_AMT
              ,replace(to_char(CLR.CLM_LINE_GRS_BLW_THRSHLD_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_GRS_BLW_THRSHLD_AMT
            --****/\****

            --****\/****			
            --OLD: NUMBER(8,2) 9(6)v99 	NEW: NUMBER(11,2) S9(9)v99														 
            --,replace(to_char(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,'FM000000.00'),'.','') 
            --												 AS  CLM_LINE_GRS_ABOVE_THRSHLD_AMT
              ,replace(to_char(CLR.CLM_LINE_GRS_ABOVE_THRSHLD_AMT,'MI000000000.00'),'.','') 
                                                             AS  CLM_LINE_GRS_ABOVE_THRSHLD_AMT
            --****/\****
                                                             
            ,replace(to_char(CL.CLM_LINE_BENE_PMT_AMT,'FM000000.00'),'.','')
                                                             AS  CLM_LINE_BENE_PMT_AMT
            ,replace(to_char(CL.CLM_LINE_OTHR_TP_PD_AMT,'FM000000.00'),'.','')
                                                             AS  CLM_LINE_OTHR_TP_PD_AMT
                                                             
            --****\/****			
            --OLD: NUMBER(9,2) 9(6)v99 	NEW: NUMBER(11,2) S9(9)v99														 
            --,replace(to_char(CLR.CLM_LINE_LIS_AMT,'FM0000000.00'),'.','')
            --												 AS  CLM_LINE_LIS_AMT
              ,replace(to_char(CLR.CLM_LINE_LIS_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_LIS_AMT														 
            --****/\****

            --****\/****			
            --OLD: NUMBER(8,2) 9(6)v99 	NEW: NUMBER(11,2) S9(9)v99														 
            --,replace(to_char(ABS(CLR.CLM_LINE_PLRO_AMT),'FM0000000.00'),'.','')
            --												 AS  CLM_LINE_PLRO_AMT
              ,replace(to_char(CLR.CLM_LINE_PLRO_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_PLRO_AMT	
            --****/\****														 

            ,replace(to_char(CL.CLM_LINE_CVRD_PD_AMT,'FM000000.00'),'.','')
                                                             AS  CLM_LINE_CVRD_PD_AMT
                                                             
            --****\/****			
            --OLD: NUMBER(8,2) 9(6)v99 	NEW: NUMBER(11,2) S9(9)v99														 
            --,replace(to_char(ABS(CL.CLM_LINE_NCVRD_PD_AMT),'FM0000000.00'),'.','')
            --												 AS  CLM_LINE_NCVRD_PD_AMT
              ,replace(to_char(CL.CLM_LINE_NCVRD_PD_AMT,'MI000000000.00'),'.','')
                                                             AS  CLM_LINE_NCVRD_PD_AMT
            --****/\****														 
                                                             
            ,RPAD(C.CLM_SBMTR_CNTRCT_NUM,5,' ')       AS  CNTRCT_NUM
            ,RPAD(C.CLM_SBMTR_CNTRCT_PBP_NUM,3,' ')   AS  CNTRCT_PBP_NUM
            ,to_char(CLR.META_PKG_SK,'FM000000000')   AS  PKG_ID
            ,RPAD(B.BENE_MBI_ID,11,' ')               AS  BENE_MBI_ID
            ,Repeat(' ',91)                           AS  FILLER_FLD


            FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM C

            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DT_SGNTR CDS
               ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK
              AND CDS.META_SRC_SK = 1
              
            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE CL
               ON C.CLM_NUM_SK = CL.CLM_NUM_SK
              AND C.CLM_TYPE_CD = CL.CLM_TYPE_CD
              AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
              AND C.GEO_BENE_SK = CL.GEO_BENE_SK
              AND C.CLM_FROM_DT = CL.CLM_FROM_DT

            INNER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_LINE_RX CLR
               ON CL.CLM_NUM_SK = CLR.CLM_NUM_SK
              AND CL.CLM_TYPE_CD = CLR.CLM_TYPE_CD
              AND CL.CLM_DT_SGNTR_SK = CLR.CLM_DT_SGNTR_SK
              AND CL.GEO_BENE_SK = CLR.GEO_BENE_SK
              AND CL.CLM_LINE_NUM = CLR.CLM_LINE_NUM

            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_CNTRCT_{ENVNAME}.CNTRCT_PBP_NUM CPN
              ON  C.CLM_SBMTR_CNTRCT_NUM      =  CPN.CNTRCT_NUM
              AND C.CLM_SBMTR_CNTRCT_PBP_NUM  =  CPN.CNTRCT_PBP_NUM
              AND CPN.CNTRCT_PBP_EFCTV_CD     =  '1'
              AND C.CLM_FROM_DT BETWEEN CPN.CNTRCT_PBP_BGN_DT 
                                    AND CPN.CNTRCT_PBP_END_DT

            INNER JOIN IDRC_{ENVNAME}.CMS_DIM_BENE_{ENVNAME}.BENE B
               ON C.BENE_SK = B.BENE_SK
               
            WHERE c.CLM_TYPE_CD IN (1,2,4)
              AND CDS.META_SRC_SK = 1
              AND C.META_SRC_SK = 1
            /*   These dates are passed in as parameters  */
            AND CDS.CLM_EDW_PROC_DT BETWEEN '{WKLY_STRT_DT}' AND '{WKLY_END_DT}'  
            )

            ,BLBTN_DTL_SINGLE_COL as (

                  -- If any column is null, the entire string will be NULL --> must fix main query	
                  SELECT '1' as SEQ_NUM
                          ,CLM_CNTL_NUM
                        ||  CLM_HIC_NUM
                        ||	CLM_CARDHLDR_ID
                        ||	CLM_PTNT_BIRTH_DT
                        ||	CLM_PTNT_SEX_CD
                        ||	CLM_LINE_SRVC_DT
                        ||	CLM_SCHLD_PMT_DT
                        ||	CLM_LINE_RX_NUM
                        ||	PROD_NDC_CODE
                        ||	PRTY_GNRC_ID_QLFYR
                        ||	PRTY_SRVC_PRVDR_ID
                        ||	CLM_LINE_RX_FILL_NUM
                        ||	CLM_DSPNSNG_STUS_CD
                        ||	CLM_CMPND_CD
                        ||	CLM_DAW_PROD_SLCTN_CD
                        ||	CLM_LINE_NDC_QTY
                        ||	CLM_LINE_DAYS_SUPLY_QTY
                        ||	PRSCRB_ID_QLFYR
                        ||	PRTY_PRVDR_PHYSN_PRSCRB_ID
                        ||	CLM_DRUG_CVRG_STUS_CD
                        ||	CLM_ADJSMT_DEL_CD  
                        ||	CLM_SBMT_FRMT_CD
                        ||	CLM_PRCNG_EXCPTN_CD
                        ||	CLM_CTSTRPHC_CVRG_IND_CD
                        ||	CLM_LINE_INGRDNT_CST_AMT
                        ||	CLM_LINE_SRVC_CST_AMT
                        ||	CLM_LINE_SLS_TAX_AMT
                        ||	CLM_LINE_GRS_BLW_THRSHLD_AMT
                        ||	CLM_LINE_GRS_ABOVE_THRSHLD_AMT
                        ||	CLM_LINE_BENE_PMT_AMT
                        ||	CLM_LINE_OTHR_TP_PD_AMT
                        ||	CLM_LINE_LIS_AMT
                        ||	CLM_LINE_PLRO_AMT
                        ||	CLM_LINE_CVRD_PD_AMT
                        ||	CLM_LINE_NCVRD_PD_AMT
                        ||	CNTRCT_NUM
                        ||	CNTRCT_PBP_NUM
                        ||	PKG_ID
                        ||	BENE_MBI_ID
                        ||	FILLER_FLD

                      AS DTL_ROW
                    FROM BLBTN_DTL_INFO
                    
            )


            ,HEADER_ROW as (

                SELECT '0' as SEQ_NUM 
                      ,'HPDE'|| to_char(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS') || 'M' || to_char(CURRENT_DATE,'YYYYMM') ||'01000001' 
                      || to_char(last_day(CURRENT_DATE),'YYYYMMDD') || '235959'

            --****\/****			
            --OLD:353  NEW: 405
            --		  || repeat(' ',353) as DTL_ROW
                      || repeat(' ',405) as DTL_ROW
            --****/\****														 
                  FROM DUAL

            )

            ,NOF_DTL_ROWS as (
                
                SELECT COUNT(*) as TOT_RECS
                FROM BLBTN_DTL_INFO 
            )

            ,TRAILER_ROW as (

                  SELECT '2' as SEQ_NUM 
                        ,'TPDE' || to_char(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS') 
                                || to_char(TOT_RECS,'FM0000000000')
                                
            --****\/****			
            --OLD:372  NEW: 424
                    --  	    || repeat(' ',372)
                                || repeat(' ',424)
            --****/\****	

                        as DTL_ROW
                  FROM NOF_DTL_ROWS

            )

            SELECT DTL_ROW
            FROM (
                SELECT *
                FROM HEADER_ROW
                UNION ALL
              
                SELECT *
                FROM BLBTN_DTL_SINGLE_COL
                UNION ALL
              
                SELECT *
                FROM TRAILER_ROW

            )  
            ORDER BY SEQ_NUM


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
