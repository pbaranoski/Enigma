#!/usr/bin/env python
########################################################################################################
# Name:  FMR_EXTRACT_INSERT.py
#
# Desc: Script to Extract FMR Data
# On-Prem Version: 
# Mainframe PARMs: IDRFMRDT,IDRFMRFE
# Date of Implementation: 01/20/2013
# Cloud Conversion scripts
# Created: Viren Khanna  01/17/2023
#
# Modified:
#
# Paul Baranoski 2025-05-19   Convert SQL to use CTE WITH statement for readability. Compared Old and 
#                             new SQL against production and received no differences.
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
FNAME=os.getenv('FNAME')
CUR_YR=os.getenv('CUR_YR')
PRIOR_INTRVL=os.getenv('PRIOR_INTRVL')
CURRENT_INTRVL=os.getenv('CURRENT_INTRVL')


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

   ####################################################################################################
   # DELETE ALL FROM CLM_FMRD
   ####################################################################################################
   snowconvert_helpers.execute_sql_statement(f"""DELETE FROM "BIA_{ENVNAME}"."CMS_TARGET_XTR_{ENVNAME}"."CLM_FMRD" """, con,exit_on_error = True)

   
   #**************************************
   #   Insert Data into CLM_FMRD
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""INSERT INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.CLM_FMRD

(                                                                            
 CLM_FMR_INTRVL_NUM                                                           
,PRIOR_INTRVL                                                                
,GEO_RGN_CD                                                                    
,CLM_CNTRCTR_NUM                                                               
,CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                                 
,HCPCS_CD                                                                      
,HCPCS_MDFR_CD                                                                 
,HCPCS_FMR_CLSFCTN_CD                                                          
,CARR_RANK                                                                     
,NATL_RANK                                                                     
,CARR_ENRLMT                                                                   
,NATL_ENRLMT                                                                   
,CARR_ALOWD_SRVCS                                                              
,NATL_ALOWD_SRVCS                                                              
,CARR_ALOWD_CHRGS                                                              
,NATL_ALOWD_CHRGS                                                              
,CARR_FREQ                                                                     
,NATL_FREQ                                                                     
,CARR_DND_SRVCS                                                                
,NATL_DND_SRVCS                                                                
,CARR_PRR_YR_ALOWD_CHRGS                                                       
,NATL_PRR_YR_ALOWD_CHRGS                                                       
) 


WITH CARR_AGG_CALC  AS (
--  Carrior Aggregate Calculations                                           
                                                                              
    SELECT                                                                         
        FA.CLM_FMR_INTRVL_NUM                                                         
        ,'{PRIOR_INTRVL}' AS PRIOR_INTRVL  
        ,FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                              
        ,FA.HCPCS_CD                                                                   
        ,FA.HCPCS_MDFR_CD                                                              
        ,FA.HCPCS_FMR_CLSFCTN_CD                                                       
        ,FA.CLM_CNTRCTR_NUM                                                            
        ,SUM(FA.CLM_LINE_ALOWD_UNIT_QTY)    AS CARR_ALOWD_SRVCS                           
        ,SUM(FA.CLM_LINE_ALOWD_CHRG_AMT)    AS CARR_ALOWD_CHRGS                           
        ,SUM(FA.CLM_LINE_DND_SRVC_UNIT_QTY) AS CARR_DND_SRVCS                           
        ,SUM(FA.CLM_LINE_ALOWD_UNIT_QTY) + SUM(FA.CLM_LINE_DND_SRVC_UNIT_QTY)  AS  CARR_FREQ                                                                 
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA    
    WHERE FA.CLM_FMR_INTRVL_NUM='{CURRENT_INTRVL}'                
    GROUP BY CLM_FMR_INTRVL_NUM, PRIOR_INTRVL, CLM_RNDRG_FED_PRVDR_SPCLTY_CD, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD, CLM_CNTRCTR_NUM

)

,CARR_RANKING  AS  (

    SELECT  FA.CLM_FMR_INTRVL_NUM,                                                 
            FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                             
            FA.HCPCS_CD,                                                                  
            FA.HCPCS_MDFR_CD,                                                             
            FA.HCPCS_FMR_CLSFCTN_CD,                                                      
            FA.CLM_CNTRCTR_NUM,                                                           
    RANK () OVER(PARTITION BY FA.CLM_FMR_INTRVL_NUM,                              
                              FA.CLM_CNTRCTR_NUM,                                 
                              FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                    
    						  
    ORDER BY SUM(FA.CLM_LINE_ALOWD_CHRG_AMT) DESC,                                  
                              FA.HCPCS_CD,                                          
                              FA.HCPCS_MDFR_CD,                                     
                              FA.HCPCS_FMR_CLSFCTN_CD)  AS CARR_RANK                                                              
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                         
    GROUP BY CLM_FMR_INTRVL_NUM, CLM_RNDRG_FED_PRVDR_SPCLTY_CD, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD, CLM_CNTRCTR_NUM  



)

,NAT_AGG_CALCULATIONS AS   (

    SELECT                                                                        
            FA.CLM_FMR_INTRVL_NUM,                                                        
            FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                             
            FA.HCPCS_CD,                                                                  
            FA.HCPCS_MDFR_CD,                                                             
            FA.HCPCS_FMR_CLSFCTN_CD,                                                      
            SUM(FA.CLM_LINE_ALOWD_UNIT_QTY)    AS NATL_ALOWD_SRVCS,                          
            SUM(FA.CLM_LINE_DND_SRVC_UNIT_QTY) AS NATL_DND_SRVCS,                        
            SUM(FA.CLM_LINE_ALOWD_CHRG_AMT)    AS NATL_ALOWD_CHRGS,                           
            SUM(FA.CLM_LINE_ALOWD_UNIT_QTY) + SUM(FA.CLM_LINE_DND_SRVC_UNIT_QTY)  AS NATL_FREQ 

     FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                       
    GROUP BY CLM_FMR_INTRVL_NUM, CLM_RNDRG_FED_PRVDR_SPCLTY_CD, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD                                                            

)

,NAT_RANK AS  (
                                                          
    SELECT                                                                          
        FA.CLM_FMR_INTRVL_NUM,                                                          
        FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                               
        FA.HCPCS_CD,                                                                    
        FA.HCPCS_MDFR_CD,                                                               
        FA.HCPCS_FMR_CLSFCTN_CD,                                                       
        RANK () OVER(PARTITION BY FA.CLM_FMR_INTRVL_NUM,                               
                                  FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                     
        ORDER BY SUM(FA.CLM_LINE_ALOWD_CHRG_AMT) DESC,                                 
                                  FA.HCPCS_CD,                                         
                                  FA.HCPCS_MDFR_CD,                                    
                                  FA.HCPCS_FMR_CLSFCTN_CD) AS NATL_RANK                                                                   
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                        
    GROUP BY CLM_FMR_INTRVL_NUM, CLM_RNDRG_FED_PRVDR_SPCLTY_CD, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD                                                    
)

,CARR_ENROLLMENT AS  (
                                                             
    SELECT CFEC.CLM_CNTRCTR_NUM,                                                          
           SUM(CFEC.CLM_CARR_ENRLMT_CNT) AS CARR_ENRLMT                                   
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_ENRLMT_CNT CFEC                               
    WHERE CFEC.CLM_FMR_INTRVL_NUM  =  '{CURRENT_INTRVL}' 
    GROUP BY CLM_CNTRCTR_NUM                                                                     

)

,NAT_ENROLLMENT AS  (

    SELECT SUM(CARR_ENRLMT) AS NATL_ENRLMT                                            
      FROM (
            SELECT CFEC.CLM_FMR_INTRVL_NUM,                                           
                   CFEC.GEO_SSA_STATE_CD,                                                         
                   CFEC.GEO_SSA_CNTY_CD,                                                          
                   MAX(CFEC.CLM_CARR_ENRLMT_CNT) AS CARR_ENRLMT                                   
              FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_ENRLMT_CNT CFEC                               
            WHERE CFEC.CLM_FMR_INTRVL_NUM = '{CURRENT_INTRVL}'                 
            GROUP BY CLM_FMR_INTRVL_NUM, GEO_SSA_STATE_CD, GEO_SSA_CNTY_CD                                                                  
           )         
            
)

,CARR_PY_ALWD_CHRGS AS (
                                                              
    --SELECT DISTINCT
    SELECT
        FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                              
        FA.HCPCS_CD,                                                                   
        FA.HCPCS_MDFR_CD,                                                              
        FA.HCPCS_FMR_CLSFCTN_CD,                                                       
        FA.CLM_CNTRCTR_NUM,                                                            
        --SUM(COALESCE(FA.CLM_LINE_ALOWD_CHRG_AMT,0))  AS CARR_PRR_YR_ALOWD_CHRGS 
        COALESCE(SUM(FA.CLM_LINE_ALOWD_CHRG_AMT),0)   AS CARR_PRR_YR_ALOWD_CHRGS   		
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                        
    WHERE FA.CLM_FMR_INTRVL_NUM  ='{PRIOR_INTRVL}'  
    
    GROUP BY CLM_RNDRG_FED_PRVDR_SPCLTY_CD, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD, CLM_CNTRCTR_NUM  
    
)

,NAT_PY_ALWD_CHRGS AS  (

    SELECT                                                                         
        FA.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                              
        FA.HCPCS_CD,                                                                   
        FA.HCPCS_MDFR_CD,                                                              
        FA.HCPCS_FMR_CLSFCTN_CD,                                                       
        --SUM(COALESCE(FA.CLM_LINE_ALOWD_CHRG_AMT,0))   AS NATL_PRR_YR_ALOWD_CHRGS
        COALESCE(SUM(FA.CLM_LINE_ALOWD_CHRG_AMT),0)   AS NATL_PRR_YR_ALOWD_CHRGS    		
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                        
    WHERE FA.CLM_FMR_INTRVL_NUM='{PRIOR_INTRVL}' 
    GROUP BY CLM_RNDRG_FED_PRVDR_SPCLTY_CD, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD                                                               

)

,REG_CARR  AS  (

	
    SELECT CLM_CNTRCTR_NUM,                                    
        MAX(CASE WHEN CLM_CNTRCTR_NUM = '00882' THEN 11                                
                 WHEN CLM_CNTRCTR_NUM = '66001' THEN 12                
                 WHEN CLM_CNTRCTR_NUM = '16003' THEN 13                
                 WHEN CLM_CNTRCTR_NUM = '17003' THEN 14                
                 WHEN CLM_CNTRCTR_NUM = '18003' THEN 15                
                 WHEN CLM_CNTRCTR_NUM = '19003' THEN 16                
                 ELSE GEO_RGN_CD END) AS GEO_RGN_CD  
                                 
    FROM BIA_{ENVNAME}.CMS_DIM_PTB_{ENVNAME}.CLM_FMR_CARR FC  
    
    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_SSA_STATE_CD GS                        
    ON FC.GEO_SSA_STATE_CD = GS.GEO_SSA_STATE_CD                                   
    
    LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_DIM_GEO_{ENVNAME}.GEO_RGN GR                                 
    ON GS.GEO_SSA_RGN_CD  = GR.GEO_RGN_CD                                           
    
    GROUP BY CLM_CNTRCTR_NUM  
        
)

,RESULTS_2_INSERT AS  (

    SELECT                                                                         
        CAGG.CLM_FMR_INTRVL_NUM,                                                       
        CAGG.PRIOR_INTRVL, 
        -- GEO_RGN_CD can be null
        RGN.GEO_RGN_CD,                                                                
        CAGG.CLM_CNTRCTR_NUM,                                                          
        CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                            
        CAGG.HCPCS_CD,                                                                 
        CAGG.HCPCS_MDFR_CD,                                                            
        CAGG.HCPCS_FMR_CLSFCTN_CD,                                                     
        MAX(CRANK.CARR_RANK) AS CARR_RANK,                                             
        MAX(NRANK.NATL_RANK) AS NATL_RANK,                                             
        MAX(CENRLMT.CARR_ENRLMT) AS CARR_ENRLMT,                                       
        MAX(NENRLMT.NATL_ENRLMT) AS NATL_ENRLMT,                                       
        MAX(CAGG.CARR_ALOWD_SRVCS) AS CARR_ALOWD_SRVCS,                                
        MAX(NAGG.NATL_ALOWD_SRVCS) AS NATL_ALOWD_SRVCS,                                
        MAX(CAGG.CARR_ALOWD_CHRGS) AS CARR_ALOWD_CHRGS,                                
        MAX(NAGG.NATL_ALOWD_CHRGS) AS NATL_ALOWD_CHRGS,                                
        MAX(CAGG.CARR_FREQ) AS CARR_FREQ,                                              
        MAX(NAGG.NATL_FREQ) AS NATL_FREQ,                                              
        MAX(CAGG.CARR_DND_SRVCS) AS CARR_DND_SRVCS,                                    
        MAX(NAGG.NATL_DND_SRVCS) AS NATL_DND_SRVCS,                                    
        MAX(CPYAC.CARR_PRR_YR_ALOWD_CHRGS) AS CARR_PRR_YR_ALOWD_CHRGS,                 
        MAX(NPYAC.NATL_PRR_YR_ALOWD_CHRGS) AS NATL_PRR_YR_ALOWD_CHRGS  
        --COALESCE(MAX(CPYAC.CARR_PRR_YR_ALOWD_CHRGS),0) AS CARR_PRR_YR_ALOWD_CHRGS,                 
        --COALESCE(MAX(NPYAC.NATL_PRR_YR_ALOWD_CHRGS),0) AS NATL_PRR_YR_ALOWD_CHRGS  
    
    FROM CARR_AGG_CALC CAGG                                                                          
    
    INNER JOIN CARR_RANKING CRANK
       ON CAGG.CLM_FMR_INTRVL_NUM            = CRANK.CLM_FMR_INTRVL_NUM                             
      AND CAGG.CLM_CNTRCTR_NUM               = CRANK.CLM_CNTRCTR_NUM                                
      AND CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD = CRANK.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                            
      AND CAGG.HCPCS_CD                      = CRANK.HCPCS_CD                                              
      AND TRIM(CAGG.HCPCS_MDFR_CD)           = TRIM(CRANK.HCPCS_MDFR_CD)                                    
      AND CAGG.HCPCS_FMR_CLSFCTN_CD          = CRANK.HCPCS_FMR_CLSFCTN_CD  
    
     INNER JOIN NAT_AGG_CALCULATIONS NAGG
       ON CAGG.CLM_FMR_INTRVL_NUM             = NAGG.CLM_FMR_INTRVL_NUM  
       AND CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD = NAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                            
       AND CAGG.HCPCS_CD                      = NAGG.HCPCS_CD                                                
       AND TRIM(CAGG.HCPCS_MDFR_CD)           = TRIM(NAGG.HCPCS_MDFR_CD)                                     
       AND CAGG.HCPCS_FMR_CLSFCTN_CD          = NAGG.HCPCS_FMR_CLSFCTN_CD                       
    
     INNER JOIN NAT_RANK NRANK
        ON CAGG.CLM_FMR_INTRVL_NUM            = NRANK.CLM_FMR_INTRVL_NUM                          
       AND CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD = NRANK.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                         
       AND CAGG.HCPCS_CD                      = NRANK.HCPCS_CD                                             
       AND TRIM(CAGG.HCPCS_MDFR_CD)           = TRIM(NRANK.HCPCS_MDFR_CD)                                 
       AND CAGG.HCPCS_FMR_CLSFCTN_CD          = NRANK.HCPCS_FMR_CLSFCTN_CD   
                                      
    LEFT OUTER JOIN CARR_ENROLLMENT CENRLMT                                                                             
      ON CAGG.CLM_CNTRCTR_NUM = CENRLMT.CLM_CNTRCTR_NUM                                                                         
                                                                       
    CROSS JOIN NAT_ENROLLMENT NENRLMT                               
    
    LEFT OUTER JOIN CARR_PY_ALWD_CHRGS CPYAC
      ON CAGG.CLM_CNTRCTR_NUM               = CPYAC.CLM_CNTRCTR_NUM                                
     AND CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD = CPYAC.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                          
     AND CAGG.HCPCS_CD                      = CPYAC.HCPCS_CD                                             
     AND TRIM(CAGG.HCPCS_MDFR_CD)           = TRIM(CPYAC.HCPCS_MDFR_CD)                                  
     AND CAGG.HCPCS_FMR_CLSFCTN_CD          = CPYAC.HCPCS_FMR_CLSFCTN_CD  
    
    LEFT OUTER JOIN NAT_PY_ALWD_CHRGS NPYAC 
      ON CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD  = NPYAC.CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                        
     AND CAGG.HCPCS_CD                       = NPYAC.HCPCS_CD                                           
     AND TRIM(CAGG.HCPCS_MDFR_CD)            = TRIM(NPYAC.HCPCS_MDFR_CD)
     AND CAGG.HCPCS_FMR_CLSFCTN_CD           = NPYAC.HCPCS_FMR_CLSFCTN_CD  	 
    
    INNER JOIN REG_CARR RGN 
       ON CAGG.CLM_CNTRCTR_NUM = RGN.CLM_CNTRCTR_NUM 
    
    
    GROUP BY CAGG.CLM_FMR_INTRVL_NUM,                                                       
             CAGG.PRIOR_INTRVL,                                                             
             RGN.GEO_RGN_CD,                                                                
             CAGG.CLM_CNTRCTR_NUM,                                                          
             CAGG.CLM_RNDRG_FED_PRVDR_SPCLTY_CD,                                            
             CAGG.HCPCS_CD,                                                                 
             CAGG.HCPCS_MDFR_CD,                                                            
             CAGG.HCPCS_FMR_CLSFCTN_CD 

)

--***************************************************************
--* MAIN SQL
--***************************************************************
SELECT *
FROM RESULTS_2_INSERT

 """,con,exit_on_error=True)


   #**************************************
   #   Insert Data into CLM_FMRD again
   #**************************************   
   snowconvert_helpers.execute_sql_statement(f"""INSERT INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.CLM_FMRD

(                                                                            
 CLM_FMR_INTRVL_NUM                                                           
,PRIOR_INTRVL                                                                
,GEO_RGN_CD                                                                    
,CLM_CNTRCTR_NUM                                                               
,CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                                 
,HCPCS_CD                                                                      
,HCPCS_MDFR_CD                                                                 
,HCPCS_FMR_CLSFCTN_CD                                                          
,CARR_RANK                                                                     
,NATL_RANK                                                                     
,CARR_ENRLMT                                                                   
,NATL_ENRLMT                                                                   
,CARR_ALOWD_SRVCS                                                              
,NATL_ALOWD_SRVCS                                                              
,CARR_ALOWD_CHRGS                                                              
,NATL_ALOWD_CHRGS                                                              
,CARR_FREQ                                                                     
,NATL_FREQ                                                                     
,CARR_DND_SRVCS                                                                
,NATL_DND_SRVCS                                                                
,CARR_PRR_YR_ALOWD_CHRGS                                                       
,NATL_PRR_YR_ALOWD_CHRGS                                                       
) 


WITH CARR_AGG_CALC  AS (

    --  Carrior Aggregate Calculations    
                                                                          
    SELECT                                                                       
         CLM_FMR_INTRVL_NUM                                                           
        ,PRIOR_INTRVL                                                                
        ,GEO_RGN_CD                                                                  
        ,CLM_CNTRCTR_NUM                                                             
        ,HCPCS_CD                                                                    
        ,HCPCS_MDFR_CD                                                               
        ,HCPCS_FMR_CLSFCTN_CD                                                        
        ,CARR_ENRLMT                                                                 
        ,NATL_ENRLMT                                                                 
        ,SUM(CARR_ALOWD_SRVCS) AS CARR_ALOWD_SRVCS                                   
        ,SUM(CARR_ALOWD_CHRGS) AS CARR_ALOWD_CHRGS                                   
        ,SUM(CARR_FREQ)        AS CARR_FREQ                                                 
        ,SUM(CARR_DND_SRVCS)   AS CARR_DND_SRVCS                                       
        ,SUM(CARR_PRR_YR_ALOWD_CHRGS) AS CARR_PRR_YR_ALOWD_CHRGS  
    FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.CLM_FMRD                                             
    GROUP BY CLM_FMR_INTRVL_NUM                                                           
            ,PRIOR_INTRVL                                                                
            ,GEO_RGN_CD                                                                  
            ,CLM_CNTRCTR_NUM                                                             
            ,HCPCS_CD                                                                    
            ,HCPCS_MDFR_CD                                                               
            ,HCPCS_FMR_CLSFCTN_CD                                                        
            ,CARR_ENRLMT                                                                 
            ,NATL_ENRLMT 

)

,CARR_RANKING  AS  (

    SELECT                                                                        
        FA.CLM_FMR_INTRVL_NUM,                                                        
        FA.CLM_CNTRCTR_NUM,
        FA.HCPCS_CD,                                                                  
        FA.HCPCS_MDFR_CD,                                                            
        FA.HCPCS_FMR_CLSFCTN_CD, 
        RANK () OVER(PARTITION BY FA.CLM_FMR_INTRVL_NUM,                              
                              FA.CLM_CNTRCTR_NUM                                  
    ORDER BY SUM(FA.CARR_ALOWD_CHRGS) DESC,                                       
                              FA.HCPCS_CD,                                        
    			              FA.HCPCS_MDFR_CD,                                    
                              FA.HCPCS_FMR_CLSFCTN_CD)                             
          AS CARR_RANK                                                                   
    FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.CLM_FMRD FA                                             
    GROUP BY CLM_FMR_INTRVL_NUM, CLM_CNTRCTR_NUM, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD    

)

,NAT_AGG_CALCULATIONS  AS  (

    SELECT                                                                         
        FA.CLM_FMR_INTRVL_NUM,                                                          
        FA.HCPCS_CD,                                                                    
        FA.HCPCS_MDFR_CD,                                                               
        FA.HCPCS_FMR_CLSFCTN_CD,                                                        
        SUM(FA.CLM_LINE_ALOWD_UNIT_QTY)  AS NATL_ALOWD_SRVCS,                           
        SUM(FA.CLM_LINE_ALOWD_CHRG_AMT)  AS NATL_ALOWD_CHRGS,                           
        SUM(FA.CLM_LINE_ALOWD_UNIT_QTY) + SUM(FA.CLM_LINE_DND_SRVC_UNIT_QTY)            
        AS NATL_FREQ,                                                                   
        SUM(FA.CLM_LINE_DND_SRVC_UNIT_QTY) AS NATL_DND_SRVCS                            
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                 
    GROUP BY CLM_FMR_INTRVL_NUM, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD  

)

,NAT_RANK  AS (

    /* National Rank */                                                           
                                                                                  
    SELECT                                                                       
        FA.CLM_FMR_INTRVL_NUM,                                                        
        FA.HCPCS_CD,                                                                  
        FA.HCPCS_MDFR_CD,                                                             
        FA.HCPCS_FMR_CLSFCTN_CD,                                                      
        RANK () OVER(PARTITION BY FA.CLM_FMR_INTRVL_NUM                               
        ORDER BY SUM(FA.CLM_LINE_ALOWD_CHRG_AMT) DESC,                                
                                FA.HCPCS_CD,                                          
                                FA.HCPCS_MDFR_CD,                                     
                                FA.HCPCS_FMR_CLSFCTN_CD)                              
        AS NATL_RANK                                                                  
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                       
    GROUP BY CLM_FMR_INTRVL_NUM, HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD

)

,NAT_PY_ALWD_CHRGS  AS  (

    /*  National Prior Year Allowed Charges */                                    
                                                           
    SELECT                                                                        
        FA.HCPCS_CD,                                                                  
        FA.HCPCS_MDFR_CD,                                                             
        FA.HCPCS_FMR_CLSFCTN_CD,                                                      
        COALESCE(SUM(FA.CLM_LINE_ALOWD_CHRG_AMT), 0)  AS NATL_PRR_YR_ALOWD_CHRGS                                                    
    FROM BIA_{ENVNAME}.CMS_AGG_PTB_{ENVNAME}.CLM_FMR_AGG FA                                       
    WHERE FA.CLM_FMR_INTRVL_NUM='{PRIOR_INTRVL}'   
    GROUP BY HCPCS_CD, HCPCS_MDFR_CD, HCPCS_FMR_CLSFCTN_CD 

)

,RESULTS_2_INSERT AS  (

	SELECT                                                                         
		CAGG.CLM_FMR_INTRVL_NUM                                                        
		,CAGG.PRIOR_INTRVL                                                             
		,CAGG.GEO_RGN_CD                                                               
		,CAGG.CLM_CNTRCTR_NUM                                                          
		,'AA' AS CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                         
		,CAGG.HCPCS_CD                                                                 
		,CAGG.HCPCS_MDFR_CD                                                            
		,CAGG.HCPCS_FMR_CLSFCTN_CD                                                     
		,CRANK.CARR_RANK                                                               
		,NRANK.NATL_RANK                                                               
		,CAGG.CARR_ENRLMT                                                              
		,CAGG.NATL_ENRLMT                                                              
		,CAGG.CARR_ALOWD_SRVCS                                                         
		,MAX(NAGG.NATL_ALOWD_SRVCS) AS NATL_ALOWD_SRVCS                                
		,CAGG.CARR_ALOWD_CHRGS                                                         
		,MAX(NAGG.NATL_ALOWD_CHRGS) AS NATL_ALOWD_CHRGS                                
		,CAGG.CARR_FREQ                                                                
		,MAX(NAGG.NATL_FREQ) AS NATL_FREQ                                              
		,CAGG.CARR_DND_SRVCS                                                           
		,MAX(NAGG.NATL_DND_SRVCS) AS NATL_DND_SRVCS                                    
		,CAGG.CARR_PRR_YR_ALOWD_CHRGS                                                  
		,MAX(NPYAC.NATL_PRR_YR_ALOWD_CHRGS) AS NATL_PRR_YR_ALOWD_CHRGS 
		
	FROM CARR_AGG_CALC CAGG                                                                          

	INNER JOIN CARR_RANKING CRANK
	   ON CAGG.CLM_FMR_INTRVL_NUM   = CRANK.CLM_FMR_INTRVL_NUM                            
	  AND CAGG.CLM_CNTRCTR_NUM      = CRANK.CLM_CNTRCTR_NUM                                 
	  AND CAGG.HCPCS_CD             = CRANK.HCPCS_CD                                             
	  AND TRIM(CAGG.HCPCS_MDFR_CD)  = TRIM(CRANK.HCPCS_MDFR_CD)                                  
	  AND CAGG.HCPCS_FMR_CLSFCTN_CD = CRANK.HCPCS_FMR_CLSFCTN_CD                       

	LEFT OUTER JOIN NAT_AGG_CALCULATIONS NAGG
	  ON CAGG.CLM_FMR_INTRVL_NUM   = NAGG.CLM_FMR_INTRVL_NUM                           
	 AND CAGG.HCPCS_CD             = NAGG.HCPCS_CD                                               
	 AND TRIM(CAGG.HCPCS_MDFR_CD)  = TRIM(NAGG.HCPCS_MDFR_CD)                                    
	 AND CAGG.HCPCS_FMR_CLSFCTN_CD = NAGG.HCPCS_FMR_CLSFCTN_CD                         

	INNER JOIN NAT_RANK NRANK
	   ON CAGG.CLM_FMR_INTRVL_NUM   = NRANK.CLM_FMR_INTRVL_NUM                           
	  AND CAGG.HCPCS_CD             = NRANK.HCPCS_CD                                            
	  AND TRIM(CAGG.HCPCS_MDFR_CD)  = TRIM(NRANK.HCPCS_MDFR_CD)                                  
	  AND CAGG.HCPCS_FMR_CLSFCTN_CD = NRANK.HCPCS_FMR_CLSFCTN_CD                      

	LEFT OUTER JOIN NAT_PY_ALWD_CHRGS NPYAC
	  ON CAGG.HCPCS_CD             = NPYAC.HCPCS_CD                                        
	 AND TRIM(CAGG.HCPCS_MDFR_CD)  = TRIM(NPYAC.HCPCS_MDFR_CD)                            
	 AND CAGG.HCPCS_FMR_CLSFCTN_CD = NPYAC.HCPCS_FMR_CLSFCTN_CD                 
																			 
	GROUP BY CAGG.CLM_FMR_INTRVL_NUM                                                        
			,CAGG.PRIOR_INTRVL                                                             
			,CAGG.GEO_RGN_CD                                                               
			,CAGG.CLM_CNTRCTR_NUM                                                          
			,CLM_RNDRG_FED_PRVDR_SPCLTY_CD                                         
			,CAGG.HCPCS_CD                                                                 
			,CAGG.HCPCS_MDFR_CD                                                            
			,CAGG.HCPCS_FMR_CLSFCTN_CD                                                     
			,CRANK.CARR_RANK                                                               
			,NRANK.NATL_RANK                                                               
			,CAGG.CARR_ENRLMT                                                              
			,CAGG.NATL_ENRLMT                                                              
			,CAGG.CARR_ALOWD_SRVCS                                                         
			,CAGG.CARR_ALOWD_CHRGS                                                         
			,CAGG.CARR_FREQ                                                                
			,CAGG.CARR_DND_SRVCS                                                           
			,CAGG.CARR_PRR_YR_ALOWD_CHRGS                                                  
)

--***************************************************************
--* MAIN SQL
--***************************************************************
SELECT * 
FROM RESULTS_2_INSERT

""",con,exit_on_error=True)
                  

                      
   
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
