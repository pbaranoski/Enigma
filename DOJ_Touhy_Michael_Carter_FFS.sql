COPY INTO @BIA_PRD.CMS_STAGE_XTR_PRD.BIA_PRD_XTR_DOJ_STG/DOJ_TOUHY_MICHAEL_CARTER_FFS_2023b.txt.gz
 FROM (

                                                
WITH CLM_BENE_INFO AS (

                  SELECT DISTINCT
                       'START'
                      ,C.CLM_TYPE_CD
                      ,TO_CHAR(CL.CLM_LINE_NUM,'FM00000')       AS CLM_LINE_NUM
                      ,RPAD(COALESCE(C.CLM_HIC_NUM,' '),12,' ')  AS BENE_HICN
                      ,RPAD(REPLACE(COALESCE(C.BENE_SEX_CD,''),'~',''),1,' ') AS BENE_SEX_CD
                      ,RPAD(COALESCE(TO_CHAR(C.CLM_PTNT_BIRTH_DT, 'YYYYMMDD'),''),8,' ') AS BENE_DOB

                      ,RPAD(COALESCE(BENE.BENE_LAST_NAME,''),40,' ')    AS BENE_LAST_NAME 
                      ,RPAD(COALESCE(BENE.BENE_1ST_NAME,''),30,' ')     AS BENE_1ST_NAME
                      ,RPAD(COALESCE(BENE.BENE_MIDL_NAME,''),15,' ')    AS BENE_MIDL_NAME
                              
                      ,RPAD(REPLACE(COALESCE(GCNTY.GEO_SSA_STATE_CD,''),'~',''),2,' ')  AS SSA_STATE_CD
                      ,RPAD(REPLACE(COALESCE(ZIP5.GEO_ZIP5_CD,''),'~',''),5,' ')        AS BENE_MAILING_ZIPCD  
                      
                      ,TO_CHAR(CLM_ACTV_CARE_FROM_DT,'YYYYMMDD')  AS CLM_ACTV_CARE_FROM_DT
                      ,TO_CHAR(CDS.CLM_DSCHRG_DT,'YYYYMMDD')      AS CLM_DSCHRG_DT
                      ,TO_CHAR(C.CLM_FROM_DT,'YYYYMMDD')          AS CLM_FROM_DT
                      ,TO_CHAR(C.CLM_THRU_DT,'YYYYMMDD')          AS CLM_THRU_DT
                      
                      ,RPAD(C.CLM_CNTL_NUM,40,' ')                   AS CLAIM_CONTROL_NBR
                      ,RPAD(COALESCE(C.CLM_ORIG_CNTL_NUM,''),40,' ') AS ORIG_CLAIM_CONTROL_NBR
                      
                      ,CI.CLM_TRANS_CD
                      ,C.CLM_DISP_CD
                      ,CI.BENE_PTNT_STUS_CD
                      
                      ,RPAD(CASE WHEN COALESCE(CI.CLM_ADMSN_TYPE_CD,' ') = '~' THEN ' ' ELSE CI.CLM_ADMSN_TYPE_CD END,2,' ')  AS CLM_ADMSN_TYPE_CD
                      ,RPAD(CASE WHEN COALESCE(CI.CLM_ADMSN_SRC_CD,' ')  = '~' THEN ' ' ELSE CI.CLM_ADMSN_SRC_CD  END,2,' ')  AS CLM_ADMSN_SRC_CD
                      
                      ,CL.CLM_LINE_REV_CTR_CD
                      ,C.CLM_BILL_FAC_TYPE_CD
                      ,C.CLM_BILL_CLSFCTN_CD  
                      
                      ,TO_CHAR(CI.DGNS_DRG_VRSN_NUM,'FM00')  AS  DGNS_DRG_VRSN_NUM 
                      ,TO_CHAR(CI.DGNS_DRG_CD,'FM0000')      AS  DGNS_DRG_CD 
                      ,RPAD(CPM.CLM_PRNCPL_DGNS_CD,7,' ')    AS  CLM_PRNCPL_DGNS_CD 
                      ,RPAD(COALESCE(DC1.DGNS_CD_DESC,' '),250,' ')  AS  DGNS_CD_DESC 
                      
                      ,RPAD(CASE WHEN CL.CLM_LINE_NDC_CD = '~' THEN ' ' ELSE CL.CLM_LINE_NDC_CD END,11,' ') AS CLM_LINE_NDC_CD
                      ,RPAD(COALESCE(CL.CLM_LINE_HCPCS_CD,' '),5,' ')  AS CLM_LINE_HCPCS_CD
                      ,RPAD(CL.HCPCS_1_MDFR_CD,2,' ')  AS HCPCS_1_MDFR_CD
                      ,RPAD(CL.HCPCS_2_MDFR_CD,2,' ')  AS HCPCS_2_MDFR_CD
                      ,RPAD(CL.HCPCS_3_MDFR_CD,2,' ')  AS HCPCS_3_MDFR_CD
                      ,RPAD(CL.HCPCS_4_MDFR_CD,2,' ')  AS HCPCS_4_MDFR_CD

                      ,RPAD(C.CLM_BLG_PRVDR_OSCAR_NUM,20,' ')   AS  CLM_BLG_PRVDR_OSCAR_NUM 
                      ,RPAD(C.CLM_BLG_PRVDR_NPI_NUM,10,' ')     AS  CLM_BLG_PRVDR_NPI_NUM
                      ,RPAD(C.CLM_ATNDG_PRVDR_NPI_NUM,10,' ')   AS  CLM_ATNDG_PRVDR_NPI_NUM
                      ,RPAD(C.PRVDR_ATNDG_PRVDR_NPI_NUM,10,' ') AS  PRVDR_ATNDG_PRVDR_NPI_NUM

                      ,RPAD(CASE WHEN C.PRVDR_OPRTG_PRVDR_NPI_NUM = '~' THEN ' ' ELSE C.PRVDR_OPRTG_PRVDR_NPI_NUM END,10,' ')  AS PRVDR_OPRTG_PRVDR_NPI_NUM
                      ,RPAD(CASE WHEN C.PRVDR_OTHR_PRVDR_NPI_NUM = '~' THEN ' ' ELSE C.PRVDR_OTHR_PRVDR_NPI_NUM END,10,' ')    AS PRVDR_OTHR_PRVDR_NPI_NUM
                      
                      ,RPAD(REPLACE(CL.PRVDR_FAC_PRVDR_NPI_NUM,'~',''),10,' ')  AS PRVDR_FAC_PRVDR_NPI_NUM
                      ,RPAD(CASE WHEN C.PRVDR_SRVC_PRVDR_NPI_NUM = '~' THEN ' ' ELSE C.PRVDR_SRVC_PRVDR_NPI_NUM END,10,' ')  AS PRVDR_SRVC_PRVDR_NPI_NUM
                      
                      ,RPAD(COALESCE(P.PRVDR_LGL_NAME,' '),70,' ')         AS  PRVDR_LGL_NAME
                      ,RPAD(COALESCE(P.PRVDR_MLG_TEL_NUM,' '),20,' ')      AS  PRVDR_MLG_TEL_NUM
                      
                      ,RPAD(COALESCE(P.PRVDR_MLG_LINE_1_ADR,' '),100,' ')  AS  PRVDR_MLG_LINE_1_ADR
                      ,RPAD(COALESCE(P.PRVDR_MLG_LINE_2_ADR,' '),100,' ')  AS  PRVDR_MLG_LINE_2_ADR
                      ,RPAD(COALESCE(P.PRVDR_INVLD_MLG_PLC_NAME,' '),40,' ')  AS  PRVDR_INVLD_MLG_PLC_NAME 
                      ,RPAD(COALESCE(P.PRVDR_INVLD_MLG_STATE_CD,' '),2,' ')   AS  PRVDR_INVLD_MLG_STATE_CD
                      ,RPAD(COALESCE(P.PRVDR_INVLD_MLG_ZIP_CD,' '),9,' ')     AS  PRVDR_INVLD_MLG_ZIP_CD 


                      -- NUMERIC FORMATTING -- we should include decimal place and sign going forward?
                      ,TO_CHAR(C.CLM_PMT_AMT,'S0000000000000.00')  AS  CLM_PMT_AMT 
                      ,RPAD(CI.CLM_MDCR_NPMT_RSN_CD,2,' ')  AS  CLM_MDCR_NPMT_RSN_CD 
                               
                      ,TO_CHAR(COALESCE(C.CLM_PRVDR_PMT_AMT,0),'S000000000.00')  AS  CLM_PRVDR_PMT_AMT 
                      ,TO_CHAR(CL.CLM_LINE_SRVC_UNIT_QTY,'S00000000000000.0000')  AS  CLM_LINE_SRVC_UNIT_QTY 
                               
                      ,TO_CHAR(COALESCE(C.CLM_BENE_PD_AMT,0),'S0000000000000.00')  AS CLM_BENE_PD_AMT  
                      ,TO_CHAR(C.CLM_SBMT_CHRG_AMT,'S0000000000000.00')            AS CLM_SBMT_CHRG_AMT 
                               
                      ,TO_CHAR(COALESCE(C.CLM_ALOWD_CHRG_AMT,0),'S0000000000000.00')  AS CLM_ALOWD_CHRG_AMT 
                      ,TO_CHAR(COALESCE(C.CLM_MDCR_DDCTBL_AMT,0),'S0000000000000.00') AS CLM_MDCR_DDCTBL_AMT 
                               
                      ,TO_CHAR(CL.CLM_LINE_CVRD_PD_AMT,'S000000000.00')           AS CLM_LINE_CVRD_PD_AMT
                               
                      ,TO_CHAR(CL.CLM_LINE_BENE_PD_AMT,'S0000000000000.00')     AS  CLM_LINE_BENE_PD_AMT
                      ,TO_CHAR(CL.CLM_LINE_BENE_PMT_AMT,'S0000000000000.00')    AS  CLM_LINE_BENE_PMT_AMT
                               
                      ,TO_CHAR(CL.CLM_LINE_PRVDR_PMT_AMT,'S000000000.00')       AS  CLM_LINE_PRVDR_PMT_AMT
                      ,TO_CHAR(CL.CLM_LINE_MDCR_DDCTBL_AMT,'S000000000.00')     AS  CLM_LINE_MDCR_DDCTBL_AMT 

                      ,RPAD(COALESCE(C.CLM_NCH_PRMRY_PYR_CD,' '),1,' ') AS CLM_NCH_PRMRY_PYR_CD
                      ,TO_CHAR(COALESCE(CI.CLM_MDCR_INSTNL_BENE_PD_AMT,0),'S000000000.00')  AS CLM_MDCR_INSTNL_BENE_PD_AMT
                      ,TO_CHAR(CI.CLM_MDCR_INSTNL_PRMRY_PYR_AMT,'S000000000.00')            AS CLM_MDCR_INSTNL_PRMRY_PYR_AMT
                      ,TO_CHAR(COALESCE(CL.CLM_LINE_MDCR_COINSRNC_AMT,0),'S000000000.00')   AS CLM_LINE_MDCR_COINSRNC_AMT
                               
                      ,TO_CHAR(COALESCE(CL.CLM_LINE_PTB_BLOOD_DDCTBL_QTY,0),'FM000') AS CLM_LINE_PTB_BLOOD_DDCTBL_QTY
                               
                      ,TO_CHAR(CL.CLM_LINE_SBMT_CHRG_AMT,'S0000000000000.00')                AS CLM_LINE_SBMT_CHRG_AMT
                      ,TO_CHAR(COALESCE(CL.CLM_LINE_ALOWD_CHRG_AMT,0),'S0000000000000.00') AS CLM_LINE_ALOWD_CHRG_AMT
                      ,TO_CHAR(COALESCE(CL.CLM_LINE_OTHR_TP_PD_AMT,0),'S0000000000000.00') AS CLM_LINE_OTHR_TP_PD_AMT

                      ,TO_CHAR(COALESCE(C.CLM_OTHR_TP_PD_AMT,0),'S0000000000000.00')       AS CLM_OTHR_TP_PD_AMT
                      ,TO_CHAR(COALESCE(CL.CLM_LINE_BENE_COPMT_AMT,0),'S0000000000000.00') AS CLM_LINE_BENE_COPMT_AMT
                      ,CLM_FINL_ACTN_IND
                      ,RPAD(COALESCE(C.CLM_BENE_MBI_ID,''),11,' ') AS BENE_MBI_ID
                      ,'END'

                        FROM IDRC_PRD.CMS_FCT_CLM_PRD.CLM C   

                        INNER JOIN IDRC_PRD.CMS_FCT_CLM_PRD.CLM_DT_SGNTR CDS
                        ON C.CLM_DT_SGNTR_SK = CDS.CLM_DT_SGNTR_SK

                        INNER JOIN IDRC_PRD.CMS_DIM_BENE_PRD.BENE BENE
                        ON C.BENE_SK = BENE.BENE_SK

                        INNER JOIN IDRC_PRD.CMS_FCT_CLM_PRD.CLM_INSTNL  CI
                        ON  C.GEO_BENE_SK     = CI.GEO_BENE_SK
                        AND C.CLM_DT_SGNTR_SK = CI.CLM_DT_SGNTR_SK
                        AND C.CLM_TYPE_CD     = CI.CLM_TYPE_CD
                        AND C.CLM_NUM_SK      = CI.CLM_NUM_SK 

                        INNER JOIN IDRC_PRD.CMS_FCT_CLM_PRD.CLM_PROD_MTRLZD CPM 
                        ON  C.GEO_BENE_SK     = CPM.GEO_BENE_SK
                        AND C.CLM_DT_SGNTR_SK = CPM.CLM_DT_SGNTR_SK
                        AND C.CLM_TYPE_CD     = CPM.CLM_TYPE_CD
                        AND C.CLM_NUM_SK      = CPM.CLM_NUM_SK
                  
                        INNER JOIN IDRC_PRD.CMS_FCT_CLM_PRD.CLM_LINE CL
                        ON  C.GEO_BENE_SK     = CL.GEO_BENE_SK
                        AND C.CLM_DT_SGNTR_SK = CL.CLM_DT_SGNTR_SK
                        AND C.CLM_TYPE_CD     = CL.CLM_TYPE_CD
                        AND C.CLM_NUM_SK      = CL.CLM_NUM_SK
                        AND C.CLM_FROM_DT     = CL.CLM_FROM_DT

                        -- Getting PRVDR LGL_NAME --> for BILLING PRVDR_NPI_NUM?
                        LEFT OUTER JOIN IDRC_PRD.CMS_DIM_PRVDR_PRD.PRVDR P
                        ON C.CLM_BLG_PRVDR_NPI_NUM = P.PRVDR_NPI_NUM
                  
                        LEFT OUTER JOIN IDRC_PRD.CMS_VDM_VIEW_MDCR_PRD.V2_MDCR_DGNS_CD DC1
                        ON  CPM.CLM_PRNCPL_DGNS_CD = DC1.DGNS_CD
                        AND C.CLM_THRU_DT BETWEEN DC1.DGNS_CD_BGN_DT AND DC1.DGNS_CD_END_DT

                        -- Zip code should be for what was active at time of claim  
                        INNER JOIN IDRC_PRD.CMS_DIM_GEO_PRD.GEO_ZIP5_CD ZIP5
                        ON C.GEO_BENE_SK = ZIP5.GEO_SK

                        --LEFT OUTER JOIN IDRC_PRD.CMS_DIM_GEO_PRD.GEO_FIPS_CNTY_CD GCNTY
                        --ON  ZIP5.GEO_FIPS_CNTY_CD   = GCNTY.GEO_FIPS_CNTY_CD
                        --AND ZIP5.GEO_FIPS_STATE_CD  = GCNTY.GEO_FIPS_STATE_CD 

                        LEFT OUTER JOIN ( 
                                SELECT DISTINCT  GCNTY.GEO_FIPS_CNTY_CD ,GCNTY.GEO_FIPS_STATE_CD 
                                                ,GCNTY.GEO_SSA_STATE_CD ,GCNTY.GEO_SSA_CNTY_CD
                                FROM IDRC_PRD.CMS_DIM_GEO_PRD.GEO_FIPS_CNTY_CD GCNTY ) GCNTY
                        ON  ZIP5.GEO_FIPS_CNTY_CD   = GCNTY.GEO_FIPS_CNTY_CD
                        AND ZIP5.GEO_FIPS_STATE_CD  = GCNTY.GEO_FIPS_STATE_CD 
                        
                       WHERE C.CLM_TYPE_CD IN (20,30,40,60,61)
					   AND C.CLM_FINL_ACTN_IND = 'Y'
					   AND CL.CLM_LINE_FINL_ACTN_IND = 'Y'
                       AND C.CLM_FROM_DT BETWEEN TO_DATE('2023-01-01','YYYY-MM-DD') AND TO_DATE('2023-12-31','YYYY-MM-DD')
            
                      AND (
                         C.CLM_ATNDG_PRVDR_NPI_NUM  IN (SELECT FF.NPI_NUM FROM BIA_PRD.CMS_TARGET_XTR_PRD.DOJ_TOUHY_NPI_FF FF )
                      OR C.CLM_BLG_PRVDR_NPI_NUM    IN (SELECT FF.NPI_NUM FROM BIA_PRD.CMS_TARGET_XTR_PRD.DOJ_TOUHY_NPI_FF FF )
                      OR C.CLM_OPRTG_PRVDR_NPI_NUM  IN (SELECT FF.NPI_NUM FROM BIA_PRD.CMS_TARGET_XTR_PRD.DOJ_TOUHY_NPI_FF FF )
                      OR C.CLM_OTHR_PRVDR_NPI_NUM   IN (SELECT FF.NPI_NUM FROM BIA_PRD.CMS_TARGET_XTR_PRD.DOJ_TOUHY_NPI_FF FF )
                      OR CL.PRVDR_FAC_PRVDR_NPI_NUM IN (SELECT FF.NPI_NUM FROM BIA_PRD.CMS_TARGET_XTR_PRD.DOJ_TOUHY_NPI_FF FF )
                      OR C.PRVDR_SRVC_PRVDR_NPI_NUM IN (SELECT FF.NPI_NUM FROM BIA_PRD.CMS_TARGET_XTR_PRD.DOJ_TOUHY_NPI_FF FF )

                      ) 
            
)

SELECT *
FROM CLM_BENE_INFO C



) FILE_FORMAT = (TYPE = CSV field_delimiter = "|"  ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY = none )
                        SINGLE=TRUE  max_file_size=5368709120 
                      
	