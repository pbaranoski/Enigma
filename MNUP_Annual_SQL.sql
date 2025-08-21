COPY INTO BIA_PRD.CMS_TARGET_XTR_PRD.MNUP_YEAR_FF
	(BENE_HIC_NUM, BENE_BRTH_DT)
	FROM (SELECT TRIM(SUBSTR(f.$1, 1, 11)) as BENE_HIC_NUM, to_date(SUBSTR(f.$1, 12, 8),'YYYYMMDD') as BENE_BRTH_DT  
          FROM @BIA_PRD.CMS_STAGE_XTR_PRD.BIA_PRD_XTR_FF_SSA_STG/MNUP_FNDR_YYYYMMDD.txt f)
	      FORCE=TRUE FILE_FORMAT = (TYPE = CSV)




                WITH SSA_MNUP_BENE_MATCH as (
                    
                    SELECT DISTINCT BENE.BENE_SK, TRIM(BENE.BENE_HIC_NUM) as BENE_HIC_NUM 
                    FROM IDRC_PRD.CMS_DIM_BENE_PRD.BENE  BENE
                    INNER JOIN BIA_PRD.CMS_TARGET_XTR_PRD.MNUP_YEAR_FF  SSA_BENE
                    ON TRIM(BENE.BENE_HIC_NUM) =  TRIM(SSA_BENE.BENE_HIC_NUM)
                    AND BENE.BENE_BRTH_DT      =  SSA_BENE.BENE_BRTH_DT
                    WHERE BENE.BENE_DISP_CD    = 'A' 

                )

                ,SSA_MNUP_CLM_MATCH as (

                    SELECT DISTINCT BENE.BENE_SK, TRIM(BENE.BENE_HIC_NUM) as BENE_HIC_NUM
                    FROM SSA_MNUP_BENE_MATCH  BENE
                    INNER JOIN IDRC_PRD.CMS_FCT_CLM_PRD.CLM  C
                    ON  BENE.BENE_SK        =  C.BENE_SK
                    AND C.CLM_FROM_DT BETWEEN dateadd(year, -3, CURRENT_DATE) AND CURRENT_DATE 
                    AND C.CLM_FINL_ACTN_IND = 'Y'

                )


                SELECT DISTINCT

                    RPAD(SSA_BENE.BENE_HIC_NUM,11,' ')   AS BENE_HICN

                    ,case when bene.bene_sk is null then 'N' else 'Y' end 
                     as bene_match
                    ,case when clm.bene_sk is null then 'N' else 'Y' end 
                     as Medicare_Usedin_Last3YRS

                    ,case when BD.BENE_SK is null then 'N' else 'Y' end 
                     as HMO_IND  /* MCO_IND OR HMO_IND */
                    ,case when BDS.BENE_SK is null then 'N' else 'Y' end 
                     AS IN_Nursinghome 
                    ,case when BPR.BENE_SK is null then 'N' else 'Y' end  
                     AS  Private_Health_Insurance

                    ,CASE when BCC.BENE_VA_CVRG_SW IS NULL 
                          THEN 'N' 
                          WHEN BCC.BENE_VA_CVRG_SW = '1'
                          THEN 'Y'
                          ELSE 'N' end  
                     AS VA_Switch

                    ,CASE WHEN BCC.BENE_TRICR_CVRG_SW IS NULL 
                          THEN 'N' 
                          WHEN BCC.BENE_TRICR_CVRG_SW = '1'
                          THEN 'Y'
                          ELSE 'N' end  
                     AS TRICARE_Switch
                    ,REPEAT(' ',12) as FILLER 
                 

                /* Table which contains the finder file data from SSA */
                FROM BIA_PRD.CMS_TARGET_XTR_PRD.MNUP_YEAR_FF SSA_BENE

                /* this table contains the BENE's matched from the finder file against*/
                /* BENE table in IDR */
                LEFT OUTER JOIN SSA_MNUP_BENE_MATCH  BENE
                ON BENE.BENE_HIC_NUM = SSA_BENE.BENE_HIC_NUM

                /* this table contains the BENE's matched from the BENE table in IDR*/
                /* WHO have claims within the last 3 years*/
                LEFT OUTER JOIN SSA_MNUP_CLM_MATCH  CLM
                ON CLM.BENE_HIC_NUM = SSA_BENE.BENE_HIC_NUM

                /* This join for the Bene coverage HMO OR Managed care Indicator */
                LEFT OUTER JOIN IDRC_PRD.CMS_FCT_BENE_MTRLZD_PRD.BENE_DNMTR BD
                on BD.BENE_SK = bene.BENE_SK
                and BD.BENE_HIC_NUM = SSA_BENE.BENE_HIC_NUM
                and BD.BENE_DNMTR_CY_NUM  = 2024
                and BD.BENE_DNMTR_MO_NUM  = 12
                and BD.BENE_HMO_CVRG_IND  <> '0'

                /* This join for the Bene Nursing Home Indicator */
                LEFT OUTER JOIN IDRC_PRD.CMS_DIM_BENE_PRD.BENE_DUAL_STUS BDS
                on BDS.BENE_SK             = BENE.BENE_SK
                and BDS.IDR_LTST_TRANS_FLG = 'Y'
                and BDS.BENE_DUAL_INSTNL_STUS_IND_SW = 'Y'

                /* This join for the Bene Private health Indicator */
                LEFT OUTER JOIN  IDRC_PRD.CMS_DIM_BENE_PRD.BENE_MI_PRFL  BPR
                ON BENE.BENE_SK  = BPR.BENE_SK
                --and BPR.BENE_AUDT_SQNC_NUM=0
                and BPR.BENE_MDCL_CVRG_TYPE_CD = 'S'


                /* This join for the Bene VA coverage Indicator */
                /* This join for the Bene TRICARE coverage Indicator */
                LEFT OUTER JOIN BIA_PRD.CMS_DIM_BEPSD_PRD.BENE_CRDTBL_CVRG BCC
                ON BCC.BENE_LINK_KEY = BENE.BENE_SK
                And BCC.CLNDR_CY_NUM = 2024

                ORDER BY BENE_HICN
