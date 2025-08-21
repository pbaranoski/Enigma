########################################################################################################
# Name: Cred_Cov_Rpts.py
# ***********************************************************************************
# ***********************************************************************************
# ***********************************************************************************
# ***********************************************************************************
# *********************************************************************************** 
# Desc: Creditable Coverage Reports. These are Adhoc scripts. Please copy the SQLs to 
# Snowflake and execute them after Creditable Coverage load. Please change {ENV_NAME}
# to DEV/TST/IMPL/PRD and  {clndr_cy_num} to the current year.
# ***********************************************************************************
# ***********************************************************************************
# ***********************************************************************************
# ***********************************************************************************
# Created: Sumathi Gayam 02/02/2023
# Modified:
#
########################################################################################################


/* To get the total (Using the date when the CC file has been loaded to IDR) */
Select	b.BENE_PTD_STUS_CD,b.BENE_RTRMT_RX_BNFT_CD,a.BENE_VA_CVRG_SW,
		a.BENE_TRICR_CVRG_SW, a.BENE_FEHBP_CVRG_SW, a.BENE_WORKG_AGED_CVRG_SW, count(b.BENE_SK)
From	IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS b
       Left outer join BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG a
	on	a.BENE_LINK_KEY=b.BENE_SK and 
    a.clndr_cy_num='{clndr_cy_num}'
where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
GRoup	by 1, 2, 3, 4, 5,6
order by 1,2,3,4,5,6;
/* To get the total NO DRUG CVRG  (that we know of) with PTD=N and RTR=N */
Select	b.BENE_PTD_STUS_CD,b.BENE_RTRMT_RX_BNFT_CD,a.BENE_VA_CVRG_SW,
		a.BENE_TRICR_CVRG_SW, a.BENE_FEHBP_CVRG_SW, a.BENE_WORKG_AGED_CVRG_SW, count(b.BENE_SK)
From	IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}. BENE_FCT_TRANS b
           left outer join BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG a
	on	a.BENE_LINK_KEY=b.BENE_SK and
    a.clndr_cy_num='{clndr_cy_num}'
where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
GRoup	by 1, 2, 3, 4, 5,6
order by 1,2,3,4,5,6;

/* To get the part d only */
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD') and bene_ptd_stus_cd='Y'
and bene_sk not in (select BENE_LINK_KEY
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num});

/* To get the RTR only */
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS 
where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
and BENE_RTRMT_RX_BNFT_CD='Y'
and bene_sk not in (select BENE_LINK_KEY
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num});

/* To get total Count of benes matched in medicare for TRICARE */
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
and bene_sk in (select BENE_LINK_KEY
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num} and BENE_TRICR_CVRG_SW='1' );
/* To get total Count of benes in cred cvrg table for TRICARE */
select count(*) 
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG 
where clndr_cy_num={clndr_cy_num} and BENE_TRICR_CVRG_SW='1' ;


/* To get total Count of benes matched in medicare for VA */
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
and bene_sk in (select BENE_LINK_KEY
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num}
and BENE_VA_CVRG_SW='1' );

/* To get total Count of benes in cred cvrg table for VA */
select count(*) 
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG 
where clndr_cy_num={clndr_cy_num} and BENE_VA_CVRG_SW='1' ;

/* To get total Count of benes matched in medicare for FEHBP */
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
and bene_sk in (select BENE_LINK_KEY
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num}
and BENE_FEHBP_CVRG_SW='1' );
/* To get total Count of benes in cred cvrg table for FEHBP */
select count(*) 
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG 
where clndr_cy_num={clndr_cy_num} and BENE_FEHBP_CVRG_SW='1' ;


/* To get total Count of benes matched in medicare for Working Aged Sum */
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
and bene_sk in (select BENE_LINK_KEY 
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num}
and BENE_WORKG_AGED_CVRG_SW='1' );

/* To get total Count of benes in cred cvrg table for Working Aged Sum */
select count(*) 
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG 
where clndr_cy_num={clndr_cy_num} and BENE_WORKG_AGED_CVRG_SW='1' ;

/* To get total Count of benes matched Part D with other creditable coverage*/
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD') 
and bene_ptd_stus_cd='Y'
and bene_sk in (select BENE_LINK_KEY 
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num}
and (BENE_WORKG_AGED_CVRG_SW='1' or  BENE_FEHBP_CVRG_SW='1'  or BENE_VA_CVRG_SW='1' or BENE_TRICR_CVRG_SW='1') );


/* To get total Count of benes matched rtr with other crediable coverage*/
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
and BENE_RTRMT_RX_BNFT_CD='Y' and bene_sk  in (select BENE_LINK_KEY
from BIA_{ENV_NAME}.CMS_DIM_BEPSD_{ENV_NAME}.BENE_CRDTBL_CVRG where clndr_cy_num={clndr_cy_num}
and (BENE_WORKG_AGED_CVRG_SW='1' or  BENE_FEHBP_CVRG_SW='1'  or BENE_VA_CVRG_SW='1' or BENE_TRICR_CVRG_SW='1') );
/* to verify the total bene count with variour types of codes*/
Select	b.BENE_PTD_STUS_CD,b.BENE_RTRMT_RX_BNFT_CD, count(b.BENE_SK)
From	IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS b where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD')
Group	by 1, 2;
/* to get the total counts Bene_Fct */ 
select count(*) from IDRC_{ENV_NAME}.CMS_FCT_BENE_MTRLZD_{ENV_NAME}.BENE_FCT_TRANS where  '{clndr_cy_num}-06-01'  between BENE_FCT_EFCTV_DT AND BENE_FCT_OBSLT_DT
AND IDR_TRANS_OBSLT_TS = to_date('9999-12-31','YYYY-MM-DD');

