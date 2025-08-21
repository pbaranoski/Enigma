#!/usr/bin/env python
########################################################################################################
# Name: PartABExtract.py
#
# Desc: 
#
# Created: Sean Whitelock
# Modified: 
#
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
bPythonExceptionOccurred = False

# Timestamp and environment setup
now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

TMSTMP=os.getenv('TMSTMP')
ENVNAME=os.getenv('ENVNAME')

# Read environment variables
BEG_MONTH = os.getenv('BEGMONTH')
END_MONTH = os.getenv('ENDMONTH')
YEAR = os.getenv('YEAR')
FISCAL_YEAR_START = os.getenv('FISCAL_YEAR_START')

# Output paths
output_dir = os.getenv('PARTAB_OUTPUT_DIR', '/tmp/')
YEAR_FILE_ZIP = f"PartAB_Year_{TMSTMP}.txt.gz"
MONTH_FILE_ZIP = f"PartAB_Month_{TMSTMP}.txt.gz"

########################################################################################################
# RUN
########################################################################################################
print('')
print("Run date and time: " + date_time  )
print('')

########################################################################################################
# Execute SQL and Save to Dataframe
########################################################################################################
try:
    snowconvert_helpers.configure_log()
    con = snowconvert_helpers.log_on()
    snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
    snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

    #****************
    # SQL for Yearly
    #****************
	
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PART_AB_EXTRACT/{YEAR_FILE_ZIP}
        FROM (

			SELECT 
				'YTD' AS Year_Month, 
				CLM.CLM_TYPE_CD,
			CASE CLM.CLM_TYPE_CD 
				WHEN 60 THEN 'Hospital'
				WHEN 61 THEN 'Hospital (MCO)'
				WHEN 50 THEN 'Hospice'
				WHEN 40 THEN 'Outpatient'
				WHEN 30 THEN 'Swing Bed SNF'
				WHEN 20 THEN 'SNF'
				WHEN 10 THEN 'HHA'
				WHEN 71 THEN 'Carrier'
				WHEN 72 THEN 'Carrier'
				WHEN 81 THEN 'DMERC'
				WHEN 82 THEN 'DMERC'
				ELSE 'Unknown' 
			END 
		AS Provider_Type,
			SUM
		(CASE 
			WHEN CLM.CLM_TYPE_CD IN(20,30,60) AND (CLM.CLM_BILL_CLSFCTN_CD NOT IN ('2','4') OR CLM.CLM_BILL_CLSFCTN_CD IS NULL) AND CLM_NRLN_RIC_CD <> 'W' 
			THEN 
		(CASE CLM_ADJSTMT_TYPE_CD 
			WHEN '0' THEN 1*CLM_PMT_AMT 
			WHEN '2' THEN 1*CLM_PMT_AMT 
			WHEN '1' THEN -1*CLM_PMT_AMT 
			ELSE 0 
		END)
		WHEN CLM.CLM_TYPE_CD = 61 AND CLM_RLT_COND_CD IS NULL AND (CLM.CLM_BILL_CLSFCTN_CD NOT IN ('2','4') OR CLM.CLM_BILL_CLSFCTN_CD IS NULL) AND CLM_NRLN_RIC_CD <> 'W' 
		THEN 
	(CASE CLM_ADJSTMT_TYPE_CD 
		WHEN '0' THEN 1*CLM_PMT_AMT 
		WHEN '2' THEN 1*CLM_PMT_AMT 
		WHEN '1' THEN -1*CLM_PMT_AMT 
		ELSE 0 
	END)
	WHEN CLM.CLM_TYPE_CD=50 
	THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
	WHEN '0' THEN 1*CLM_PMT_AMT 
	WHEN '2' THEN 1*CLM_PMT_AMT 
	WHEN '1' THEN -1*CLM_PMT_AMT 
	ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD = 10 
THEN 
CASE CLM_NRLN_RIC_CD 
WHEN 'V' THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN 'U' THEN HH_A_Reimb + HH_A_Outlier 
ELSE 0 
END
ELSE 0 
END )
AS Part_A_Medicare_Payment,
SUM
(CASE 
WHEN CLM.CLM_TYPE_CD IN(20,30,60) AND (CLM.CLM_BILL_CLSFCTN_CD IN ('2','4') OR CLM_NRLN_RIC_CD = 'W') 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD = 61 AND CLM_RLT_COND_CD IS NULL AND (CLM.CLM_BILL_CLSFCTN_CD IN ('2','4') OR CLM_NRLN_RIC_CD = 'W') 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD IN(40,71,72,81,82) 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD = 10 
THEN 
CASE CLM_NRLN_RIC_CD 
WHEN 'W' THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN 'U' THEN HH_B_Reimb + HH_B_Outlier 
ELSE 0 
END
ELSE 0 
END)
AS Part_B_Medicare_Payment,
SUM
(CASE 
WHEN CLM.CLM_TYPE_CD IN (60,61) 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT 
WHEN '2' THEN 1*CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT 
WHEN '1' THEN -1*CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT 
ELSE 0 
END)
ELSE 0 
END)
AS IP_Pass_Thru,
SUM
(CASE 
WHEN CLM.CLM_TYPE_CD = 61 AND CLM_RLT_COND_CD = '69' 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
ELSE 0 
END)
AS MCO_IME,
SUM(
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
				)+IP_Pass_Thru AS Total_Medicare_Payment,
				(Total_Medicare_Payment - Part_A_Medicare_Payment - Part_B_Medicare_Payment - IP_Pass_Thru - MCO_IME) AS Compare_Difference
			FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM AS CLM 
				INNER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_DT
					ON CLM_EFCTV_DT=CLNDR_DT 
				LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN AS DCMTN
					ON CLM.GEO_BENE_SK=DCMTN.GEO_BENE_SK
					AND CLM.CLM_DT_SGNTR_SK=DCMTN.CLM_DT_SGNTR_SK
					AND CLM.CLM_TYPE_CD=DCMTN.CLM_TYPE_CD
					AND CLM.CLM_NUM_SK=DCMTN.CLM_NUM_SK
					AND DCMTN.CLM_TYPE_CD IN(10,20,30,60,61) 
				LEFT OUTER JOIN 
					(SELECT 
						VAL.GEO_BENE_SK
						,VAL.CLM_DT_SGNTR_SK
						,VAL.CLM_TYPE_CD
						,VAL.CLM_NUM_SK
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '62' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_A_Vis
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '63' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_B_Vis
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '64' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_A_Reimb
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '65' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_B_Reimb
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '17' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_Outlier_Amt
						,
					CASE 
						WHEN HH_A_Vis + HH_B_Vis <> 0 
						THEN CAST( HH_A_Vis /(HH_A_Vis + HH_B_Vis) * HH_Outlier_Amt AS DECIMAL(11,2)) 
					ELSE 0 
					END 
				AS HH_A_Outlier
					,HH_Outlier_Amt - HH_A_Outlier AS HH_B_Outlier
				FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_VAL VAL 
					JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM CLM
						ON CLM.GEO_BENE_SK=VAL.GEO_BENE_SK
						AND CLM.CLM_DT_SGNTR_SK=VAL.CLM_DT_SGNTR_SK
						AND CLM.CLM_TYPE_CD=VAL.CLM_TYPE_CD
						AND CLM.CLM_NUM_SK=VAL.CLM_NUM_SK
					WHERE VAL.CLM_TYPE_CD = 10
						AND CLM_VAL_CD IN ('17','62','63','64','65')
					GROUP BY 1, 2, 3, 4) AS VAL
						ON CLM.GEO_BENE_SK=VAL.GEO_BENE_SK
						AND CLM.CLM_DT_SGNTR_SK=VAL.CLM_DT_SGNTR_SK
						AND CLM.CLM_TYPE_CD=VAL.CLM_TYPE_CD
						AND CLM.CLM_NUM_SK=VAL.CLM_NUM_SK 
					LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_INSTNL AS INSTNL
						ON CLM.GEO_BENE_SK=INSTNL.GEO_BENE_SK
						AND CLM.CLM_DT_SGNTR_SK=INSTNL.CLM_DT_SGNTR_SK
						AND CLM.CLM_TYPE_CD=INSTNL.CLM_TYPE_CD
						AND CLM.CLM_NUM_SK=INSTNL.CLM_NUM_SK
						AND INSTNL.CLM_TYPE_CD IN(20,30,60,61) 
					LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_RLT_COND_SGNTR_MBR AS RLT
						ON CLM.CLM_RLT_COND_SGNTR_SK = rlt.CLM_RLT_COND_SGNTR_SK
						AND CLM_RLT_COND_CD = '69'
					WHERE CLM.CLM_TYPE_CD IN (10,20,30,40,50,60,61,71,72,81,82)
						AND CLNDR_DT BETWEEN '{YEAR}' AND '{END_MONTH}'
					GROUP BY 1,2,3
						ORDER BY 1,2,3
							
					)
        FILE_FORMAT = (TYPE = CSV field_delimiter = '|' ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY = NONE NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE)
        SINGLE = TRUE
        HEADER = TRUE
        MAX_FILE_SIZE = 5368709120
    """, con, exit_on_error=True)                      
    


    #*****************
    # SQL for Monthly
    #*****************
    snowconvert_helpers.execute_sql_statement(f"""COPY INTO @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_PART_AB_EXTRACT/{MONTH_FILE_ZIP}
        FROM (
			SELECT 
				CLNDR_CY_MO_NUM AS Year_Month, 
				CLM.CLM_TYPE_CD,
			CASE CLM.CLM_TYPE_CD 
				WHEN 60 THEN 'Hospital'
				WHEN 61 THEN 'Hospital (MCO)'
				WHEN 50 THEN 'Hospice'
				WHEN 40 THEN 'Outpatient'
				WHEN 30 THEN 'Swing Bed SNF'
				WHEN 20 THEN 'SNF'
				WHEN 10 THEN 'HHA'
				WHEN 71 THEN 'Carrier'
				WHEN 72 THEN 'Carrier'
				WHEN 81 THEN 'DMERC'
				WHEN 82 THEN 'DMERC'
				ELSE 'Unknown' 
			END 
		AS Provider_Type,
			SUM
		(CASE 
			WHEN CLM.CLM_TYPE_CD IN(20,30,60) AND (CLM.CLM_BILL_CLSFCTN_CD NOT IN ('2','4') OR CLM.CLM_BILL_CLSFCTN_CD IS NULL) AND CLM_NRLN_RIC_CD <> 'W' 
			THEN 
		(CASE CLM_ADJSTMT_TYPE_CD 
			WHEN '0' THEN 1*CLM_PMT_AMT 
			WHEN '2' THEN 1*CLM_PMT_AMT 
			WHEN '1' THEN -1*CLM_PMT_AMT 
			ELSE 0 
		END)
		WHEN CLM.CLM_TYPE_CD = 61 AND CLM_RLT_COND_CD IS NULL AND (CLM.CLM_BILL_CLSFCTN_CD NOT IN ('2','4') OR CLM.CLM_BILL_CLSFCTN_CD IS NULL) AND CLM_NRLN_RIC_CD <> 'W' 
		THEN 
	(CASE CLM_ADJSTMT_TYPE_CD 
		WHEN '0' THEN 1*CLM_PMT_AMT 
		WHEN '2' THEN 1*CLM_PMT_AMT 
		WHEN '1' THEN -1*CLM_PMT_AMT 
		ELSE 0 
	END)
	WHEN CLM.CLM_TYPE_CD=50 
	THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
	WHEN '0' THEN 1*CLM_PMT_AMT 
	WHEN '2' THEN 1*CLM_PMT_AMT 
	WHEN '1' THEN -1*CLM_PMT_AMT 
	ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD = 10 
THEN 
CASE CLM_NRLN_RIC_CD 
WHEN 'V' THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN 'U' THEN HH_A_Reimb + HH_A_Outlier 
ELSE 0 
END
ELSE 0 
END )
AS Part_A_Medicare_Payment,
SUM
(CASE 
WHEN CLM.CLM_TYPE_CD IN(20,30,60) AND (CLM.CLM_BILL_CLSFCTN_CD IN ('2','4') OR CLM_NRLN_RIC_CD = 'W') 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD = 61 AND CLM_RLT_COND_CD IS NULL AND (CLM.CLM_BILL_CLSFCTN_CD IN ('2','4') OR CLM_NRLN_RIC_CD = 'W') 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD IN(40,71,72,81,82) 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN CLM.CLM_TYPE_CD = 10 
THEN 
CASE CLM_NRLN_RIC_CD 
WHEN 'W' THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
WHEN 'U' THEN HH_B_Reimb + HH_B_Outlier 
ELSE 0 
END
ELSE 0 
END)
AS Part_B_Medicare_Payment,
SUM
(CASE 
WHEN CLM.CLM_TYPE_CD IN (60,61) 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT 
WHEN '2' THEN 1*CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT 
WHEN '1' THEN -1*CLM_INSTNL_PER_DIEM_AMT * CLM_INSTNL_CVRD_DAY_CNT 
ELSE 0 
END)
ELSE 0 
END)
AS IP_Pass_Thru,
SUM
(CASE 
WHEN CLM.CLM_TYPE_CD = 61 AND CLM_RLT_COND_CD = '69' 
THEN 
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
ELSE 0 
END)
AS MCO_IME,
SUM(
(CASE CLM_ADJSTMT_TYPE_CD 
WHEN '0' THEN 1*CLM_PMT_AMT 
WHEN '2' THEN 1*CLM_PMT_AMT 
WHEN '1' THEN -1*CLM_PMT_AMT 
ELSE 0 
END)
				)+IP_Pass_Thru AS Total_Medicare_Payment,
				(Total_Medicare_Payment - Part_A_Medicare_Payment - Part_B_Medicare_Payment - IP_Pass_Thru - MCO_IME) AS Compare_Difference
			FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM AS CLM 
				INNER JOIN IDRC_{ENVNAME}.CMS_DIM_CLNDR_{ENVNAME}.CLNDR_DT
					ON CLM_EFCTV_DT=CLNDR_DT 
				LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_DCMTN AS DCMTN
					ON CLM.GEO_BENE_SK=DCMTN.GEO_BENE_SK
					AND CLM.CLM_DT_SGNTR_SK=DCMTN.CLM_DT_SGNTR_SK
					AND CLM.CLM_TYPE_CD=DCMTN.CLM_TYPE_CD
					AND CLM.CLM_NUM_SK=DCMTN.CLM_NUM_SK
					AND DCMTN.CLM_TYPE_CD IN(10,20,30,60,61) 
				LEFT OUTER JOIN 
					(SELECT 
						VAL.GEO_BENE_SK
						,VAL.CLM_DT_SGNTR_SK
						,VAL.CLM_TYPE_CD
						,VAL.CLM_NUM_SK
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '62' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_A_Vis
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '63' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_B_Vis
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '64' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_A_Reimb
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '65' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_B_Reimb
						,SUM
					(CASE 
						WHEN CLM_VAL_CD = '17' 
						THEN 
					(CASE CLM_ADJSTMT_TYPE_CD 
						WHEN '0' THEN 1*CLM_VAL_AMT 
						WHEN '2' THEN 1*CLM_VAL_AMT 
						WHEN '1' THEN -1*CLM_VAL_AMT 
						ELSE 0 
					END)
						ELSE 0 
						END)
					AS HH_Outlier_Amt
						,
					CASE 
						WHEN HH_A_Vis + HH_B_Vis <> 0 
						THEN CAST( HH_A_Vis /(HH_A_Vis + HH_B_Vis) * HH_Outlier_Amt AS DECIMAL(11,2)) 
					ELSE 0 
					END 
				AS HH_A_Outlier
					,HH_Outlier_Amt - HH_A_Outlier AS HH_B_Outlier
				FROM IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_VAL VAL 
					JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM CLM
						ON CLM.GEO_BENE_SK=VAL.GEO_BENE_SK
						AND CLM.CLM_DT_SGNTR_SK=VAL.CLM_DT_SGNTR_SK
						AND CLM.CLM_TYPE_CD=VAL.CLM_TYPE_CD
						AND CLM.CLM_NUM_SK=VAL.CLM_NUM_SK
					WHERE VAL.CLM_TYPE_CD = 10
						AND CLM_VAL_CD IN ('17','62','63','64','65')
					GROUP BY 1, 2, 3, 4) AS VAL
						ON CLM.GEO_BENE_SK=VAL.GEO_BENE_SK
						AND CLM.CLM_DT_SGNTR_SK=VAL.CLM_DT_SGNTR_SK
						AND CLM.CLM_TYPE_CD=VAL.CLM_TYPE_CD
						AND CLM.CLM_NUM_SK=VAL.CLM_NUM_SK 
					LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_INSTNL AS INSTNL
						ON CLM.GEO_BENE_SK=INSTNL.GEO_BENE_SK
						AND CLM.CLM_DT_SGNTR_SK=INSTNL.CLM_DT_SGNTR_SK
						AND CLM.CLM_TYPE_CD=INSTNL.CLM_TYPE_CD
						AND CLM.CLM_NUM_SK=INSTNL.CLM_NUM_SK
						AND INSTNL.CLM_TYPE_CD IN(20,30,60,61) 
					LEFT OUTER JOIN IDRC_{ENVNAME}.CMS_FCT_CLM_{ENVNAME}.CLM_RLT_COND_SGNTR_MBR AS RLT
						ON CLM.CLM_RLT_COND_SGNTR_SK = rlt.CLM_RLT_COND_SGNTR_SK
						AND CLM_RLT_COND_CD = '69'
					WHERE CLM.CLM_TYPE_CD IN (10,20,30,40,50,60,61,71,72,81,82)
						AND CLNDR_DT BETWEEN '{BEG_MONTH}' AND '{END_MONTH}'
					GROUP BY 1,2,3
						ORDER BY 1,2,3
                        )
        FILE_FORMAT = (TYPE = CSV field_delimiter = '|' ESCAPE_UNENCLOSED_FIELD=NONE FIELD_OPTIONALLY_ENCLOSED_BY = NONE NULL_IF=() EMPTY_FIELD_AS_NULL=FALSE)
        SINGLE = TRUE
        HEADER = TRUE
        MAX_FILE_SIZE = 5368709120
    """, con, exit_on_error=True)
    


   #**************************************
   # End Application
   #**************************************
    snowconvert_helpers.quit_application()

except Exception as e:
    print(e)
    bPythonExceptionOccurred = True

finally:
    if con is not None:
        con.close()

    if bPythonExceptionOccurred:
        sys.exit(12)
    else:
        snowconvert_helpers.quit_application()





