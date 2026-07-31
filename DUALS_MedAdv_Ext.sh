#!/usr/bin/bash
############################################################################################################
# Name:  DUALS_MedAdv_Ext.sh
#
# Desc: Duals Quarterly PTA/PTB Medicare Advantage extract 
#
# NOTE: Extract will run for a year's worth of data with a 3-month lag. 
#  Ex1: Run June 30, 2025 for Jan 1-Mar 31 2025
#  Ex2: Run September 30, 2025 for April 1 - June 30, 2025
#
# NOTE2: Historical runs will use override dates entered in RunDeck
# 
# Execute as ./DUALS_MedAdv_Ext.sh $1 $2 (Where $1 and $2 are both optional)
#
# $1 = optional override EXT FROM DATE (YYYY-MM-DD format)   
# $2 = optional override EXT THRU DATE (YYYY-MM-DD format)
#
#
# ST_EXT_FNAME_MODEL=DUALS_MedAdv_MD_AH_202410_202412_${TMSTMP}.txt
#
# ST_EXT_FNAME_MODEL=DUALS_MedAdv_XX_{EXT_TYPE}_${YYYYMM_FROM}_${YYYYMM_THRU}_${TMSTMP}.txt
# S3_EXTRACT_FILE=DUALS_MedAdv_XX_${YYYYMM_FROM}_${YYYYMM_THRU}_${TMSTMP}.txt.gz
#
#
# Author     : Paul Baranoski	
# Created    : 01/23/2025
#
# Modified:
#
# Paul Baranoski 2025-01-23 Created script.
# Paul Baranoski 2026-05-29 Modified EFT mask to display in success emails.
# Paul Baranoski 2026-06-04 Add split logic for BH files.
############################################################################################################

set +x

#############################################################
# Include module that includes all constants 
#############################################################
TESTING="N"
export TESTING

source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh  

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/${TESTLOG}DUALS_MedAdv_Ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

# State Parameter file contains states that want this extract.
ST_PARMFILE=DUALS_MedAdv_StParms.txt
DUALS_MEDADV_FILE_PREFIX=DUALS_MedAdv
EFT_FILEMASK="P#EFT.ON.GST.DUAL.AH.PYYQQ.TIMESTAMP"


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DUALS_MedAdv_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# Verify that required NOF parameters have been sent 
#############################################################
if ! [[ $# -eq 0 || $# -eq 2  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

#############################################################
# Get override extract dates if passed 
#############################################################
P_EXT_FROM_DT=$1
P_EXT_THRU_DT=$2

echo "Parameters to script: " >> ${LOGNAME}
echo "   P_EXT_FROM_DT=${P_EXT_FROM_DT} " >> ${LOGNAME}
echo "   P_EXT_THRU_DT=${P_EXT_THRU_DT} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
S3CONFIG_BUCKET=${bucket}config/
S3BUCKET=${DUALS_MedAdv_BUCKET} 

echo "S3CONFIG_BUCKET=${S3CONFIG_BUCKET} " >> ${LOGNAME}
echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash


#################################################################################
# Create Extract Date parameters  (YYYY-MM-DD)
#################################################################################
echo " " >> ${LOGNAME}
echo "Create Extract From and Thru date parameters for the Python Extract programs." >> ${LOGNAME}


CUR_YR=`date +%Y`
PRIOR_YR=`expr ${CUR_YR} - 1` 

echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
echo "PRIOR_YR=${PRIOR_YR}" >> ${LOGNAME}

CUR_YY=`echo ${CUR_YR} | cut -c3-4 `
PRIOR_YY=`echo ${PRIOR_YR} | cut -c3-4 `

echo "CUR_YY=${CUR_YY}" >> ${LOGNAME}
echo "PRIOR_YY=${PRIOR_YY}" >> ${LOGNAME}

#################################
# Calculdate Ext date range
#################################
if [ "${P_EXT_FROM_DT}" == "" ]; then

	echo "Calculating Extract dates" >> ${LOGNAME}
	
    # Example: Run July 1, 2025 for Jan 1-Mar 31 2025	
	############################################
	# Determine Ext date range
	############################################
	MM=`date +%m`
	if [   $MM = "07" -o $MM = "08" -o $MM = "09" ]; then
		CLM_FROM_DT=${CUR_YR}-01-01
		CLM_THRU_DT=${CUR_YR}-03-31
		CLM_PRD=P${CUR_YY}Q1

	elif [ $MM = "10" -o $MM = "11" -o $MM = "12" ]; then
		CLM_FROM_DT=${CUR_YR}-04-01
		CLM_THRU_DT=${CUR_YR}-06-30
		CLM_PRD=P${CUR_YY}Q2

	elif [ $MM = "01" -o $MM = "02" -o $MM = "03" ]; then
		CLM_FROM_DT=${PRIOR_YR}-07-01
		CLM_THRU_DT=${PRIOR_YR}-09-30		
		CLM_PRD=P${PRIOR_YY}Q3

	elif [ $MM = "04" -o $MM = "05" -o $MM = "06" ]; then
		CLM_FROM_DT=${PRIOR_YR}-10-01
		CLM_THRU_DT=${PRIOR_YR}-12-31		
		CLM_PRD=P${PRIOR_YY}Q4
	fi
	
else
	# Use Override parm dates
	echo "Using override parameter Extract dates" >> ${LOGNAME}
		
	CLM_FROM_DT=${P_EXT_FROM_DT}
	CLM_THRU_DT=${P_EXT_THRU_DT}
	CLM_PRD=P${P_CLM_PRD}
	
fi

	
echo "CLM_FROM_DT=${CLM_FROM_DT}" >> ${LOGNAME}
echo "CLM_THRU_DT=${CLM_THRU_DT}" >> ${LOGNAME}
echo "CLM_PRD=${CLM_PRD}" >> ${LOGNAME}


#################################################################################
# Remove residual duals linux files
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove residual DUALS_* files on linux data directory." >> ${LOGNAME}

rm "${DATADIR}${DUALS_MEDADV_FILE_PREFIX}"  2>>  ${LOGNAME}


#################################################################################
# Download DUAL MA State parameter file to Linux.
#################################################################################
echo "" >> ${LOGNAME}

## Copy PTD Dual State parameter file to Linux
aws s3 cp s3://${CONFIG_BUCKET}${ST_PARMFILE} ${DATADIR}${ST_PARMFILE}   1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying ${ST_PARMFILE} from S3 to Linux failed." >> ${LOGNAME}

	# Send Failure email	
	SUBJECT="DUALS MedAdv Extract - Failed (${ENVNAME}${TESTEMAIL})"
	MSG="Copying ${ST_PARMFILE} from S3 to Linux failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Loop thru States  
#################################################################################


#################################################################################
# Create State IN-Phrase for Python program  
#################################################################################
echo " " >> ${LOGNAME}
echo "Create State In-Phrase parameter for the Python Extract program." >> ${LOGNAME}
echo " " >> ${LOGNAME}

# Get list of states from state param file: get first 2 bytes, remove comments
EXT_STATES=`cat ${DATADIR}${ST_PARMFILE} | cut -c1-2 | grep -v '^#' `  2>> ${LOGNAME}
EXT_STATES=`echo ${EXT_STATES} | sed 's/ /,/g' `  2>> ${LOGNAME}
echo "EXT_STATES=${EXT_STATES}" >> ${LOGNAME}


#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of DUALS_MedAdv_Ext.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP
export CLM_FROM_DT
export CLM_THRU_DT
export EXT_STATES
export CLM_PRD


#############################################################
# Extract MA PTA Claim Info   
#############################################################
${PYTHON_COMMAND} ${RUNDIR}DUALS_MedAdv_PTA_Ext_old.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script DUALS_MedAdv_PTA_Ext.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DUALS_MedAdv_Ext.sh  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG="DUALS_MedAdv_PTA_Ext.py has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script DUALS_MedAdv_PTA_Ext.py completed successfully. " >> ${LOGNAME}


#############################################################
# Extract MA PTB Claim Info   
#############################################################
${PYTHON_COMMAND} ${RUNDIR}DUALS_MedAdv_PTB_Ext_old.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script DUALS_MedAdv_PTB_Ext.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DUALS_MedAdv_Ext.sh  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG="DUALS_MedAdv_PTB_Ext.py has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script DUALS_MedAdv_PTB_Ext.py completed successfully. " >> ${LOGNAME}


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 
	
	
#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}
	

SUBJECT="DUALS Medicare Advantage extract (${ENVNAME}${TESTEMAIL})" 
MSG="The Extract for the creation of the DUALS Medicare Advantage file(s) from Snowflake has completed.\n\nEFT versions of the below files were created using the following file mask ${EFT_FILEMASK}.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DUALMEDADV_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in DUALS_MedAdv_Ext.sh  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG="Sending Success email in DUALS_MedAdv_Ext.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	



#############################################################
# Download BH file from s3
#############################################################




#############################################################
# Split BH file into two parts
#############################################################
num_split_files=2	

echo "" >> ${LOGNAME}
echo "Split file ${txt_filename} into ${num_split_files} files "  >> ${LOGNAME}

total_lines=`cat ${DATADIR}${txt_filename} | wc -l ` 2>> ${LOGNAME}
echo "total_lines: ${total_lines}"          >> ${LOGNAME}

((lines_per_file = ($total_lines + $num_split_files - 1) / $num_split_files))
echo "lines_per_file: ${lines_per_file} " >> ${LOGNAME}

# DUALS_MedAdv_BL_{ST}_{CLM_PRD}_{TMSTMP}.txt_1.gz --> P#EFT.ON.GCA.DUAL.BH.P25Q41.D260528.T0858091

# Split the actual file into 2 separate files for EFT processing. 
split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${txt_filename} ${DATADIR}/${txt_filename}_  2>> ${LOGNAME}

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script TRICARE_extract.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" TRICARE split extract file  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG=" TRICARE split extract file failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

#############################################################
# Copy original BH file to archive directory
#############################################################


#############################################################
# Zip BH parts files
#############################################################


#############################################################
# Copy BH Parts file to s3 directory
#############################################################


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT DUALS Medicare Advantage Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DUALS Medicare Advantage EFT process  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG="DUALS Medicare Advantage EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo " " >> ${LOGNAME}
echo "Remove residual files from linux data directory." >> ${LOGNAME}
rm "${DATADIR}${DUALS_MEDADV_FILE_PREFIX}"  2>>  ${LOGNAME}

#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "DUALS_MedAdv_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS

