#!/usr/bin/bash
############################################################################################################
# Name: SRTR_FFS_PTA_SNF.sh
#
# Desc: SRTR FFS Part A Extract for SNF
#
# Author     : Joshua Turner	
# Created    : 2/13/2023
#
# Modified:
# Joshua Turner 	2023-02-13 	New script.
# Joshua Turner         2023-05-30      Added EFT functionality and updated to use getExtractFilenamesAndCounts script 
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
S3BUCKET=${SRTR_FFS_BUCKET}
#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/SRTR_FFS_PTA_SNF_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

#############################################################
# Included to produce filenames and counts from the extracts
#############################################################
source ${RUNDIR}FilenameCounts.bash

echo "################################### " >> ${LOGNAME}
echo "SRTR_FFS_PTA_SNF.sh started at ${TMSTMP} " >> ${LOGNAME}
echo "" >> ${LOGNAME}

###########################################################################################
# Establish Parameters - Download SRTR FFS years parameter file from S3 to local data folder
###########################################################################################
SRTR_EXT_YEARS_PARM_FILE=SRTR_EXT_YEARS_PARM_FILE.txt
aws s3 cp s3://${CONFIG_BUCKET}${SRTR_EXT_YEARS_PARM_FILE} ${DATADIR}${SRTR_EXT_YEARS_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 SRTR FFSPTAB Years Parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SRTR FFS Part A SNF Extract - Failed"
	MSG="Copying S3 files from ${FINDER_FILE_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Loop through the years parameter file and call SRTR_FFS_PartA_Extract.py for each 
# year indicated in the file.
# NOTE: The tr command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character will 
#       prevent the file from being processed properly.
###########################################################################################	
ParmFile2Process=`ls ${DATADIR}${SRTR_EXT_YEARS_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "SRTR Year Extract Parameter file: ${ParmFile2Process}" >> ${LOGNAME}

years=`cat ${ParmFile2Process} | tr -d '\r' `

for line in $(echo $years ) 
do
	#############################################################
	# Start extract for next parameter year
	#############################################################
	echo " " >> ${LOGNAME}
	echo "-----------------------------------" >> ${LOGNAME}
	
	# Extract Year from Parameter file record
	echo "Parameter record=${line}" >> ${LOGNAME}

	#################################################################################
	# Load parameters for Extract
	#################################################################################
	echo " " >> ${LOGNAME}

	EXT_YEAR=${line}
	CTYP="SNF"
	CLM_TYPE_CD="20,30"

	echo "CLM_TYPE_LIT=${CTYP}" >> ${LOGNAME}
	echo "EXT_CLM_TYPES=${CLM_TYPE_CD}" >> ${LOGNAME}
	echo "EXT_YEAR=${EXT_YEAR}" >> ${LOGNAME}

	# Export environment variables for Python code
	export TMSTMP
	export CLM_TYPE_CD
	export EXT_YEAR
	export CTYP

	#############################################################
	# Execute Python code to extract data.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Start execution of SRTR_FFS_PartA_Extract.py program"  >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}SRTR_FFS_PartA_Extract.py >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script  
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script SRTR_FFS_PartA_Extract.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SRTR_FFS_PTA_SNF.sh  - Failed"
		MSG="SRTR FFS Part A SNF extract has failed in extract step."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		exit 12
	fi

	echo "" >> ${LOGNAME}
	echo "Python script SRTR_FFS_PartA_Extract.py completed successfully for SNF. " >> ${LOGNAME}
done

###########################################################################################
# Get a list of all S3 files for success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}" 

###########################################################################################
# EFT Extract files and check status of the extract script
###########################################################################################
echo " " >> ${LOGNAME}
echo "EFT SRTR FFS Part A SNF Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed" >> ${LOGNAME}

	# Send Failure email	
	SUBJECT="SRTR_FFS_PTA_SNF.sh - Failed"
	MSG="EFT for SRTR FFS Part A SNF has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

   exit 12
fi

###########################################################################################
# Send Success Email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="SRTR FFS Part A SNF Extract" 
MSG="The SRTR FFS Part A SNF extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in SRTR_FFS_PTA_SNF.sh - Failed"
	MSG="Sending Success email in SRTR_FFS_PTA_SNF.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

###########################################################################################
# End script
###########################################################################################
echo "" >> ${LOGNAME}
echo "SRTR_FFS_PTA_SNF.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
