#!/usr/bin/bash
############################################################################################################
# Name: SAF_PDE_Extract.sh
#
# Desc: SAF PDE Extract
#
# Author     : Viren Khanna
# Created    : 4/07/2023
#
# Modified:
# Viren Khanna 	2023-04-07 	New script.
# Paul Baranoski 2024-09-11 Added EFT functionality. 
#                           Changed SAF_PDE_EMAIL_SENDER to CMS_EMAIL_SENDER.
#                           Changed SAF_PDE_EMAIL_FAILURE_RECIPIENT to CMS_EMAIL_FAILURE_RECIPIENT.
#                           Add (${ENVNAME}) to SUBJECT line of all emails.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/SAF_PDE_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/




touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "SAF_PDE_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${SAF_PDE_BUCKET} 
echo "SAF PDE bucket=${S3BUCKET}" >> ${LOGNAME}

###########################################################################################
# Delete any temp SAF PDE file list files from DATADIR
###########################################################################################
rm ${DATADIR}temp_SAF_PDE_Files.txt >> ${LOGNAME} 2>&1


############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y `
CUR_YR=`expr ${CUR_YR}` 
EXT_YR=`expr ${CUR_YR} - 1` 
CLM_TYPE=PDE

echo "EXT_YR=${EXT_YR}" >> ${LOGNAME}
echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}


#############################################################
# Make variables available to Python code module.
#############################################################
export TMSTMP	
export CUR_YR
export EXT_YR
export CLM_TYPE=PDE
export DATADIR



#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of SAF_PDE_Extract.py program"  >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}SAF_PDE_Extract.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script SAF_PDE_Extract.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SAF_PDE_Extract.sh  - Failed (${ENVNAME})"
	MSG="SAF PDE extract has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script SAF_PDE_Extract.py completed successfully. " >> ${LOGNAME}


###########################################################################################
# Loop through the extract file list and call combineFiles.sh for each 
# Note: this step will likely take the bulk of processing time for the script
###########################################################################################
echo "##########################################" >> ${LOGNAME}
echo "Reading extract file list and calling CombineS3Files.sh" >> ${LOGNAME}

SAF_FILE_LIST=temp_SAF_PDE_Files.txt	
FILE_LIST=`ls ${DATADIR}${SAF_FILE_LIST}` 1>> ${LOGNAME}  2>&1
FILE_PREFIX=`cat ${FILE_LIST} | tr -d '\r' `

for EXT in $(echo $FILE_PREFIX ) 
do
	${RUNDIR}CombineS3Files.sh ${SAF_PDE_BUCKET} ${EXT} >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "SAF_PDE_Extract.sh failed during the combine step" >> ${LOGNAME}

		#Send Failure email	
		SUBJECT="SAF PDE Extract - Failed (${ENVNAME})"
		MSG="SAF_PDE_Extract.sh failed while combining S3 files."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		exit 12
	fi
	
done


###########################################################################################
# Call getExtractFilenamesAndCounts() from FilenameCounts.bash to get all files created
# and the counts for each file to send in the success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts." >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME} >> ${LOGNAME} 2>&1
FILE_LIST="${filenamesAndCounts}"


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}

SUBJECT="SAF PDE extract (${ENVNAME})" 
MSG="The SAF PDE extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${FILE_LIST}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SAF_PDE_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in SAF_PDE_Extract.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in SAF_PDE_Extract.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT SAF PDE Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" SAF PDE Extract EFT process  - Failed (${ENVNAME})"
	MSG="SAF PDE Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "SAF_PDE_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
