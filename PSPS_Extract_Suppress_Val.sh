#!/usr/bin/bash
#
######################################################################################
# Name:  PSPS_Extract_Suppress.sh
#
# Desc: PSPS Extract for Q6 Suppresion file. 
#
# Created: Paul Baranoski  07/15/2022
# Modified:
#
# Paul Baranoski 2022-11-10 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-10 Added code to send Success emails with filenames from script
#                           instead of python code.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_Extract_Supress_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSPS_Extract_Suppression.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${PSPS_BUCKET} 

echo "PSPS bucket=${S3BUCKET}" >> ${LOGNAME}


############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y`
PRIOR_YR=`expr ${CUR_YR} - 1` 

echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
echo "PRIOR_YR=${PRIOR_YR}" >> ${LOGNAME}


############################################
# Determine Processing Qtr
############################################
MM=`date +%m`
MM="07"
if [ $MM = "07" -o $MM = "08" -o $MM = "09" ]; then
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q2"
else
	echo "Extract is processed each July with Q6 data. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}

	# Send Did not run email	
	SUBJECT="PSPS Extract did not run."
	MSG="Extract is processed each July with Q6 data. Extract is not scheduled to run for this time period."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0 
fi


echo "CLNDR_CYQ_BEG_DT=${CLNDR_CYQ_BEG_DT}" >> ${LOGNAME}
echo "CLNDR_CYQ_END_DT=${CLNDR_CYQ_END_DT}" >> ${LOGNAME}

#############################################################
# Make variables available to Python code module.
#############################################################
export TMSTMP	
export CLNDR_CYQ_BEG_DT
export CLNDR_CYQ_END_DT


#############################################################
# Execute Python code
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of PSPS_Extract_Suppress.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}PSPS_Extract_Suppress.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_Extract_Suppress.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Extract Supress - Failed"
		MSG="The PSPS Extract Suppress script has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSPS_Extract_Suppress.py completed successfully." >> ${LOGNAME}


#############################################################
# Get list of S3 files for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

S3Files=`aws s3 ls s3://${S3BUCKET} | awk '{print $4}' | grep ${TMSTMP} | tr ' ' '\n' `  2>> ${LOGNAME}

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
        echo "" >> ${LOGNAME}
        echo "Error in getting S3 Extract file list" >> ${LOGNAME}

		# Send Failure email	
		SUBJECT="Get S3 Extract file list in PSPS_Extract_Suppress.sh - Failed"
		MSG="S3 Extract file list in PSPS_Extract_Suppress.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="PSPS Q6 Suppression Extract" 
MSG="The PSPS Q6 Suppression Extract has completed.\n\nThe following file(s) were created:\n\n${S3Files}"


${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in PSPS_Extract_Suppress.sh  - Failed"
		MSG="Sending Success email in PSPS_Extract_Suppress.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for PSPS Extract.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${PSPS_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Create Manifest file in PSPS_Extract_Suppress.sh  - Failed"
		MSG="Create Manifest file in PSPS_Extract_Suppress.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_Extract_Suppress.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS


