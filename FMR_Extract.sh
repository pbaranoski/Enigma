#!/usr/bin/bash
#
######################################################################################
# Name:  FMR_Extract.sh
#
# Desc: FMR Extract for 6 months
#
# Created:  Viren Khanna  1/17/2023
# Modified: Joshua Turner 9/01/2023 Updated for EFT Functionality  
#
# Paul Baranoski  2023-11-30  Updated extract files and record counts logic to use our standard logic.
#                             (This is needed for Dashboard.sh parsing).
#                             Added $ENVNAME to SUBJECT line of all emails.
# Paul Baranoski 2024-12-02   Replaces FMR_EMAIL_SENDER with CMS_EMAIL_SENDER.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/FMR_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
TMPDIR=/app/IDRC/XTR/CMS/tmp/



touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "FMR_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${FMR_BUCKET} 

echo "FMR Bucket=${S3BUCKET}" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y `
CUR_YR=`expr ${CUR_YR}` 
PRIOR_YR=`expr ${CUR_YR} - 1` 
CURR_YR=`expr ${CUR_YR} - 2` 

echo "CURR_YR=${CURR_YR}" >> ${LOGNAME}
echo "PRIOR_YR=${PRIOR_YR}" >> ${LOGNAME}
echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
############################################
# Determine Prior Interval
############################################
MM=`date +%m`
if [   $MM = "04" -o $MM = "05"  -o $MM = "06"  -o $MM = "07"  -o $MM = "08"  -o $MM = "09" ];  then
	PRIOR_INTRVL="${PRIOR_YR}01"
elif [   $MM = "10" -o $MM = "11" -o $MM = "12"  ]; then
	PRIOR_INTRVL="${PRIOR_YR}02"
elif [   $MM = "01"  -o $MM = "02"  -o $MM = "03" ]; then
	PRIOR_INTRVL="${CURR_YR}02"

else
	echo "Extract is processed half yearly for months other than April and October. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}
	
	Send Did not run email	
	SUBJECT="FMR Extract did not run."
	MSG="Extract is processed half yearly for months April and October. Extract is not scheduled to run for this time period. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "PRIOR_INTRVL=${PRIOR_INTRVL}" >> ${LOGNAME}

############################################
# Determine Current Interval
############################################
MM=`date +%m`
if [  $MM = "04" -o $MM = "05"  -o $MM = "06"  -o $MM = "07"  -o $MM = "08"  -o $MM = "09"  ];  then
	CURRENT_INTRVL="${PRIOR_YR}02"
elif [  $MM = "10" -o $MM = "11" -o $MM = "12"  ]; then
	CURRENT_INTRVL="${CUR_YR}01"
elif [  $MM = "01"  -o $MM = "02"  -o $MM = "03" ]; then
	CURRENT_INTRVL="${PRIOR_YR}01"

else
	echo "Extract is processed half yearly for months other than April and October. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}
	
	# Send Did not run email	
	SUBJECT="FMR Extract did not run."
	MSG="Extract is processed half yearly for months April and October. Extract is not scheduled to run for this time period. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "CURRENT_INTRVL=${CURRENT_INTRVL}" >> ${LOGNAME}

############################################
# Determine File Name
############################################
MM=`date +%m`
if [   $MM = "04" -o $MM = "05"  -o $MM = "06"  -o $MM = "07"  -o $MM = "08"  -o $MM = "09"  ];  then
	#FNAME="${PRIOR_YR}_I02_APR_${CUR_YR}"
        FNAME_INT="Y${PRIOR_YR:(-2)}I02"
        FNAME_RUN="APR${CUR_YR:(-2)}"
elif [   $MM = "10" -o $MM = "11"  -o $MM = "12" ]; then
	#FNAME="${CUR_YR}_I01_OCT_${CUR_YR}"
        FNAME_INT="Y${CUR_YR:(-2)}I01"
        FNAME_RUN="OCT${CUR_YR:(-2)}"

elif [   $MM = "01" -o $MM = "02"  -o $MM = "03" ]; then
	#FNAME="${PRIOR_YR}_I01_OCT_${PRIOR_YR}"
        FNAME_INT="Y${PRIOR_YR:(-2)}I01"
        FNAME_RUN="OCT${PRIOR_YR:(-2)}"
else
	echo "Extract is processed half yearly for months other than April and October. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}
	
	# Send Did not run email	
	#SUBJECT="FMR Extract did not run."
	#MSG="Extract is processed half yearly for months April and October. Extract is not scheduled to run for this time period. "
	#${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "FNAME=${FNAME}" >> ${LOGNAME}

#############################################################
# Make variables available to Python code module.
#############################################################
export TMSTMP	
export CUR_YR
export PRIOR_YR
export PRIOR_INTRVL
export CURRENT_INTRVL
#export FNAME
export FNAME_INT
export FNAME_RUN
#export CLNDR_CYQ_END_DT



############################################
# Execute FMR_Extract Insert
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for FMR_Extract for appropriate Inserting data. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}FMR_Extract_Insert.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script FMR_Extract_Insert.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="FMR Extract - Failed (${ENVNAME})"
		MSG="FMR extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


############################################
# Execute FMR_Extract 
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for FMR_Extract for appropriate 6 Month run. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}FMR_Extract.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script FMR_Extract.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="FMR Extract - Failed (${ENVNAME})"
		MSG="FMR extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#############################################################
# Get list of S3 files for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send Success email.
#############################################################

echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="FMR Files Extract for ${MONTH}${CUR_YR} (${ENVNAME})" 
MSG="The FMR Extract has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in FMR_Extract.sh  - Failed (${ENVNAME})"
		MSG="Sending Success email in FMR_Extract.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	

#############################################################
# EFT Extract files and check status of the extract script
#############################################################
echo " " >> ${LOGNAME}
echo "EFT FMR Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed" >> ${LOGNAME}

	#Send Failure email	
	SUBJECT="FMR_Extract.sh  - Failed (${ENVNAME})"
	MSG="EFT for FMR has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${FMR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

   exit 12
fi

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "FMR_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
